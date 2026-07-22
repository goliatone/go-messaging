// Package goadmin adapts go-admin command-run updates to go-messaging routes.
package goadmin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	admin "github.com/goliatone/go-admin/pkg/admin"
	messaging "github.com/goliatone/go-messaging"
)

const defaultTransportName = "go-messaging"

const automaticCloseTimeout = 5 * time.Second

// SourceBinding identifies one host-owned driver source used by an ingress.
type SourceBinding struct {
	Name         string
	LogicalRoute string
	Driver       string
	Source       messaging.Source
}

// TransportConfig assembles publisher and subscriber directions independently.
// The adapter owns subscriptions it creates, but never the injected router or drivers.
type TransportConfig struct {
	Name         string
	Router       *messaging.Router
	Drivers      *messaging.DriverRegistry
	PublishRoute string
	Sources      []SourceBinding
	Codec        *Codec
	ErrorBuffer  int
}

// Transport implements the go-admin command-run transport over go-messaging.
type Transport struct {
	name         string
	router       *messaging.Router
	drivers      *messaging.DriverRegistry
	publishRoute string
	sources      []SourceBinding
	codec        *Codec
	errorBuffer  int
	capabilities admin.CommandRunTransportCapabilities
}

// NewTransport validates adapter wiring without starting or taking ownership of drivers.
func NewTransport(config TransportConfig) (*Transport, error) {
	config.Name = strings.TrimSpace(config.Name)
	if config.Name == "" {
		config.Name = defaultTransportName
	}
	config.PublishRoute = strings.TrimSpace(config.PublishRoute)
	if config.Codec == nil {
		return nil, fmt.Errorf("%w: codec is required", ErrInvalidConfig)
	}
	publishEnabled := config.Router != nil || config.PublishRoute != ""
	if publishEnabled && (config.Router == nil || config.PublishRoute == "") {
		return nil, fmt.Errorf("%w: router and publish route must be configured together", ErrInvalidConfig)
	}
	subscribeEnabled := len(config.Sources) > 0
	if subscribeEnabled && config.Drivers == nil {
		return nil, fmt.Errorf("%w: driver registry is required for ingress", ErrInvalidConfig)
	}
	if !publishEnabled && !subscribeEnabled {
		return nil, fmt.Errorf("%w: publisher or subscriber wiring is required", ErrInvalidConfig)
	}
	if config.Drivers == nil {
		return nil, fmt.Errorf("%w: driver registry is required", ErrInvalidConfig)
	}
	if config.ErrorBuffer <= 0 {
		config.ErrorBuffer = 16
	}

	usedDrivers := make(map[string]messaging.Driver)
	if publishEnabled {
		route, ok := config.Router.Route(config.PublishRoute)
		if !ok {
			return nil, fmt.Errorf("%w: publish route is unavailable", ErrInvalidConfig)
		}
		if !routeAllowsCommandRuns(route) {
			return nil, fmt.Errorf("%w: publish route rejects command-run events", ErrInvalidConfig)
		}
		if config.Drivers != nil {
			for _, binding := range route.Bindings {
				driver, ok := config.Drivers.Lookup(binding.Driver)
				if !ok {
					return nil, fmt.Errorf("%w: publish driver is unavailable", ErrInvalidConfig)
				}
				usedDrivers[binding.Driver] = driver
			}
		}
	}

	sources := make([]SourceBinding, len(config.Sources))
	copy(sources, config.Sources)
	seenSources := make(map[string]struct{}, len(sources))
	for index := range sources {
		source := &sources[index]
		source.Name = strings.TrimSpace(source.Name)
		source.LogicalRoute = strings.TrimSpace(source.LogicalRoute)
		source.Driver = strings.TrimSpace(source.Driver)
		source.Source.Name = strings.TrimSpace(source.Source.Name)
		if source.Name == "" || source.LogicalRoute == "" || source.Driver == "" || source.Source.Name == "" {
			return nil, fmt.Errorf("%w: source name, logical route, driver, and destination are required", ErrInvalidConfig)
		}
		if _, duplicate := seenSources[source.Name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate source binding", ErrInvalidConfig)
		}
		seenSources[source.Name] = struct{}{}
		driver, err := config.Drivers.Require(source.Driver)
		if err != nil {
			return nil, fmt.Errorf("%w: source driver is unavailable", ErrInvalidConfig)
		}
		if _, ok := driver.(messaging.ConsumeDriver); !ok {
			return nil, fmt.Errorf("%w: source driver cannot consume", ErrInvalidConfig)
		}
		usedDrivers[source.Driver] = driver
	}

	capabilities := mapCapabilities(config.Name, usedDrivers)
	if err := capabilities.Validate(); err != nil {
		return nil, fmt.Errorf("%w: transport capabilities", ErrInvalidConfig)
	}
	return &Transport{
		name: config.Name, router: config.Router, drivers: config.Drivers,
		publishRoute: config.PublishRoute, sources: sources, codec: config.Codec,
		errorBuffer: config.ErrorBuffer, capabilities: capabilities,
	}, nil
}

// Capabilities reports only provider-neutral semantics.
func (t *Transport) Capabilities() admin.CommandRunTransportCapabilities {
	if t == nil {
		return admin.CommandRunTransportCapabilities{}
	}
	return t.capabilities
}

// PublishCommandRun encodes and publishes one update through the configured logical route.
func (t *Transport) PublishCommandRun(ctx context.Context, update admin.CommandRunUpdate) error {
	if t == nil || t.router == nil || t.publishRoute == "" {
		return ErrPublisherDisabled
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	envelope, err := t.codec.Encode(update)
	if err != nil {
		return err
	}
	if _, err := t.router.Publish(ctx, t.publishRoute, envelope); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return ErrPublishFailed
	}
	return nil
}

// SubscribeCommandRuns creates a strict ingress and an adapter-owned composite subscription.
func (t *Transport) SubscribeCommandRuns(
	ctx context.Context,
	selector admin.CommandRunSelector,
	handler admin.CommandRunHandler,
) (admin.CommandRunSubscription, error) {
	if t == nil || t.drivers == nil || len(t.sources) == 0 {
		return nil, ErrSubscriberDisabled
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selector = selector.Normalize()
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrInvalidConfig)
	}

	subscriptionCtx, cancel := context.WithCancel(ctx)
	handlerErrors := make(chan error, t.errorBuffer)
	report := func(err error) {
		select {
		case handlerErrors <- err:
		default:
		}
	}
	messageHandler := func(deliveryCtx context.Context, delivery messaging.Delivery) messaging.HandleResult {
		if delivery == nil {
			report(ErrEnvelopeRejected)
			return messaging.Reject(ErrEnvelopeRejected)
		}
		update, err := t.codec.Decode(delivery.Envelope())
		if err != nil {
			report(classifyDecodeError(err))
			return messaging.Reject(ErrEnvelopeRejected)
		}
		if !selector.Matches(update.Scope) {
			report(ErrScopeRejected)
			return messaging.Reject(ErrScopeRejected)
		}
		handlerCtx, cancelHandler := context.WithCancel(deliveryCtx)
		stopCancel := context.AfterFunc(subscriptionCtx, cancelHandler)
		defer func() {
			stopCancel()
			cancelHandler()
		}()
		if err := handler(handlerCtx, update.Clone()); err != nil {
			report(admin.ErrCommandRunHandlerFailed)
			return messaging.Reject(admin.ErrCommandRunHandlerFailed)
		}
		return messaging.Complete()
	}

	bindings := make([]messaging.IngressBinding, 0, len(t.sources))
	for _, source := range t.sources {
		bindings = append(bindings, messaging.IngressBinding{
			Name: source.Name, LogicalRoute: source.LogicalRoute,
			Driver: source.Driver, Source: source.Source,
			AcceptedKinds:        []messaging.Kind{messaging.KindEvent},
			AcceptedTypes:        []string{MessageType},
			AcceptedContentTypes: []string{ContentTypeJSON},
			AcceptedSchemas:      []string{EnvelopeSchemaVersion},
			Handlers:             []messaging.Handler{messageHandler},
			RequiredDispositions: []messaging.Disposition{messaging.DispositionComplete, messaging.DispositionReject},
		})
	}
	ingress, err := messaging.NewIngress(t.drivers, bindings)
	if err != nil {
		cancel()
		return nil, ErrSubscriptionFailed
	}
	subscriptions, err := ingress.Subscribe(subscriptionCtx)
	if err != nil {
		cancel()
		return nil, ErrSubscriptionFailed
	}
	if len(subscriptions) == 0 {
		cancel()
		return nil, ErrSubscriptionFailed
	}
	return newCompositeSubscription(subscriptionCtx, cancel, subscriptions, handlerErrors, t.errorBuffer), nil
}

func routeAllowsCommandRuns(route messaging.Route) bool {
	allowsKind := len(route.Kinds) == 0
	for _, kind := range route.Kinds {
		allowsKind = allowsKind || kind == messaging.KindEvent
	}
	allowsType := len(route.Types) == 0
	for _, messageType := range route.Types {
		allowsType = allowsType || messageType == MessageType
	}
	return allowsKind && allowsType
}

func mapCapabilities(name string, drivers map[string]messaging.Driver) admin.CommandRunTransportCapabilities {
	capabilities := admin.CommandRunTransportCapabilities{
		Name: name, Fanout: true, Durability: admin.CommandRunTransportDurabilityDurable, Replay: true,
	}
	if len(drivers) == 0 {
		capabilities.Durability = admin.CommandRunTransportDurabilityEphemeral
		capabilities.Replay = false
		return capabilities
	}
	for _, driver := range drivers {
		current := driver.Capabilities()
		capabilities.Fanout = capabilities.Fanout && current.Fanout
		if !current.Durability {
			capabilities.Durability = admin.CommandRunTransportDurabilityEphemeral
		}
		capabilities.Replay = capabilities.Replay && current.Replay
	}
	if capabilities.Durability != admin.CommandRunTransportDurabilityDurable {
		capabilities.Replay = false
	}
	return capabilities
}

func classifyDecodeError(err error) error {
	if errors.Is(err, ErrIdentityMismatch) {
		return ErrIdentityMismatch
	}
	return ErrEnvelopeRejected
}

type compositeSubscription struct {
	cancel        context.CancelFunc
	subscriptions []messaging.Subscription
	ready         chan struct{}
	errors        chan error
	done          chan struct{}
	closeDone     chan struct{}
	closeOnce     sync.Once
	closeMu       sync.RWMutex
	closeErr      error
}

func newCompositeSubscription(
	ctx context.Context,
	cancel context.CancelFunc,
	subscriptions []messaging.Subscription,
	handlerErrors <-chan error,
	errorBuffer int,
) *compositeSubscription {
	s := &compositeSubscription{
		cancel: cancel, subscriptions: append([]messaging.Subscription(nil), subscriptions...),
		ready: make(chan struct{}), errors: make(chan error, errorBuffer),
		done: make(chan struct{}), closeDone: make(chan struct{}),
	}
	go s.monitor(ctx, handlerErrors)
	go func() {
		<-ctx.Done()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), automaticCloseTimeout)
		defer closeCancel()
		_ = s.Close(closeCtx)
	}()
	return s
}

func (s *compositeSubscription) Ready() <-chan struct{} { return s.ready }
func (s *compositeSubscription) Errors() <-chan error   { return s.errors }

func (s *compositeSubscription) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.closeOnce.Do(func() {
		s.cancel()
		go func() {
			var joined error
			for _, subscription := range s.subscriptions {
				if err := subscription.Close(ctx); err != nil {
					joined = errors.Join(joined, ErrSubscriptionFailed)
				}
			}
			<-s.done
			s.closeMu.Lock()
			s.closeErr = joined
			s.closeMu.Unlock()
			close(s.closeDone)
		}()
	})
	select {
	case <-s.closeDone:
		s.closeMu.RLock()
		defer s.closeMu.RUnlock()
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *compositeSubscription) monitor(ctx context.Context, handlerErrors <-chan error) {
	defer close(s.done)
	defer close(s.errors)

	readySignals := make(chan struct{}, len(s.subscriptions))
	subscriptionErrors := make(chan error, len(s.subscriptions))
	var monitors sync.WaitGroup
	for _, subscription := range s.subscriptions {
		monitors.Add(1)
		go func(sub messaging.Subscription) {
			defer monitors.Done()
			ready := sub.Ready()
			errorsCh := sub.Errors()
			for ready != nil || errorsCh != nil {
				select {
				case <-ctx.Done():
					return
				case <-ready:
					readySignals <- struct{}{}
					ready = nil
				case err, ok := <-errorsCh:
					if !ok {
						if ready != nil {
							select {
							case subscriptionErrors <- ErrSubscriptionFailed:
							case <-ctx.Done():
							}
						}
						errorsCh = nil
						continue
					}
					if err != nil {
						classified := classifySubscriptionError(err)
						if classified == nil {
							continue
						}
						select {
						case subscriptionErrors <- classified:
						case <-ctx.Done():
						}
					}
				}
			}
		}(subscription)
	}

	monitorsDone := make(chan struct{})
	go func() {
		monitors.Wait()
		close(monitorsDone)
	}()
	readyCount := 0
	readyClosed := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-monitorsDone:
			return
		case <-readySignals:
			readyCount++
			if !readyClosed && readyCount == len(s.subscriptions) {
				close(s.ready)
				readyClosed = true
			}
		case err := <-handlerErrors:
			if err != nil {
				s.report(err)
			}
		case err := <-subscriptionErrors:
			if err != nil {
				s.report(err)
			}
		}
	}
}

func classifySubscriptionError(err error) error {
	if err == nil {
		return nil
	}
	if messaging.IsMessageError(err) {
		// These failures were already reported at the adapter boundary. Ignore the
		// driver's disposition echo so diagnostics count one rejection per event.
		for _, known := range []error{
			ErrEnvelopeRejected,
			ErrIdentityMismatch,
			ErrScopeRejected,
			admin.ErrCommandRunHandlerFailed,
		} {
			if errors.Is(err, known) {
				return nil
			}
		}
		return ErrDeliveryFailed
	}
	return ErrSubscriptionFailed
}

func (s *compositeSubscription) report(err error) {
	if err == nil {
		return
	}
	select {
	case s.errors <- err:
	default:
	}
}

var _ admin.CommandRunTransport = (*Transport)(nil)
var _ admin.CommandRunSubscription = (*compositeSubscription)(nil)
