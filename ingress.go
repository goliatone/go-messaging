package messaging

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

type IngressBinding struct {
	Name                 string
	LogicalRoute         string
	Driver               string
	Source               Source
	AcceptedKinds        []Kind
	AcceptedTypes        []string
	AcceptedContentTypes []string
	AcceptedSchemas      []string
	Handlers             []Handler
	RequiredCapabilities []Capability
	RequiredDispositions []Disposition
}

func (b IngressBinding) clone() IngressBinding {
	b.AcceptedKinds = append([]Kind(nil), b.AcceptedKinds...)
	b.AcceptedTypes = append([]string(nil), b.AcceptedTypes...)
	b.AcceptedContentTypes = append([]string(nil), b.AcceptedContentTypes...)
	b.AcceptedSchemas = append([]string(nil), b.AcceptedSchemas...)
	b.Handlers = append([]Handler(nil), b.Handlers...)
	b.RequiredCapabilities = append([]Capability(nil), b.RequiredCapabilities...)
	b.RequiredDispositions = append([]Disposition(nil), b.RequiredDispositions...)
	return b
}

func (b IngressBinding) accepts(envelope Envelope) bool {
	if len(b.AcceptedKinds) > 0 && !containsKind(b.AcceptedKinds, envelope.Kind) {
		return false
	}
	if len(b.AcceptedTypes) > 0 {
		if !slices.Contains(b.AcceptedTypes, envelope.Type) {
			return false
		}
	}
	if len(b.AcceptedContentTypes) > 0 && !slices.Contains(b.AcceptedContentTypes, envelope.ContentType) {
		return false
	}
	if len(b.AcceptedSchemas) > 0 && !slices.Contains(b.AcceptedSchemas, envelope.SchemaVersion) {
		return false
	}
	return true
}

type ingressSnapshot struct{ bindings map[string]IngressBinding }

type Ingress struct {
	drivers  *DriverRegistry
	observer Observer
	snapshot atomic.Pointer[ingressSnapshot]
}

type IngressOption func(*Ingress)

func WithIngressObserver(observer Observer) IngressOption {
	return func(ingress *Ingress) {
		if observer != nil {
			ingress.observer = observer
		}
	}
}

func NewIngress(drivers *DriverRegistry, bindings []IngressBinding, options ...IngressOption) (*Ingress, error) {
	if drivers == nil {
		return nil, fmt.Errorf("%w: driver registry is required", ErrUnknownDriver)
	}
	i := &Ingress{drivers: drivers, observer: NopObserver{}}
	for _, option := range options {
		if option != nil {
			option(i)
		}
	}
	if err := i.ReplaceBindings(bindings); err != nil {
		return nil, err
	}
	return i, nil
}

func (i *Ingress) ReplaceBindings(bindings []IngressBinding) error {
	next := make(map[string]IngressBinding, len(bindings))
	for _, binding := range bindings {
		if strings.TrimSpace(binding.Name) == "" || strings.TrimSpace(binding.LogicalRoute) == "" || strings.TrimSpace(binding.Source.Name) == "" {
			return fmt.Errorf("%w: ingress binding name, route, and source are required", ErrUnknownRoute)
		}
		if len(binding.Handlers) == 0 {
			return fmt.Errorf("%w: ingress binding %q requires a handler", ErrUnknownRoute, binding.Name)
		}
		if err := validateIngressPolicy(binding); err != nil {
			return err
		}
		if _, exists := next[binding.Name]; exists {
			return fmt.Errorf("%w: duplicate ingress binding %q", ErrUnknownRoute, binding.Name)
		}
		required := append([]Capability(nil), binding.RequiredCapabilities...)
		for _, disposition := range binding.RequiredDispositions {
			required = append(required, CapabilityForDisposition(disposition)...)
		}
		driver, err := i.drivers.Require(binding.Driver, required...)
		if err != nil {
			return fmt.Errorf("ingress %q: %w", binding.Name, err)
		}
		if _, ok := driver.(ConsumeDriver); !ok {
			return fmt.Errorf("ingress %q: %w", binding.Name, ErrUnsupportedCapability)
		}
		next[binding.Name] = binding.clone()
	}
	i.snapshot.Store(&ingressSnapshot{bindings: next})
	return nil
}

func validateIngressPolicy(binding IngressBinding) error {
	for _, kind := range binding.AcceptedKinds {
		switch kind {
		case KindCommand, KindQuery, KindEvent, KindReply:
		default:
			return fmt.Errorf("%w: ingress binding %q has invalid kind %q", ErrInvalidEnvelope, binding.Name, kind)
		}
	}
	for label, values := range map[string][]string{
		"type": binding.AcceptedTypes, "content type": binding.AcceptedContentTypes, "schema": binding.AcceptedSchemas,
	} {
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("%w: ingress binding %q has an empty accepted %s", ErrInvalidEnvelope, binding.Name, label)
			}
		}
	}
	return nil
}

// Subscribe starts every binding from one immutable generation.
func (i *Ingress) Subscribe(ctx context.Context) ([]Subscription, error) {
	snapshot := i.snapshot.Load()
	if snapshot == nil {
		return nil, nil
	}
	subscriptions := make([]Subscription, 0, len(snapshot.bindings))
	for _, binding := range snapshot.bindings {
		driver, ok := i.drivers.Lookup(binding.Driver)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownDriver, binding.Driver)
		}
		consumer, ok := driver.(ConsumeDriver)
		if !ok {
			return nil, fmt.Errorf("%w: driver %s cannot consume", ErrUnsupportedCapability, binding.Driver)
		}
		bindingCopy := binding.clone()
		subscription, err := consumer.Subscribe(ctx, bindingCopy.Source, func(ctx context.Context, delivery Delivery) HandleResult {
			started := time.Now()
			if delivery == nil {
				result := Reject(ErrInvalidEnvelope)
				i.observeConsume(ctx, bindingCopy, Envelope{}, DeliveryInfo{}, result, started)
				return result
			}
			envelope := delivery.Envelope()
			if !bindingCopy.accepts(envelope) {
				result := Reject(fmt.Errorf("%w: ingress policy rejected kind, type, content type, or schema", ErrInvalidEnvelope))
				i.observeConsume(ctx, bindingCopy, envelope, delivery.Info(), result, started)
				return result
			}
			result := Complete()
			for _, handler := range bindingCopy.Handlers {
				result = InvokeHandler(ctx, handler, delivery)
				if result.Disposition != DispositionComplete {
					break
				}
			}
			i.observeConsume(ctx, bindingCopy, envelope, delivery.Info(), result, started)
			return result
		})
		if err != nil {
			var closeErr error
			for _, existing := range subscriptions {
				closeErr = errors.Join(closeErr, existing.Close(ctx))
			}
			return nil, errors.Join(err, closeErr)
		}
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions, nil
}

func (i *Ingress) observeConsume(ctx context.Context, binding IngressBinding, envelope Envelope, info DeliveryInfo, result HandleResult, started time.Time) {
	i.observer.Observe(ctx, Observation{
		Operation:     OperationConsume,
		LogicalRoute:  binding.LogicalRoute,
		Kind:          envelope.Kind,
		MessageType:   envelope.Type,
		Transport:     binding.Driver,
		Destination:   binding.Source.Name,
		CorrelationID: envelope.CorrelationID,
		Attempt:       info.Attempt,
		Outcome:       string(result.Disposition),
		Latency:       time.Since(started),
		Err:           safeObservationError(result.Err),
	})
}
