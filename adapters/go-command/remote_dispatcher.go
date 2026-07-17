package commandadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

const (
	HeaderDelayNanos = "go-command.delay-nanos"
	HeaderRunAt      = "go-command.run-at"
)

type CatalogSelector func(command.MessageRegistration) bool

type RemoteDispatcherConfig struct {
	Router        *messaging.Router
	Correlations  *messaging.CorrelationRegistry
	TypedCodec    TypedCodec
	CatalogCodec  CatalogCodec
	UseCatalog    CatalogSelector
	ReplyCodec    ReplyCodec
	ReplyRoute    string
	SchemaVersion string
}

type RemoteDispatcher struct {
	router        *messaging.Router
	correlations  *messaging.CorrelationRegistry
	typedCodec    TypedCodec
	catalogCodec  CatalogCodec
	useCatalog    CatalogSelector
	replyCodec    ReplyCodec
	replyRoute    string
	schemaVersion string
}

var _ command.RemoteDispatcher = (*RemoteDispatcher)(nil)

func NewRemoteDispatcher(config RemoteDispatcherConfig) (*RemoteDispatcher, error) {
	if config.Router == nil || config.Correlations == nil {
		return nil, fmt.Errorf("go-command adapter: router and correlation registry are required")
	}
	if config.TypedCodec == nil || isTypedNil(config.TypedCodec) {
		config.TypedCodec = JSONTypedCodec{}
	}
	if config.ReplyCodec == nil || isTypedNil(config.ReplyCodec) {
		config.ReplyCodec = JSONReplyCodec{}
	}
	if strings.TrimSpace(config.SchemaVersion) == "" {
		config.SchemaVersion = "1"
	}
	return &RemoteDispatcher{
		router: config.Router, correlations: config.Correlations,
		typedCodec: config.TypedCodec, catalogCodec: config.CatalogCodec,
		useCatalog: config.UseCatalog, replyCodec: config.ReplyCodec,
		replyRoute: strings.TrimSpace(config.ReplyRoute), schemaVersion: strings.TrimSpace(config.SchemaVersion),
	}, nil
}

// ValidateRoutes lets application startup fail before installing placement
// policies that cannot provide a correlated reply path.
func (d *RemoteDispatcher) ValidateRoutes(routes ...command.DispatchRoute) error {
	if d == nil {
		return fmt.Errorf("go-command adapter: remote dispatcher is required")
	}
	for _, route := range routes {
		if route.Target != command.DispatchTargetRemote || strings.TrimSpace(route.Name) == "" {
			return command.NewInvalidDispatchRouteError(route)
		}
		if _, err := d.resolveReplyRoute(route.Name); err != nil {
			return err
		}
		if _, ok := d.router.Route(route.Name); !ok {
			return fmt.Errorf("%w: %s", messaging.ErrUnknownRoute, route.Name)
		}
	}
	return nil
}

func (d *RemoteDispatcher) DispatchRemote(ctx context.Context, route command.DispatchRoute, registration command.MessageRegistration, message any, options command.DispatchOptions) (command.DispatchOutcome, error) {
	if err := d.validateDispatch(route, registration); err != nil {
		return command.DispatchOutcome{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	options, lineage, err := prepareRemoteOptions(ctx, options)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	replyRoute, err := d.resolveReplyRoute(route.Name)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	envelope, err := d.encodeRequest(ctx, registration, message, options, replyRoute, lineage.causationID)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	return d.publishAndAwait(ctx, route.Name, registration, options.CorrelationID, options.Mode, envelope)
}

func (d *RemoteDispatcher) validateDispatch(route command.DispatchRoute, registration command.MessageRegistration) error {
	if d == nil {
		return fmt.Errorf("go-command adapter: remote dispatcher is required")
	}
	if err := validateRegistration(registration); err != nil {
		return err
	}
	if route.Target != command.DispatchTargetRemote || strings.TrimSpace(route.Name) == "" {
		return command.NewInvalidDispatchRouteError(route)
	}
	return nil
}

func prepareRemoteOptions(ctx context.Context, options command.DispatchOptions) (command.DispatchOptions, outboundLineage, error) {
	lineage := outboundLineageFromContext(ctx)
	mode := command.NormalizeExecutionMode(options.Mode)
	if mode == "" {
		mode = command.ExecutionModeInline
	}
	options.Mode = mode
	if err := command.ValidateDispatchOptions(mode, options); err != nil {
		return command.DispatchOptions{}, outboundLineage{}, err
	}
	if strings.TrimSpace(options.CorrelationID) == "" {
		options.CorrelationID = lineage.correlationID
		if options.CorrelationID == "" {
			id, err := newEnvelopeID()
			if err != nil {
				return command.DispatchOptions{}, outboundLineage{}, err
			}
			options.CorrelationID = id
		}
	}
	return options, lineage, nil
}

func (d *RemoteDispatcher) publishAndAwait(ctx context.Context, routeName string, registration command.MessageRegistration, correlationID string, expectedMode command.ExecutionMode, envelope messaging.Envelope) (command.DispatchOutcome, error) {
	deadline, _ := ctx.Deadline()
	waiter, err := d.correlations.Register(correlationID, envelope.Type, deadline)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	defer waiter.Cancel()
	result, err := d.router.Publish(ctx, routeName, envelope)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	if !routingAccepted(result) {
		return command.DispatchOutcome{}, messaging.ErrNotPublished
	}
	reply, err := waiter.Await(ctx)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	outcome, err := d.replyCodec.Decode(ctx, registration, reply)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	if outcome.Receipt.CommandID != registration.ID() {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: remote receipt registration mismatch")
	}
	if command.NormalizeExecutionMode(outcome.Receipt.Mode) != expectedMode {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: remote receipt execution mode mismatch")
	}
	if outcome.Receipt.CorrelationID != correlationID {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: remote receipt correlation mismatch")
	}
	return outcome, nil
}

func (d *RemoteDispatcher) HandleReply(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
	if d == nil || delivery == nil || isTypedNil(delivery) {
		return messaging.Reject(fmt.Errorf("go-command adapter: reply delivery is required"))
	}
	envelope := delivery.Envelope()
	if err := envelope.Validate(); err != nil {
		return messaging.Reject(err)
	}
	if envelope.Kind != messaging.KindReply {
		return messaging.Reject(fmt.Errorf("go-command adapter: expected reply envelope"))
	}
	// Unknown, late, duplicate, and mismatched replies are terminal for this
	// delivery and must not become poison-message redelivery loops.
	d.correlations.Deliver(envelope)
	return messaging.Complete()
}

func (d *RemoteDispatcher) resolveReplyRoute(requestRoute string) (string, error) {
	if d.replyRoute != "" {
		if err := d.validateReplyRoute(d.replyRoute); err != nil {
			return "", err
		}
		return d.replyRoute, nil
	}
	route, ok := d.router.Route(requestRoute)
	if !ok {
		return "", fmt.Errorf("%w: %s", messaging.ErrUnknownRoute, requestRoute)
	}
	if slices.Contains(route.Required, messaging.CapabilityRequestReply) {
		if err := validateReplyKinds(route); err != nil {
			return "", err
		}
		return requestRoute, nil
	}
	return "", fmt.Errorf("%w: remote route %q requires request/reply or an explicit reply route", messaging.ErrUnsupportedCapability, requestRoute)
}

func (d *RemoteDispatcher) validateReplyRoute(name string) error {
	route, ok := d.router.Route(name)
	if !ok {
		return fmt.Errorf("%w: %s", messaging.ErrUnknownRoute, name)
	}
	return validateReplyKinds(route)
}

func validateReplyKinds(route messaging.Route) error {
	if len(route.Kinds) > 0 && !slices.Contains(route.Kinds, messaging.KindReply) {
		return fmt.Errorf("%w: reply route %q does not accept reply envelopes", messaging.ErrUnsupportedCapability, route.Name)
	}
	return nil
}

func (d *RemoteDispatcher) encodeRequest(ctx context.Context, registration command.MessageRegistration, message any, options command.DispatchOptions, replyRoute, causationID string) (messaging.Envelope, error) {
	kind := messaging.KindCommand
	if registration.Kind() == command.HandlerKindQuery {
		kind = messaging.KindQuery
	}
	messageType := registration.MessageType()
	contentType := "application/json"
	var payload []byte
	var err error
	if d.useCatalog != nil && d.useCatalog(registration) {
		if registration.Kind() != command.HandlerKindCommand || d.catalogCodec == nil || isTypedNil(d.catalogCodec) {
			return messaging.Envelope{}, fmt.Errorf("go-command adapter: catalog dispatch is not configured for %s/%s", registration.Kind(), registration.ID())
		}
		dto, encodeErr := d.catalogCodec.EncodeCatalog(ctx, registration, message, options)
		if encodeErr != nil {
			return messaging.Envelope{}, encodeErr
		}
		payload, err = json.Marshal(dto)
		messageType = CatalogEnvelopeType
	} else {
		payload, err = d.typedCodec.Encode(ctx, registration, message)
	}
	if err != nil {
		return messaging.Envelope{}, err
	}
	id, err := newEnvelopeID()
	if err != nil {
		return messaging.Envelope{}, err
	}
	headers := headersFromDispatchOptions(options)
	envelope := messaging.NewEnvelope(id, messageType, kind, d.schemaVersion, contentType, payload, headers)
	envelope.CorrelationID = options.CorrelationID
	envelope.CausationID = strings.TrimSpace(causationID)
	envelope.ReplyTo = replyRoute
	if deadline, ok := ctx.Deadline(); ok {
		envelope.Deadline = deadline
	}
	return envelope, nil
}

func headersFromDispatchOptions(options command.DispatchOptions) map[string]string {
	headers := map[string]string{
		HeaderExecutionMode: string(command.NormalizeExecutionMode(options.Mode)),
	}
	if value := strings.TrimSpace(options.IdempotencyKey); value != "" {
		headers[HeaderIdempotencyKey] = value
	}
	if value := command.NormalizeDedupPolicy(options.DedupPolicy); value != "" {
		headers[HeaderDedupPolicy] = string(value)
	}
	if options.Delay != 0 {
		headers[HeaderDelayNanos] = strconv.FormatInt(options.Delay.Nanoseconds(), 10)
	}
	if options.RunAt != nil {
		headers[HeaderRunAt] = options.RunAt.UTC().Format(time.RFC3339Nano)
	}
	return headers
}

func routingAccepted(result messaging.RoutingResult) bool {
	for _, published := range result.Results {
		if published.Outcome == messaging.PublishAccepted {
			return true
		}
	}
	return false
}
