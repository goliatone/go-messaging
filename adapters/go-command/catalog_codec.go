package commandadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

const (
	CatalogWireVersion  = "1"
	CatalogEnvelopeType = "go-command.catalog"
)

// CatalogOptionsDTO is the explicit allowlisted wire representation of
// DispatchOptions. DispatchOptions itself is intentionally never serialized.
type CatalogOptionsDTO struct {
	Mode           string `json:"mode,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	DedupPolicy    string `json:"dedup_policy,omitempty"`
	DelayNanos     int64  `json:"delay_nanos,omitempty"`
	RunAt          string `json:"run_at,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
}

type CatalogDispatchDTO struct {
	Version       string            `json:"version"`
	SchemaVersion string            `json:"schema_version"`
	CommandID     string            `json:"command_id"`
	HandlerKind   string            `json:"handler_kind,omitempty"`
	Payload       map[string]any    `json:"payload"`
	IDs           []string          `json:"ids,omitempty"`
	Options       CatalogOptionsDTO `json:"options,omitempty"`
}

type CatalogBinding struct {
	CatalogID           string
	RegistrationID      string
	Kind                command.HandlerKind
	SchemaVersion       string
	AllowEventToCommand bool
}

type CatalogCodec interface {
	EncodeCatalog(context.Context, command.MessageRegistration, any, command.DispatchOptions) (CatalogDispatchDTO, error)
	DecodeCatalog(context.Context, CatalogDispatchDTO, command.RegistrationProvider) (command.CommandDispatchRequest, command.MessageRegistration, any, error)
	ValidateCoverage(...command.MessageRegistration) error
	AllowsEventToCommand(command.HandlerKind, string) bool
}

type JSONCatalogCodec struct {
	byCatalogID    map[catalogBindingKey]catalogBindingRecord
	byRegistration map[catalogBindingKey]catalogBindingRecord
	typed          TypedCodec
}

type catalogBindingRecord struct {
	binding     CatalogBinding
	messageType string
	requestType reflect.Type
	resultType  reflect.Type
}

type catalogBindingKey struct {
	kind command.HandlerKind
	id   string
}

func NewJSONCatalogCodec(provider command.RegistrationProvider, typed TypedCodec, bindings ...CatalogBinding) (*JSONCatalogCodec, error) {
	if provider == nil || isTypedNil(provider) {
		return nil, command.NewRegistrationProviderNotConfiguredError()
	}
	if typed == nil || isTypedNil(typed) {
		typed = JSONTypedCodec{}
	}
	codec := &JSONCatalogCodec{
		byCatalogID:    make(map[catalogBindingKey]catalogBindingRecord, len(bindings)),
		byRegistration: make(map[catalogBindingKey]catalogBindingRecord, len(bindings)),
		typed:          typed,
	}
	for _, binding := range bindings {
		normalized, err := normalizeCatalogBinding(provider, binding)
		if err != nil {
			return nil, err
		}
		if err := codec.addBinding(normalized); err != nil {
			return nil, err
		}
	}
	return codec, nil
}

func normalizeCatalogBinding(provider command.RegistrationProvider, binding CatalogBinding) (catalogBindingRecord, error) {
	binding.CatalogID = strings.TrimSpace(binding.CatalogID)
	binding.RegistrationID = strings.TrimSpace(binding.RegistrationID)
	binding.SchemaVersion = strings.TrimSpace(binding.SchemaVersion)
	if binding.CatalogID == "" {
		binding.CatalogID = binding.RegistrationID
	}
	if binding.CatalogID == "" || binding.RegistrationID == "" || binding.SchemaVersion == "" {
		return catalogBindingRecord{}, fmt.Errorf("go-command adapter: catalog id, registration id, and schema version are required")
	}
	if binding.Kind != command.HandlerKindCommand && binding.Kind != command.HandlerKindQuery {
		return catalogBindingRecord{}, fmt.Errorf("go-command adapter: invalid catalog handler kind %q", binding.Kind)
	}
	if binding.AllowEventToCommand && binding.Kind != command.HandlerKindCommand {
		return catalogBindingRecord{}, fmt.Errorf("go-command adapter: event reinterpretation requires a command binding")
	}
	registration, ok := provider.RegistrationByID(binding.Kind, binding.RegistrationID)
	if !ok {
		return catalogBindingRecord{}, command.NewRegistrationNotFoundError(binding.Kind, binding.RegistrationID)
	}
	if err := validateRegistration(registration); err != nil {
		return catalogBindingRecord{}, err
	}
	return catalogBindingRecord{
		binding: binding, messageType: registration.MessageType(),
		requestType: registration.RequestType(), resultType: registration.ResultType(),
	}, nil
}

func (c *JSONCatalogCodec) addBinding(record catalogBindingRecord) error {
	binding := record.binding
	catalogKey := catalogBindingKey{kind: binding.Kind, id: binding.CatalogID}
	registrationKey := catalogBindingKey{kind: binding.Kind, id: binding.RegistrationID}
	if _, exists := c.byCatalogID[catalogKey]; exists {
		return fmt.Errorf("go-command adapter: ambiguous catalog id %q for %s", binding.CatalogID, binding.Kind)
	}
	if _, exists := c.byRegistration[registrationKey]; exists {
		return fmt.Errorf("go-command adapter: ambiguous registration mapping %q for %s", binding.RegistrationID, binding.Kind)
	}
	c.byCatalogID[catalogKey] = record
	c.byRegistration[registrationKey] = record
	return nil
}

func (c *JSONCatalogCodec) EncodeCatalog(ctx context.Context, registration command.MessageRegistration, message any, options command.DispatchOptions) (CatalogDispatchDTO, error) {
	if c == nil {
		return CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: catalog codec is required")
	}
	if err := validateRegistration(registration); err != nil {
		return CatalogDispatchDTO{}, err
	}
	record, ok := c.byRegistration[catalogBindingKey{kind: registration.Kind(), id: strings.TrimSpace(registration.ID())}]
	if !ok {
		return CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: catalog mapping missing for %s/%s", registration.Kind(), registration.ID())
	}
	if err := record.validateRegistration(registration); err != nil {
		return CatalogDispatchDTO{}, err
	}
	payloadBytes, err := c.typed.Encode(ctx, registration, message)
	if err != nil {
		return CatalogDispatchDTO{}, err
	}
	payload := make(map[string]any)
	if decodeErr := json.Unmarshal(payloadBytes, &payload); decodeErr != nil {
		return CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: catalog payload must be a structured object: %w", decodeErr)
	}
	optionsDTO, err := encodeCatalogOptions(options)
	if err != nil {
		return CatalogDispatchDTO{}, err
	}
	return CatalogDispatchDTO{
		Version: CatalogWireVersion, SchemaVersion: record.binding.SchemaVersion,
		CommandID: record.binding.CatalogID, HandlerKind: string(record.binding.Kind),
		Payload: payload, Options: optionsDTO,
	}, nil
}

func (c *JSONCatalogCodec) DecodeCatalog(ctx context.Context, dto CatalogDispatchDTO, provider command.RegistrationProvider) (command.CommandDispatchRequest, command.MessageRegistration, any, error) {
	if c == nil {
		return command.CommandDispatchRequest{}, nil, nil, fmt.Errorf("go-command adapter: catalog codec is required")
	}
	if provider == nil || isTypedNil(provider) {
		return command.CommandDispatchRequest{}, nil, nil, command.NewRegistrationProviderNotConfiguredError()
	}
	if strings.TrimSpace(dto.Version) != CatalogWireVersion {
		return command.CommandDispatchRequest{}, nil, nil, fmt.Errorf("go-command adapter: unsupported catalog wire version %q", dto.Version)
	}
	kind := command.HandlerKind(strings.TrimSpace(dto.HandlerKind))
	if kind == "" {
		kind = command.HandlerKindCommand
	}
	record, ok := c.byCatalogID[catalogBindingKey{kind: kind, id: strings.TrimSpace(dto.CommandID)}]
	if !ok {
		return command.CommandDispatchRequest{}, nil, nil, command.NewRegistrationNotFoundError(kind, dto.CommandID)
	}
	if strings.TrimSpace(dto.SchemaVersion) != record.binding.SchemaVersion {
		return command.CommandDispatchRequest{}, nil, nil, fmt.Errorf("go-command adapter: unsupported catalog schema version %q", dto.SchemaVersion)
	}
	registration, ok := provider.RegistrationByID(kind, record.binding.RegistrationID)
	if !ok {
		return command.CommandDispatchRequest{}, nil, nil, command.NewRegistrationNotFoundError(kind, record.binding.RegistrationID)
	}
	if err := record.validateRegistration(registration); err != nil {
		return command.CommandDispatchRequest{}, nil, nil, err
	}
	options, err := decodeCatalogOptions(dto.Options)
	if err != nil {
		return command.CommandDispatchRequest{}, nil, nil, err
	}
	clonedPayload, err := cloneAnyMap(dto.Payload)
	if err != nil {
		return command.CommandDispatchRequest{}, nil, nil, err
	}
	request := command.CommandDispatchRequest{
		CommandID: registration.ID(), Payload: clonedPayload,
		IDs: append([]string(nil), dto.IDs...), Options: options,
	}
	if validationErr := command.ValidateCommandDispatchRequest(request); validationErr != nil {
		return command.CommandDispatchRequest{}, nil, nil, validationErr
	}
	payload, err := json.Marshal(dto.Payload)
	if err != nil {
		return command.CommandDispatchRequest{}, nil, nil, fmt.Errorf("go-command adapter: encode catalog payload: %w", err)
	}
	message, err := c.typed.Decode(ctx, registration, payload)
	if err != nil {
		return command.CommandDispatchRequest{}, nil, nil, err
	}
	return request, registration, message, nil
}

func (c *JSONCatalogCodec) ValidateCoverage(registrations ...command.MessageRegistration) error {
	missing := make([]string, 0)
	for _, registration := range registrations {
		if err := validateRegistration(registration); err != nil {
			return err
		}
		key := catalogBindingKey{kind: registration.Kind(), id: strings.TrimSpace(registration.ID())}
		record, ok := c.byRegistration[key]
		if !ok {
			missing = append(missing, fmt.Sprintf("%s/%s", registration.Kind(), registration.ID()))
			continue
		}
		if err := record.validateRegistration(registration); err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("go-command adapter: catalog coverage missing for %s", strings.Join(missing, ", "))
}

func (c *JSONCatalogCodec) AllowsEventToCommand(kind command.HandlerKind, catalogID string) bool {
	if c == nil {
		return false
	}
	record, ok := c.byCatalogID[catalogBindingKey{kind: kind, id: strings.TrimSpace(catalogID)}]
	return ok && record.binding.AllowEventToCommand
}

func encodeCatalogOptions(options command.DispatchOptions) (CatalogOptionsDTO, error) {
	mode := command.NormalizeExecutionMode(options.Mode)
	if mode == "" {
		mode = command.ExecutionModeInline
	}
	if err := command.ValidateDispatchOptions(mode, options); err != nil {
		return CatalogOptionsDTO{}, err
	}
	dto := CatalogOptionsDTO{
		Mode: string(mode), IdempotencyKey: strings.TrimSpace(options.IdempotencyKey),
		DedupPolicy: string(command.NormalizeDedupPolicy(options.DedupPolicy)),
		DelayNanos:  options.Delay.Nanoseconds(), CorrelationID: strings.TrimSpace(options.CorrelationID),
	}
	if options.RunAt != nil {
		dto.RunAt = options.RunAt.UTC().Format(time.RFC3339Nano)
	}
	return dto, nil
}

func decodeCatalogOptions(dto CatalogOptionsDTO) (command.DispatchOptions, error) {
	mode, err := command.ParseExecutionMode(dto.Mode)
	if err != nil {
		return command.DispatchOptions{}, err
	}
	if mode == "" {
		mode = command.ExecutionModeInline
	}
	policy, err := command.ParseDedupPolicy(dto.DedupPolicy)
	if err != nil {
		return command.DispatchOptions{}, err
	}
	if dto.DelayNanos < 0 {
		return command.DispatchOptions{}, fmt.Errorf("go-command adapter: delay_nanos must be non-negative")
	}
	options := command.DispatchOptions{
		Mode: mode, IdempotencyKey: strings.TrimSpace(dto.IdempotencyKey), DedupPolicy: policy,
		Delay:         time.Duration(dto.DelayNanos),
		CorrelationID: strings.TrimSpace(dto.CorrelationID),
	}
	if strings.TrimSpace(dto.RunAt) != "" {
		runAt, err := time.Parse(time.RFC3339Nano, dto.RunAt)
		if err != nil {
			return command.DispatchOptions{}, fmt.Errorf("go-command adapter: invalid run_at: %w", err)
		}
		options.RunAt = &runAt
	}
	if err := command.ValidateDispatchOptions(mode, options); err != nil {
		return command.DispatchOptions{}, err
	}
	return options, nil
}

func cloneAnyMap(source map[string]any) (map[string]any, error) {
	if source == nil {
		return map[string]any{}, nil
	}
	data, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("go-command adapter: catalog payload is not JSON-compatible: %w", err)
	}
	var cloned map[string]any
	if decodeErr := json.Unmarshal(data, &cloned); decodeErr != nil {
		return nil, fmt.Errorf("go-command adapter: clone catalog payload: %w", decodeErr)
	}
	return cloned, nil
}

func (r catalogBindingRecord) validateRegistration(registration command.MessageRegistration) error {
	if err := validateRegistration(registration); err != nil {
		return err
	}
	if registration.MessageType() != r.messageType || registration.RequestType() != r.requestType || registration.ResultType() != r.resultType {
		return command.NewRegistrationInvalidError("catalog registration is incompatible with its validated binding", map[string]any{
			"registration_id": registration.ID(), "handler_kind": registration.Kind(),
		})
	}
	return nil
}

type CatalogIngressExecutor interface {
	ExecuteCatalog(context.Context, command.CommandDispatchRequest, command.MessageRegistration, any, IngressMetadata) (command.DispatchOutcome, error)
}

type DefaultCatalogExecutor struct {
	Typed IngressExecutor
}

func (e DefaultCatalogExecutor) ExecuteCatalog(ctx context.Context, request command.CommandDispatchRequest, registration command.MessageRegistration, message any, _ IngressMetadata) (command.DispatchOutcome, error) {
	if e.Typed == nil || isTypedNil(e.Typed) {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: typed ingress executor is required")
	}
	return e.Typed.ExecuteInbound(ctx, registration, message, request.Options)
}

type IngressMetadata struct {
	EnvelopeID    string
	EnvelopeKind  messaging.Kind
	LogicalRoute  string
	DeliveryID    string
	Attempt       int
	CorrelationID string
	CausationID   string
}

type CatalogIngress struct {
	provider     command.RegistrationProvider
	codec        CatalogCodec
	executor     CatalogIngressExecutor
	logicalRoute string
}

func NewCatalogIngress(provider command.RegistrationProvider, codec CatalogCodec, executor CatalogIngressExecutor) (*CatalogIngress, error) {
	if provider == nil || isTypedNil(provider) {
		return nil, command.NewRegistrationProviderNotConfiguredError()
	}
	if codec == nil || isTypedNil(codec) {
		return nil, fmt.Errorf("go-command adapter: catalog codec is required")
	}
	if executor == nil || isTypedNil(executor) {
		return nil, fmt.Errorf("go-command adapter: catalog executor is required")
	}
	return &CatalogIngress{provider: provider, codec: codec, executor: executor}, nil
}

func (i *CatalogIngress) Execute(ctx context.Context, delivery messaging.Delivery) (IngressResult, error) {
	if i == nil || delivery == nil || isTypedNil(delivery) {
		return IngressResult{}, fmt.Errorf("go-command adapter: catalog ingress and delivery are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	envelope, dto, err := decodeCatalogDelivery(delivery)
	if err != nil {
		return IngressResult{}, err
	}
	request, registration, message, err := i.codec.DecodeCatalog(ctx, dto, i.provider)
	if err != nil {
		return IngressResult{}, err
	}
	if authorizationErr := i.authorizeRequest(envelope, dto, registration, &request); authorizationErr != nil {
		return IngressResult{}, authorizationErr
	}
	info := delivery.Info()
	ctx = contextWithDeliveryProvenance(ctx, i.logicalRoute, envelope, info)
	ctx, cancel, err := contextWithEnvelopeDeadline(ctx, envelope)
	if err != nil {
		return IngressResult{Registration: registration, Message: message}, err
	}
	defer cancel()
	metadata := IngressMetadata{
		EnvelopeID: envelope.ID, EnvelopeKind: envelope.Kind,
		LogicalRoute: i.logicalRoute,
		DeliveryID:   info.DeliveryID, Attempt: info.Attempt,
		CorrelationID: envelope.CorrelationID, CausationID: envelope.CausationID,
	}
	outcome, err := i.executor.ExecuteCatalog(ctx, request, registration, message, metadata)
	if err != nil {
		return IngressResult{Registration: registration, Message: message}, err
	}
	return IngressResult{Registration: registration, Message: message, Outcome: outcome}, nil
}

func (i *CatalogIngress) HandlerWith(mapper ErrorMapper) messaging.Handler {
	if mapper == nil || isTypedNil(mapper) {
		mapper = DefaultErrorMapper{}
	}
	return func(ctx context.Context, delivery messaging.Delivery) messaging.HandleResult {
		_, err := i.Execute(ctx, delivery)
		attempt := 0
		if delivery != nil && !isTypedNil(delivery) {
			attempt = delivery.Info().Attempt
		}
		return mapper.Map(err, attempt)
	}
}

func decodeCatalogDelivery(delivery messaging.Delivery) (messaging.Envelope, CatalogDispatchDTO, error) {
	envelope := delivery.Envelope()
	if err := envelope.Validate(); err != nil {
		return messaging.Envelope{}, CatalogDispatchDTO{}, err
	}
	if envelope.Type != CatalogEnvelopeType {
		return messaging.Envelope{}, CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: unexpected catalog envelope type %q", envelope.Type)
	}
	if envelope.Kind != messaging.KindCommand && envelope.Kind != messaging.KindEvent {
		return messaging.Envelope{}, CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: catalog envelope kind %q is not supported", envelope.Kind)
	}
	var dto CatalogDispatchDTO
	if err := json.Unmarshal(envelope.Payload, &dto); err != nil {
		return messaging.Envelope{}, CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: decode catalog dispatch: %w", err)
	}
	return envelope, dto, nil
}

func (i *CatalogIngress) authorizeRequest(envelope messaging.Envelope, dto CatalogDispatchDTO, registration command.MessageRegistration, request *command.CommandDispatchRequest) error {
	if envelope.Kind == messaging.KindEvent && !i.codec.AllowsEventToCommand(registration.Kind(), dto.CommandID) {
		return fmt.Errorf("go-command adapter: event-to-command catalog binding is not authorized")
	}
	if request.Options.CorrelationID != "" && envelope.CorrelationID != "" && request.Options.CorrelationID != envelope.CorrelationID {
		return fmt.Errorf("go-command adapter: catalog correlation id mismatch")
	}
	if request.Options.CorrelationID == "" {
		request.Options.CorrelationID = envelope.CorrelationID
	}
	return nil
}
