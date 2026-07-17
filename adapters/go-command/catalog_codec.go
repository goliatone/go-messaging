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
	CatalogWireVersion = "1"
	CatalogEnvelopeType = "go-command.catalog"
)

// CatalogOptionsDTO is the explicit allowlisted wire representation of
// DispatchOptions. DispatchOptions itself is intentionally never serialized.
type CatalogOptionsDTO struct {
	Mode           string `json:"mode,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	DedupPolicy    string `json:"dedup_policy,omitempty"`
	DelayMillis    int64  `json:"delay_millis,omitempty"`
	RunAt          string `json:"run_at,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
}

type CatalogDispatchDTO struct {
	Version     string            `json:"version"`
	CommandID   string            `json:"command_id"`
	HandlerKind string            `json:"handler_kind,omitempty"`
	Payload     map[string]any    `json:"payload"`
	IDs         []string          `json:"ids,omitempty"`
	Options     CatalogOptionsDTO `json:"options,omitempty"`
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
	byCatalogID     map[catalogBindingKey]CatalogBinding
	byRegistration  map[catalogBindingKey]CatalogBinding
	typed            TypedCodec
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
		byCatalogID:    make(map[catalogBindingKey]CatalogBinding, len(bindings)),
		byRegistration: make(map[catalogBindingKey]CatalogBinding, len(bindings)),
		typed:          typed,
	}
	for _, binding := range bindings {
		binding.CatalogID = strings.TrimSpace(binding.CatalogID)
		binding.RegistrationID = strings.TrimSpace(binding.RegistrationID)
		binding.SchemaVersion = strings.TrimSpace(binding.SchemaVersion)
		if binding.CatalogID == "" {
			binding.CatalogID = binding.RegistrationID
		}
		if binding.CatalogID == "" || binding.RegistrationID == "" || binding.SchemaVersion == "" {
			return nil, fmt.Errorf("go-command adapter: catalog id, registration id, and schema version are required")
		}
		if binding.Kind != command.HandlerKindCommand && binding.Kind != command.HandlerKindQuery {
			return nil, fmt.Errorf("go-command adapter: invalid catalog handler kind %q", binding.Kind)
		}
		if binding.AllowEventToCommand && binding.Kind != command.HandlerKindCommand {
			return nil, fmt.Errorf("go-command adapter: event reinterpretation requires a command binding")
		}
		registration, ok := provider.RegistrationByID(binding.Kind, binding.RegistrationID)
		if !ok {
			return nil, command.NewRegistrationNotFoundError(binding.Kind, binding.RegistrationID)
		}
		if err := validateRegistration(registration); err != nil {
			return nil, err
		}
		catalogKey := catalogBindingKey{kind: binding.Kind, id: binding.CatalogID}
		registrationKey := catalogBindingKey{kind: binding.Kind, id: binding.RegistrationID}
		if _, exists := codec.byCatalogID[catalogKey]; exists {
			return nil, fmt.Errorf("go-command adapter: ambiguous catalog id %q for %s", binding.CatalogID, binding.Kind)
		}
		if _, exists := codec.byRegistration[registrationKey]; exists {
			return nil, fmt.Errorf("go-command adapter: ambiguous registration mapping %q for %s", binding.RegistrationID, binding.Kind)
		}
		codec.byCatalogID[catalogKey] = binding
		codec.byRegistration[registrationKey] = binding
	}
	return codec, nil
}

func (c *JSONCatalogCodec) EncodeCatalog(ctx context.Context, registration command.MessageRegistration, message any, options command.DispatchOptions) (CatalogDispatchDTO, error) {
	if c == nil {
		return CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: catalog codec is required")
	}
	if err := validateRegistration(registration); err != nil {
		return CatalogDispatchDTO{}, err
	}
	binding, ok := c.byRegistration[catalogBindingKey{kind: registration.Kind(), id: strings.TrimSpace(registration.ID())}]
	if !ok {
		return CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: catalog mapping missing for %s/%s", registration.Kind(), registration.ID())
	}
	payloadBytes, err := c.typed.Encode(ctx, registration, message)
	if err != nil {
		return CatalogDispatchDTO{}, err
	}
	payload := make(map[string]any)
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return CatalogDispatchDTO{}, fmt.Errorf("go-command adapter: catalog payload must be a structured object: %w", err)
	}
	optionsDTO, err := encodeCatalogOptions(options)
	if err != nil {
		return CatalogDispatchDTO{}, err
	}
	return CatalogDispatchDTO{
		Version: CatalogWireVersion, CommandID: binding.CatalogID,
		HandlerKind: string(binding.Kind), Payload: payload, Options: optionsDTO,
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
	binding, ok := c.byCatalogID[catalogBindingKey{kind: kind, id: strings.TrimSpace(dto.CommandID)}]
	if !ok {
		return command.CommandDispatchRequest{}, nil, nil, command.NewRegistrationNotFoundError(kind, dto.CommandID)
	}
	registration, ok := provider.RegistrationByID(kind, binding.RegistrationID)
	if !ok {
		return command.CommandDispatchRequest{}, nil, nil, command.NewRegistrationNotFoundError(kind, binding.RegistrationID)
	}
	options, err := decodeCatalogOptions(dto.Options)
	if err != nil {
		return command.CommandDispatchRequest{}, nil, nil, err
	}
	request := command.CommandDispatchRequest{
		CommandID: registration.ID(), Payload: cloneAnyMap(dto.Payload),
		IDs: append([]string(nil), dto.IDs...), Options: options,
	}
	if err := command.ValidateCommandDispatchRequest(request); err != nil {
		return command.CommandDispatchRequest{}, nil, nil, err
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
		if _, ok := c.byRegistration[key]; !ok {
			missing = append(missing, fmt.Sprintf("%s/%s", registration.Kind(), registration.ID()))
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
	binding, ok := c.byCatalogID[catalogBindingKey{kind: kind, id: strings.TrimSpace(catalogID)}]
	return ok && binding.AllowEventToCommand
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
		DelayMillis: options.Delay.Milliseconds(), CorrelationID: strings.TrimSpace(options.CorrelationID),
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
	if dto.DelayMillis < 0 {
		return command.DispatchOptions{}, fmt.Errorf("go-command adapter: delay_millis must be non-negative")
	}
	options := command.DispatchOptions{
		Mode: mode, IdempotencyKey: strings.TrimSpace(dto.IdempotencyKey), DedupPolicy: policy,
		Delay: time.Duration(dto.DelayMillis) * time.Millisecond,
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

func cloneAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return map[string]any{}
	}
	data, err := json.Marshal(source)
	if err != nil {
		return map[string]any{}
	}
	var cloned map[string]any
	if err := json.Unmarshal(data, &cloned); err != nil {
		return map[string]any{}
	}
	return cloned
}

func registrationResultName(registration command.MessageRegistration) string {
	if registration == nil || registration.ResultType() == nil {
		return ""
	}
	return reflect.TypeOf(reflect.New(registration.ResultType()).Elem().Interface()).String()
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
	provider command.RegistrationProvider
	codec    CatalogCodec
	executor CatalogIngressExecutor
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
	envelope := delivery.Envelope()
	if err := envelope.Validate(); err != nil {
		return IngressResult{}, err
	}
	if envelope.Type != CatalogEnvelopeType {
		return IngressResult{}, fmt.Errorf("go-command adapter: unexpected catalog envelope type %q", envelope.Type)
	}
	if envelope.Kind != messaging.KindCommand && envelope.Kind != messaging.KindEvent {
		return IngressResult{}, fmt.Errorf("go-command adapter: catalog envelope kind %q is not supported", envelope.Kind)
	}
	var dto CatalogDispatchDTO
	if err := json.Unmarshal(envelope.Payload, &dto); err != nil {
		return IngressResult{}, fmt.Errorf("go-command adapter: decode catalog dispatch: %w", err)
	}
	request, registration, message, err := i.codec.DecodeCatalog(ctx, dto, i.provider)
	if err != nil {
		return IngressResult{}, err
	}
	if envelope.Kind == messaging.KindEvent && !i.codec.AllowsEventToCommand(registration.Kind(), dto.CommandID) {
		return IngressResult{}, fmt.Errorf("go-command adapter: event-to-command catalog binding is not authorized")
	}
	if request.Options.CorrelationID != "" && envelope.CorrelationID != "" && request.Options.CorrelationID != envelope.CorrelationID {
		return IngressResult{}, fmt.Errorf("go-command adapter: catalog correlation id mismatch")
	}
	if request.Options.CorrelationID == "" {
		request.Options.CorrelationID = envelope.CorrelationID
	}
	info := delivery.Info()
	metadata := IngressMetadata{
		EnvelopeID: envelope.ID, EnvelopeKind: envelope.Kind,
		DeliveryID: info.DeliveryID, Attempt: info.Attempt,
		CorrelationID: envelope.CorrelationID, CausationID: envelope.CausationID,
	}
	outcome, err := i.executor.ExecuteCatalog(ctx, request, registration, message, metadata)
	if err != nil {
		return IngressResult{Registration: registration, Message: message}, err
	}
	return IngressResult{Registration: registration, Message: message, Outcome: outcome}, nil
}
