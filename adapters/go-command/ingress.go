package commandadapter

import (
	"context"
	"fmt"
	"strings"
	"time"

	command "github.com/goliatone/go-command"
	"github.com/goliatone/go-command/dispatcher"
	messaging "github.com/goliatone/go-messaging"
)

const (
	HeaderExecutionMode  = "go-command.mode"
	HeaderIdempotencyKey = "go-command.idempotency-key"
	HeaderDedupPolicy    = "go-command.dedup-policy"
)

// IngressExecutor is the trusted application boundary used after an envelope
// has been validated and decoded into a registered message.
type IngressExecutor interface {
	ExecuteInbound(context.Context, command.MessageRegistration, any, command.DispatchOptions) (command.DispatchOutcome, error)
}

// RuntimeExecutor invokes the existing go-command subscription mux through
// Runtime.InvokeLocal, which installs the forced-local routing guard.
type RuntimeExecutor struct {
	Runtime *dispatcher.Runtime
}

func (e RuntimeExecutor) ExecuteInbound(
	ctx context.Context,
	registration command.MessageRegistration,
	message any,
	options command.DispatchOptions,
) (command.DispatchOutcome, error) {
	if e.Runtime == nil {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: dispatcher runtime is required")
	}
	return e.Runtime.InvokeLocal(ctx, registration, message, options)
}

type TypedIngress struct {
	provider command.RegistrationProvider
	codec    TypedCodec
	executor IngressExecutor
}

type IngressResult struct {
	Registration command.MessageRegistration
	Message      any
	Outcome      command.DispatchOutcome
}

func NewTypedIngress(provider command.RegistrationProvider, executor IngressExecutor, codec TypedCodec) (*TypedIngress, error) {
	if provider == nil || isTypedNil(provider) {
		return nil, command.NewRegistrationProviderNotConfiguredError()
	}
	if executor == nil || isTypedNil(executor) {
		return nil, fmt.Errorf("go-command adapter: ingress executor is required")
	}
	if codec == nil || isTypedNil(codec) {
		codec = JSONTypedCodec{}
	}
	return &TypedIngress{provider: provider, codec: codec, executor: executor}, nil
}

// Execute validates and decodes one typed command/query delivery. Event
// reinterpretation is intentionally excluded here and is enabled only by an
// explicit binding in the policy-aware ingress layer.
func (i *TypedIngress) Execute(ctx context.Context, delivery messaging.Delivery) (IngressResult, error) {
	if i == nil || i.provider == nil || i.executor == nil {
		return IngressResult{}, fmt.Errorf("go-command adapter: typed ingress is not configured")
	}
	if delivery == nil || isTypedNil(delivery) {
		return IngressResult{}, fmt.Errorf("go-command adapter: delivery is required")
	}
	envelope := delivery.Envelope()
	if err := envelope.Validate(); err != nil {
		return IngressResult{}, err
	}
	kind, err := commandKind(envelope.Kind)
	if err != nil {
		return IngressResult{}, err
	}
	return i.executeAs(ctx, delivery, envelope, kind)
}

func (i *TypedIngress) executeAs(ctx context.Context, delivery messaging.Delivery, envelope messaging.Envelope, kind command.HandlerKind) (IngressResult, error) {
	registration, ok := i.provider.RegistrationByMessageType(kind, envelope.Type)
	if !ok {
		return IngressResult{}, command.NewRegistrationNotFoundError(kind, envelope.Type)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	message, err := i.codec.Decode(ctx, registration, envelope.Payload)
	if err != nil {
		return IngressResult{}, err
	}
	options, err := dispatchOptionsFromEnvelope(envelope)
	if err != nil {
		return IngressResult{}, err
	}

	if !envelope.Deadline.IsZero() {
		if !envelope.Deadline.After(time.Now()) {
			return IngressResult{}, context.DeadlineExceeded
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, envelope.Deadline)
		defer cancel()
	}
	outcome, err := i.executor.ExecuteInbound(ctx, registration, message, options)
	if err != nil {
		return IngressResult{Registration: registration, Message: message}, err
	}
	return IngressResult{Registration: registration, Message: message, Outcome: outcome}, nil
}

// Handler adapts typed ingress to the transport-neutral delivery contract.
// More specific retry/dead-letter policy can decorate Execute or replace this
// conservative reject-on-error mapping.
func (i *TypedIngress) Handler(ctx context.Context, delivery messaging.Delivery) messaging.HandleResult {
	if _, err := i.Execute(ctx, delivery); err != nil {
		return messaging.Reject(err)
	}
	return messaging.Complete()
}

func commandKind(kind messaging.Kind) (command.HandlerKind, error) {
	switch kind {
	case messaging.KindCommand:
		return command.HandlerKindCommand, nil
	case messaging.KindQuery:
		return command.HandlerKindQuery, nil
	default:
		return "", fmt.Errorf("go-command adapter: envelope kind %q is not authorized for typed ingress", kind)
	}
}

func dispatchOptionsFromEnvelope(envelope messaging.Envelope) (command.DispatchOptions, error) {
	mode, err := command.ParseExecutionMode(envelope.Headers[HeaderExecutionMode])
	if err != nil {
		return command.DispatchOptions{}, err
	}
	if mode == "" {
		mode = command.ExecutionModeInline
	}
	policy, err := command.ParseDedupPolicy(envelope.Headers[HeaderDedupPolicy])
	if err != nil {
		return command.DispatchOptions{}, err
	}
	options := command.DispatchOptions{
		Mode:           mode,
		CorrelationID:  strings.TrimSpace(envelope.CorrelationID),
		IdempotencyKey: strings.TrimSpace(envelope.Headers[HeaderIdempotencyKey]),
		DedupPolicy:    policy,
	}
	if err := command.ValidateDispatchOptions(mode, options); err != nil {
		return command.DispatchOptions{}, err
	}
	return options, nil
}
