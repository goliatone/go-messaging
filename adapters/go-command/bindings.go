package commandadapter

import (
	"context"
	"fmt"
	"strings"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

// IngressBinding authorizes a non-default envelope intent. Natural command
// and query mappings do not need entries. In particular, an event reaches a
// command registration only through an explicit event-to-command binding.
type IngressBinding struct {
	EnvelopeKind messaging.Kind
	MessageType  string
	HandlerKind  command.HandlerKind
}

type IngressBindings struct {
	bindings map[ingressBindingKey]command.HandlerKind
}

type ingressBindingKey struct {
	kind        messaging.Kind
	messageType string
}

func NewIngressBindings(bindings ...IngressBinding) (*IngressBindings, error) {
	resolved := make(map[ingressBindingKey]command.HandlerKind, len(bindings))
	for _, binding := range bindings {
		messageType := strings.TrimSpace(binding.MessageType)
		if messageType == "" {
			return nil, fmt.Errorf("go-command adapter: ingress binding message type is required")
		}
		if binding.EnvelopeKind != messaging.KindEvent || binding.HandlerKind != command.HandlerKindCommand {
			return nil, fmt.Errorf("go-command adapter: only explicit event-to-command reinterpretation is supported")
		}
		key := ingressBindingKey{kind: binding.EnvelopeKind, messageType: messageType}
		if existing, ok := resolved[key]; ok && existing != binding.HandlerKind {
			return nil, fmt.Errorf("go-command adapter: ambiguous ingress binding for %s", messageType)
		}
		resolved[key] = binding.HandlerKind
	}
	return &IngressBindings{bindings: resolved}, nil
}

func (b *IngressBindings) Resolve(envelope messaging.Envelope) (command.HandlerKind, bool) {
	if b == nil {
		return "", false
	}
	kind, ok := b.bindings[ingressBindingKey{kind: envelope.Kind, messageType: strings.TrimSpace(envelope.Type)}]
	return kind, ok
}

// ExecuteBound applies an explicit ingress intent before delegating to the
// normal typed codec and forced-local executor.
func (i *TypedIngress) ExecuteBound(ctx context.Context, delivery messaging.Delivery, bindings *IngressBindings) (IngressResult, error) {
	if delivery == nil || isTypedNil(delivery) {
		return IngressResult{}, fmt.Errorf("go-command adapter: delivery is required")
	}
	envelope := delivery.Envelope()
	if err := envelope.Validate(); err != nil {
		return IngressResult{}, err
	}
	kind, ok := bindings.Resolve(envelope)
	if !ok {
		return IngressResult{}, fmt.Errorf("go-command adapter: no authorized binding for %s/%s", envelope.Kind, envelope.Type)
	}
	return i.executeAs(ctx, delivery, envelope, kind)
}
