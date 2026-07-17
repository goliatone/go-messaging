package commandadapter

import (
	"context"
	"reflect"
	"testing"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

func TestTypedEventToCommandRequiresExplicitBinding(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	ingress, err := NewTypedIngress(provider, executorFunc(func(_ context.Context, got command.MessageRegistration, message any, _ command.DispatchOptions) (command.DispatchOutcome, error) {
		decoded, ok := message.(*createMessage)
		called = ok && got.ID() == registration.ID() && decoded.Name == "Ada"
		return command.DispatchOutcome{}, nil
	}), JSONTypedCodec{})
	if err != nil {
		t.Fatal(err)
	}
	delivery := deliveryFor(messaging.KindEvent, "test.create", []byte(`{"name":"Ada"}`))

	if _, executeErr := ingress.Execute(context.Background(), delivery); executeErr == nil {
		t.Fatal("natural typed ingress accepted an event")
	}
	bindings, err := NewIngressBindings(IngressBinding{
		EnvelopeKind: messaging.KindEvent, MessageType: "test.create", HandlerKind: command.HandlerKindCommand,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := ingress.ExecuteBound(context.Background(), delivery, bindings); executeErr != nil {
		t.Fatal(executeErr)
	}
	if !called {
		t.Fatal("explicit event binding did not reach command registration")
	}
}
