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
	provider, _ := command.NewMessageRegistrationIndex(registration)
	called := false
	ingress, _ := NewTypedIngress(provider, executorFunc(func(_ context.Context, got command.MessageRegistration, message any, _ command.DispatchOptions) (command.DispatchOutcome, error) {
		called = got.ID() == registration.ID() && message.(*createMessage).Name == "Ada"
		return command.DispatchOutcome{}, nil
	}), JSONTypedCodec{})
	delivery := deliveryFor(messaging.KindEvent, "test.create", []byte(`{"name":"Ada"}`))

	if _, err := ingress.Execute(context.Background(), delivery); err == nil {
		t.Fatal("natural typed ingress accepted an event")
	}
	bindings, err := NewIngressBindings(IngressBinding{
		EnvelopeKind: messaging.KindEvent, MessageType: "test.create", HandlerKind: command.HandlerKindCommand,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingress.ExecuteBound(context.Background(), delivery, bindings); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("explicit event binding did not reach command registration")
	}
}
