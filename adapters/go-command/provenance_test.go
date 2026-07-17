package commandadapter

import (
	"context"
	"reflect"
	"testing"
	"time"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

func TestTypedIngressPropagatesBoundedDeliveryProvenance(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	var got command.DispatchProvenance
	ingress, err := NewTypedIngress(provider, executorFunc(func(ctx context.Context, _ command.MessageRegistration, _ any, _ command.DispatchOptions) (command.DispatchOutcome, error) {
		got, _ = command.DispatchProvenanceFromContext(ctx)
		return command.DispatchOutcome{}, nil
	}), JSONTypedCodec{})
	if err != nil {
		t.Fatal(err)
	}
	ingress = ingress.ForRoute("commands")
	envelope := messaging.NewEnvelope("envelope-1", "test.create", messaging.KindCommand, "1", "application/json", []byte(`{"name":"Ada"}`), nil)
	envelope.CorrelationID = "correlation-1"
	envelope.CausationID = "cause-1"
	delivery := messaging.NewDelivery(envelope, messaging.DeliveryInfo{Transport: "valkey", DeliveryID: "delivery-9", Attempt: 3, ReceivedAt: time.Now()})
	if _, err := ingress.Execute(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if got.Route != "commands" || got.DeliveryID != "delivery-9" || got.Attempt != 3 || got.CorrelationID != "correlation-1" || got.CausationID != "cause-1" {
		t.Fatalf("provenance %#v", got)
	}
}
