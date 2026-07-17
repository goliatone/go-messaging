package commandadapter

import (
	"context"
	"reflect"
	"testing"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

type ingressFunc func(context.Context, messaging.Delivery) (IngressResult, error)

func (f ingressFunc) Execute(ctx context.Context, delivery messaging.Delivery) (IngressResult, error) {
	return f(ctx, delivery)
}

func TestReplyingIngressPublishesExecutingProcessOutcome(t *testing.T) {
	registration := ingressRegistration{
		id: "test.lookup", messageType: "test.lookup", kind: command.HandlerKindQuery,
		request: reflect.TypeFor[lookupMessage](), result: reflect.TypeFor[string](),
		newMessage: func() any { return &lookupMessage{} },
	}
	driver := newRemoteTestDriver()
	var reply messaging.Envelope
	driver.publish = func(_ context.Context, _ messaging.Destination, envelope messaging.Envelope) (messaging.PublishResult, error) {
		reply = envelope
		return messaging.PublishResult{Outcome: messaging.PublishAccepted}, nil
	}
	router := remoteTestRouter(t, driver, false)
	worker := ReplyingIngress{
		Ingress: ingressFunc(func(_ context.Context, _ messaging.Delivery) (IngressResult, error) {
			return IngressResult{Registration: registration, Outcome: command.DispatchOutcome{
				Receipt: command.DispatchReceipt{Accepted: true, Mode: command.ExecutionModeInline, CommandID: registration.ID(), CorrelationID: "correlation-1"},
				Result:  "found:42", ResultPresent: true,
			}}, nil
		}),
		Replies: ReplyPublisher{Router: router},
	}
	request := messaging.NewEnvelope("request-1", registration.MessageType(), messaging.KindQuery, "1", "application/json", []byte(`{"id":"42"}`), nil)
	request.CorrelationID = "correlation-1"
	request.ReplyTo = "reply"
	result := worker.Handler(context.Background(), messaging.NewDelivery(request, messaging.DeliveryInfo{Attempt: 1}))
	if result.Disposition != messaging.DispositionComplete || reply.Kind != messaging.KindReply || reply.CorrelationID != request.CorrelationID || reply.CausationID != request.ID {
		t.Fatalf("result=%#v reply=%#v", result, reply)
	}
	decoded, err := (JSONReplyCodec{}).Decode(context.Background(), registration, reply)
	if err != nil || decoded.Result != "found:42" {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
}
