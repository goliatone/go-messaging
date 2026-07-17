package commandadapter

import (
	"context"
	"reflect"
	"testing"
	"time"

	command "github.com/goliatone/go-command"
	"github.com/goliatone/go-command/dispatcher"
	gerrors "github.com/goliatone/go-errors"
	messaging "github.com/goliatone/go-messaging"
)

type createMessage struct {
	Name string `json:"name"`
}

func (createMessage) Type() string { return "test.create" }

type lookupMessage struct {
	ID string `json:"id"`
}

func (lookupMessage) Type() string { return "test.lookup" }

type ingressRegistration struct {
	id          string
	messageType string
	kind        command.HandlerKind
	request     reflect.Type
	result      reflect.Type
	newMessage  func() any
}

func (r ingressRegistration) ID() string                { return r.id }
func (r ingressRegistration) MessageType() string       { return r.messageType }
func (r ingressRegistration) Kind() command.HandlerKind { return r.kind }
func (r ingressRegistration) NewMessage() any           { return r.newMessage() }
func (r ingressRegistration) RequestType() reflect.Type { return r.request }
func (r ingressRegistration) ResultType() reflect.Type  { return r.result }

func TestTypedIngressUsesExistingRuntimeSubscriptions(t *testing.T) {
	commandRegistration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	queryRegistration := ingressRegistration{
		id: "test.lookup", messageType: "test.lookup", kind: command.HandlerKindQuery,
		request: reflect.TypeFor[lookupMessage](), result: reflect.TypeFor[string](),
		newMessage: func() any { return &lookupMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(commandRegistration, queryRegistration)
	if err != nil {
		t.Fatal(err)
	}
	runtime := dispatcher.NewRuntime()
	var created string
	dispatcher.SubscribeCommandTo(runtime, command.CommandFunc[createMessage](func(_ context.Context, message createMessage) error {
		created = message.Name
		return nil
	}))
	dispatcher.SubscribeQueryTo(runtime, command.QueryFunc[lookupMessage, string](func(_ context.Context, message lookupMessage) (string, error) {
		return "found:" + message.ID, nil
	}))
	if attachErr := runtime.AttachRegistrationProvider(provider); attachErr != nil {
		t.Fatal(attachErr)
	}
	ingress, err := NewTypedIngress(provider, RuntimeExecutor{Runtime: runtime}, JSONTypedCodec{})
	if err != nil {
		t.Fatal(err)
	}

	commandResult, err := ingress.Execute(context.Background(), deliveryFor(messaging.KindCommand, "test.create", []byte(`{"name":"Ada"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if created != "Ada" || !commandResult.Outcome.Receipt.Accepted {
		t.Fatalf("created=%q outcome=%#v", created, commandResult.Outcome)
	}
	queryResult, err := ingress.Execute(context.Background(), deliveryFor(messaging.KindQuery, "test.lookup", []byte(`{"id":"42"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if !queryResult.Outcome.ResultPresent || queryResult.Outcome.Result != "found:42" {
		t.Fatalf("query outcome %#v", queryResult.Outcome)
	}
}

func TestTypedIngressPreservesStructuredHandlerFailure(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	runtime := dispatcher.NewRuntime()
	dispatcher.SubscribeCommandTo(runtime, command.CommandFunc[createMessage](func(context.Context, createMessage) error {
		return gerrors.New("denied", gerrors.CategoryAuthz).WithTextCode("CREATE_DENIED")
	}))
	if attachErr := runtime.AttachRegistrationProvider(provider); attachErr != nil {
		t.Fatal(attachErr)
	}
	ingress, err := NewTypedIngress(provider, RuntimeExecutor{Runtime: runtime}, JSONTypedCodec{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ingress.Execute(context.Background(), deliveryFor(messaging.KindCommand, "test.create", []byte(`{"name":"Ada"}`)))
	var structured *gerrors.Error
	if !gerrors.As(err, &structured) || structured.TextCode != "HANDLER_EXECUTION_FAILED" || structured.Category != gerrors.CategoryAuthz {
		t.Fatalf("structured failure was not preserved: %v", err)
	}
}

func TestTypedIngressRejectsEventWithoutExplicitBinding(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := NewTypedIngress(provider, executorFunc(func(context.Context, command.MessageRegistration, any, command.DispatchOptions) (command.DispatchOutcome, error) {
		return command.DispatchOutcome{}, nil
	}), JSONTypedCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingress.Execute(context.Background(), deliveryFor(messaging.KindEvent, "test.create", []byte(`{}`))); err == nil {
		t.Fatal("expected event policy rejection")
	}
}

type executorFunc func(context.Context, command.MessageRegistration, any, command.DispatchOptions) (command.DispatchOutcome, error)

func (f executorFunc) ExecuteInbound(ctx context.Context, registration command.MessageRegistration, message any, options command.DispatchOptions) (command.DispatchOutcome, error) {
	return f(ctx, registration, message, options)
}

func deliveryFor(kind messaging.Kind, messageType string, payload []byte) messaging.Delivery {
	envelope := messaging.NewEnvelope("delivery-1", messageType, kind, "1", "application/json", payload, nil)
	envelope.CorrelationID = "correlation-1"
	return messaging.NewDelivery(envelope, messaging.DeliveryInfo{DeliveryID: "delivery-1", Attempt: 1, ReceivedAt: time.Now()})
}
