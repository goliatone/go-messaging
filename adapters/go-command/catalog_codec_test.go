package commandadapter

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

func TestCatalogCodecRoundTripsExplicitWireDTO(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, _ := command.NewMessageRegistrationIndex(registration)
	codec, err := NewJSONCatalogCodec(provider, JSONTypedCodec{}, CatalogBinding{
		CatalogID: "create", RegistrationID: "test.create", Kind: command.HandlerKindCommand, SchemaVersion: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	options := command.DispatchOptions{
		Mode: command.ExecutionModeQueued, IdempotencyKey: "idem-1",
		DedupPolicy: command.DedupPolicyDrop, Delay: 1500 * time.Microsecond,
		CorrelationID: "correlation-1", Metadata: map[string]any{"must_not_cross_wire": true},
	}
	dto, err := codec.EncodeCatalog(context.Background(), registration, createMessage{Name: "Ada"}, options)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(dto)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(wire)
	if strings.Contains(encoded, "must_not_cross_wire") || strings.Contains(encoded, "IdempotencyKey") || !strings.Contains(encoded, `"idempotency_key"`) {
		t.Fatalf("dispatch options leaked or explicit fields missing: %s", encoded)
	}
	request, gotRegistration, decoded, err := codec.DecodeCatalog(context.Background(), dto, provider)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := decoded.(*createMessage)
	if !ok || message.Name != "Ada" || gotRegistration.ID() != registration.ID() {
		t.Fatalf("decoded registration=%v message=%#v", gotRegistration, decoded)
	}
	if request.CommandID != "test.create" || request.Options.Delay != options.Delay || request.Options.IdempotencyKey != "idem-1" {
		t.Fatalf("request %#v", request)
	}
}

func TestCatalogCodecValidatesCoverageAndAmbiguity(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, _ := command.NewMessageRegistrationIndex(registration)
	codec, err := NewJSONCatalogCodec(provider, JSONTypedCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.ValidateCoverage(registration); err == nil {
		t.Fatal("expected missing coverage")
	}
	_, err = NewJSONCatalogCodec(provider, JSONTypedCodec{},
		CatalogBinding{CatalogID: "create", RegistrationID: "test.create", Kind: command.HandlerKindCommand, SchemaVersion: "1"},
		CatalogBinding{CatalogID: "create", RegistrationID: "test.create", Kind: command.HandlerKindCommand, SchemaVersion: "1"},
	)
	if err == nil {
		t.Fatal("expected ambiguous mapping rejection")
	}
}

func TestCatalogIngressRequiresExplicitEventAuthorizationAndUsesPolicyExecutor(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, _ := command.NewMessageRegistrationIndex(registration)
	newCodec := func(allow bool) *JSONCatalogCodec {
		codec, err := NewJSONCatalogCodec(provider, JSONTypedCodec{}, CatalogBinding{
			CatalogID: "create", RegistrationID: "test.create", Kind: command.HandlerKindCommand,
			SchemaVersion: "1", AllowEventToCommand: allow,
		})
		if err != nil {
			t.Fatal(err)
		}
		return codec
	}
	dto := CatalogDispatchDTO{
		Version: CatalogWireVersion, CommandID: "create", HandlerKind: string(command.HandlerKindCommand),
		Payload: map[string]any{"name": "Ada"}, Options: CatalogOptionsDTO{Mode: "inline", CorrelationID: "correlation-1"},
	}
	payload, _ := json.Marshal(dto)
	delivery := deliveryFor(messaging.KindEvent, CatalogEnvelopeType, payload)

	denied, _ := NewCatalogIngress(provider, newCodec(false), catalogExecutorFunc(func(context.Context, command.CommandDispatchRequest, command.MessageRegistration, any, IngressMetadata) (command.DispatchOutcome, error) {
		t.Fatal("denied event reached executor")
		return command.DispatchOutcome{}, nil
	}))
	if _, err := denied.Execute(context.Background(), delivery); err == nil {
		t.Fatal("expected event authorization failure")
	}

	called := false
	allowed, _ := NewCatalogIngress(provider, newCodec(true), catalogExecutorFunc(func(_ context.Context, request command.CommandDispatchRequest, registration command.MessageRegistration, message any, metadata IngressMetadata) (command.DispatchOutcome, error) {
		called = request.CommandID == "test.create" && registration.ID() == "test.create" && message.(*createMessage).Name == "Ada" && metadata.EnvelopeKind == messaging.KindEvent
		return command.DispatchOutcome{Receipt: command.DispatchReceipt{Accepted: true, Mode: command.ExecutionModeInline, CommandID: registration.ID(), CorrelationID: request.Options.CorrelationID}}, nil
	}))
	if _, err := allowed.Execute(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("policy executor did not receive canonical typed dispatch")
	}
}

type catalogExecutorFunc func(context.Context, command.CommandDispatchRequest, command.MessageRegistration, any, IngressMetadata) (command.DispatchOutcome, error)

func (f catalogExecutorFunc) ExecuteCatalog(ctx context.Context, request command.CommandDispatchRequest, registration command.MessageRegistration, message any, metadata IngressMetadata) (command.DispatchOutcome, error) {
	return f(ctx, request, registration, message, metadata)
}
