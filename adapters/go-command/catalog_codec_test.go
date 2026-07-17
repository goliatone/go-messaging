package commandadapter

import (
	"context"
	"encoding/json"
	"errors"
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
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
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
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := NewJSONCatalogCodec(provider, JSONTypedCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if validationErr := codec.ValidateCoverage(registration); validationErr == nil {
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

func TestCatalogCodecRejectsSchemaAndRegistrationDrift(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := NewJSONCatalogCodec(provider, JSONTypedCodec{}, CatalogBinding{
		CatalogID: "create", RegistrationID: "test.create", Kind: command.HandlerKindCommand, SchemaVersion: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	dto := CatalogDispatchDTO{
		Version: CatalogWireVersion, SchemaVersion: "2", CommandID: "create",
		HandlerKind: string(command.HandlerKindCommand), Payload: map[string]any{"name": "Ada"},
	}
	if _, _, _, decodeErr := codec.DecodeCatalog(context.Background(), dto, provider); decodeErr == nil {
		t.Fatal("expected schema mismatch")
	}
	dto.SchemaVersion = "1"
	drifted := ingressRegistration{
		id: "test.create", messageType: "test.create.v2", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[lookupMessage](), newMessage: func() any { return &lookupMessage{} },
	}
	driftedProvider, err := command.NewMessageRegistrationIndex(drifted)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, decodeErr := codec.DecodeCatalog(context.Background(), dto, driftedProvider); decodeErr == nil {
		t.Fatal("expected incompatible registration rejection")
	}
}

func TestCatalogIngressRequiresExplicitEventAuthorizationAndUsesPolicyExecutor(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	newCodec := func(allow bool) *JSONCatalogCodec {
		codec, codecErr := NewJSONCatalogCodec(provider, JSONTypedCodec{}, CatalogBinding{
			CatalogID: "create", RegistrationID: "test.create", Kind: command.HandlerKindCommand,
			SchemaVersion: "1", AllowEventToCommand: allow,
		})
		if codecErr != nil {
			t.Fatal(codecErr)
		}
		return codec
	}
	dto := CatalogDispatchDTO{
		Version: CatalogWireVersion, SchemaVersion: "1", CommandID: "create", HandlerKind: string(command.HandlerKindCommand),
		Payload: map[string]any{"name": "Ada"}, Options: CatalogOptionsDTO{Mode: "inline", CorrelationID: "correlation-1"},
	}
	payload, err := json.Marshal(dto)
	if err != nil {
		t.Fatal(err)
	}
	delivery := deliveryFor(messaging.KindEvent, CatalogEnvelopeType, payload)

	denied, err := NewCatalogIngress(provider, newCodec(false), catalogExecutorFunc(func(context.Context, command.CommandDispatchRequest, command.MessageRegistration, any, IngressMetadata) (command.DispatchOutcome, error) {
		t.Fatal("denied event reached executor")
		return command.DispatchOutcome{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := denied.Execute(context.Background(), delivery); executeErr == nil {
		t.Fatal("expected event authorization failure")
	}

	called := false
	allowed, err := NewCatalogIngress(provider, newCodec(true), catalogExecutorFunc(func(_ context.Context, request command.CommandDispatchRequest, registration command.MessageRegistration, message any, metadata IngressMetadata) (command.DispatchOutcome, error) {
		decoded, ok := message.(*createMessage)
		called = ok && request.CommandID == "test.create" && registration.ID() == "test.create" && decoded.Name == "Ada" && metadata.EnvelopeKind == messaging.KindEvent
		return command.DispatchOutcome{Receipt: command.DispatchReceipt{Accepted: true, Mode: command.ExecutionModeInline, CommandID: registration.ID(), CorrelationID: request.Options.CorrelationID}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := allowed.Execute(context.Background(), delivery); executeErr != nil {
		t.Fatal(executeErr)
	}
	if !called {
		t.Fatal("policy executor did not receive canonical typed dispatch")
	}
}

func TestCatalogIngressAppliesTransportedDeadline(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := NewJSONCatalogCodec(provider, JSONTypedCodec{}, CatalogBinding{
		CatalogID: "create", RegistrationID: "test.create", Kind: command.HandlerKindCommand, SchemaVersion: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	dto := CatalogDispatchDTO{
		Version: CatalogWireVersion, SchemaVersion: "1", CommandID: "create", HandlerKind: string(command.HandlerKindCommand),
		Payload: map[string]any{"name": "Ada"}, Options: CatalogOptionsDTO{Mode: "inline", CorrelationID: "correlation-1"},
	}
	payload, err := json.Marshal(dto)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	ingress, err := NewCatalogIngress(provider, codec, catalogExecutorFunc(func(context.Context, command.CommandDispatchRequest, command.MessageRegistration, any, IngressMetadata) (command.DispatchOutcome, error) {
		called = true
		return command.DispatchOutcome{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	envelope := messaging.NewEnvelope("expired", CatalogEnvelopeType, messaging.KindCommand, "1", "application/json", payload, nil)
	envelope.CorrelationID = "correlation-1"
	envelope.Timestamp = time.Now().Add(-2 * time.Second)
	envelope.Deadline = time.Now().Add(-time.Second)
	_, err = ingress.Execute(context.Background(), messaging.NewDelivery(envelope, messaging.DeliveryInfo{Attempt: 1}))
	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}

type catalogExecutorFunc func(context.Context, command.CommandDispatchRequest, command.MessageRegistration, any, IngressMetadata) (command.DispatchOutcome, error)

func (f catalogExecutorFunc) ExecuteCatalog(ctx context.Context, request command.CommandDispatchRequest, registration command.MessageRegistration, message any, metadata IngressMetadata) (command.DispatchOutcome, error) {
	return f(ctx, request, registration, message, metadata)
}
