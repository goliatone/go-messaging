package commandadapter

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	command "github.com/goliatone/go-command"
	gerrors "github.com/goliatone/go-errors"
	messaging "github.com/goliatone/go-messaging"
)

func TestReplyCodecRoundTripsTypedNilAndStructuredFailure(t *testing.T) {
	registration := ingressRegistration{
		id: "test.lookup", messageType: "test.lookup", kind: command.HandlerKindQuery,
		request: reflect.TypeFor[lookupMessage](), result: reflect.TypeFor[*createMessage](),
		newMessage: func() any { return &lookupMessage{} },
	}
	codec := JSONReplyCodec{}
	outcome := command.DispatchOutcome{
		Receipt:       command.DispatchReceipt{Accepted: true, Mode: command.ExecutionModeInline, CommandID: registration.ID(), CorrelationID: "correlation-1"},
		ResultPresent: true, Result: (*createMessage)(nil),
	}
	payload, err := codec.Encode(context.Background(), registration, outcome, nil)
	if err != nil {
		t.Fatal(err)
	}
	reply := messaging.NewEnvelope("reply-1", registration.MessageType(), messaging.KindReply, "1", codec.ContentType(), payload, nil)
	reply.CorrelationID = "correlation-1"
	decoded, err := codec.Decode(context.Background(), registration, reply)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.ResultPresent || decoded.Result != nil {
		t.Fatalf("decoded %#v", decoded)
	}

	failure := gerrors.New("denied", gerrors.CategoryAuthz).WithTextCode("LOOKUP_DENIED")
	payload, err = codec.Encode(context.Background(), registration, command.DispatchOutcome{}, failure)
	if err != nil {
		t.Fatal(err)
	}
	reply.Payload = payload
	_, err = codec.Decode(context.Background(), registration, reply)
	var structured *gerrors.Error
	if !gerrors.As(err, &structured) || structured.Category != gerrors.CategoryAuthz || structured.TextCode != "LOOKUP_DENIED" {
		t.Fatalf("structured error %#v", err)
	}
}

func TestReplyCodecRejectsMismatchedResultTypeAndCorrelation(t *testing.T) {
	registration := ingressRegistration{
		id: "test.lookup", messageType: "test.lookup", kind: command.HandlerKindQuery,
		request: reflect.TypeFor[lookupMessage](), result: reflect.TypeFor[string](),
		newMessage: func() any { return &lookupMessage{} },
	}
	dto := ReplyDTO{
		Version:       ReplyWireVersion,
		Receipt:       command.DispatchReceipt{Accepted: true, Mode: command.ExecutionModeInline, CommandID: registration.ID(), CorrelationID: "other"},
		ResultPresent: true, Result: []byte(`"ok"`), ResultType: "int",
	}
	payload, err := json.Marshal(dto)
	if err != nil {
		t.Fatal(err)
	}
	reply := messaging.NewEnvelope("reply-1", registration.MessageType(), messaging.KindReply, "1", (JSONReplyCodec{}).ContentType(), payload, nil)
	reply.CorrelationID = "correlation-1"
	if _, err := (JSONReplyCodec{}).Decode(context.Background(), registration, reply); err == nil {
		t.Fatal("expected reply mismatch")
	}
}

func TestReplyCodecPreservesRetryClassificationWithoutExposingMessage(t *testing.T) {
	registration := ingressRegistration{
		id: "test.lookup", messageType: "test.lookup", kind: command.HandlerKindQuery,
		request: reflect.TypeFor[lookupMessage](), result: reflect.TypeFor[string](),
		newMessage: func() any { return &lookupMessage{} },
	}
	codec := JSONReplyCodec{}
	failure := gerrors.NewRetryableExternal("payload=secret").WithTextCode("WORKER_UNAVAILABLE")
	payload, err := codec.Encode(context.Background(), registration, command.DispatchOutcome{}, failure)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "secret") {
		t.Fatalf("unsafe failure payload: %s", payload)
	}
	reply := messaging.NewEnvelope("reply-1", registration.MessageType(), messaging.KindReply, "1", codec.ContentType(), payload, nil)
	_, err = codec.Decode(context.Background(), registration, reply)
	var retryable *gerrors.RetryableError
	if !gerrors.As(err, &retryable) || !retryable.IsRetryable() || retryable.BaseError.TextCode != "WORKER_UNAVAILABLE" {
		t.Fatalf("retry classification lost: %v", err)
	}
}
