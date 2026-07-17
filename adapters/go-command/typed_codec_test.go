package commandadapter

import (
	"context"
	"reflect"
	"testing"

	command "github.com/goliatone/go-command"
)

type codecMessage struct {
	Value string `json:"value"`
}

func (codecMessage) Type() string { return "codec.message" }

type codecRegistration struct{}

func (codecRegistration) ID() string                { return "codec.message" }
func (codecRegistration) MessageType() string       { return "codec.message" }
func (codecRegistration) Kind() command.HandlerKind { return command.HandlerKindCommand }
func (codecRegistration) NewMessage() any           { return &codecMessage{} }
func (codecRegistration) RequestType() reflect.Type { return reflect.TypeFor[codecMessage]() }
func (codecRegistration) ResultType() reflect.Type  { return nil }

func TestJSONTypedCodecRoundTripUsesRegistrationFactory(t *testing.T) {
	codec := JSONTypedCodec{MaxPayloadBytes: 1024}
	payload, err := codec.Encode(context.Background(), codecRegistration{}, codecMessage{Value: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(context.Background(), codecRegistration{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := decoded.(*codecMessage)
	if !ok || message.Value != "ok" {
		t.Fatalf("decoded %#v", decoded)
	}
}

func TestJSONTypedCodecRejectsTypeAndSizeMismatch(t *testing.T) {
	codec := JSONTypedCodec{MaxPayloadBytes: 4}
	if _, err := codec.Encode(context.Background(), codecRegistration{}, 42); err == nil {
		t.Fatal("expected type mismatch")
	}
	if _, err := codec.Decode(context.Background(), codecRegistration{}, []byte(`{"value":"too large"}`)); err == nil {
		t.Fatal("expected size failure")
	}
}
