package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONCodecRoundTripUsesExplicitBase64(t *testing.T) {
	codec := NewJSONCodec()
	want := validEnvelope()
	want.Payload = []byte{0, 1, 2, 254, 255}
	data, err := codec.Encode(context.Background(), want)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if decodeErr := json.Unmarshal(data, &wire); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if wire["payload_base64"] != "AAEC/v8=" {
		t.Fatalf("unexpected wire payload %v", wire["payload_base64"])
	}
	if _, ok := wire["Payload"]; ok {
		t.Fatal("implicit payload field leaked")
	}
	got, err := codec.Decode(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, want.Payload) || got.ID != want.ID {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}

func TestJSONCodecRejectsMalformedAndOversizedFrames(t *testing.T) {
	codec := NewJSONCodec()
	codec.MaxFrameBytes = 4
	if _, err := codec.Decode(context.Background(), []byte("12345")); err == nil {
		t.Fatal("expected size error")
	}
	codec.MaxFrameBytes = 1024
	data := []byte(`{"id":"x","type":"t","kind":"event","schema_version":"1","content_type":"x","timestamp":"2026-01-01T00:00:00Z","payload_base64":"***"}`)
	if _, err := codec.Decode(context.Background(), data); err == nil || !strings.Contains(err.Error(), "payload_base64") {
		t.Fatalf("got %v", err)
	}
}
