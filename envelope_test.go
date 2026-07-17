package messaging

import (
	"errors"
	"testing"
	"time"
)

func validEnvelope() Envelope {
	e := NewEnvelope("env-1", "orders.created", KindEvent, "1", "application/json", []byte(`{"id":"1"}`), map[string]string{"trace": "abc"})
	return e
}

func TestEnvelopeCloneIsolatesMutableState(t *testing.T) {
	original := validEnvelope()
	clone := original.Clone()
	clone.Payload[0] = 'X'
	clone.Headers["trace"] = "changed"
	if original.Payload[0] == 'X' || original.Headers["trace"] != "abc" {
		t.Fatal("clone mutated original")
	}
}

func TestEnvelopeValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Envelope)
		want error
	}{
		{"missing id", func(e *Envelope) { e.ID = "" }, ErrInvalidEnvelope},
		{"bad kind", func(e *Envelope) { e.Kind = "other" }, ErrInvalidEnvelope},
		{"reply correlation", func(e *Envelope) { e.Kind = KindReply }, ErrInvalidEnvelope},
		{"deadline", func(e *Envelope) { e.Deadline = e.Timestamp.Add(-time.Second) }, ErrInvalidEnvelope},
		{"reserved header", func(e *Envelope) { e.Headers["Messaging.Attempt"] = "2" }, ErrInvalidEnvelope},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEnvelope()
			tt.edit(&e)
			if err := e.Validate(); !errors.Is(err, tt.want) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestEnvelopeValidationLimits(t *testing.T) {
	e := validEnvelope()
	e.Payload = []byte("1234")
	limits := DefaultValidationLimits()
	limits.MaxPayloadBytes = 3
	if err := e.ValidateWith(limits); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v", err)
	}
	limits.MaxPayloadBytes = 10
	limits.SupportedSchemas = map[string]struct{}{"2": {}}
	if err := e.ValidateWith(limits); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("got %v", err)
	}
}
