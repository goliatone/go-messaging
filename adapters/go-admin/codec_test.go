package goadmin

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	admin "github.com/goliatone/go-admin/pkg/admin"
	messaging "github.com/goliatone/go-messaging"
)

func TestCodecRoundTrip(t *testing.T) {
	codec := requireCodec(t)
	update := validUpdate("round-trip", 3)
	update.DispatchID = "dispatch-1"
	update.CorrelationID = "correlation-1"
	update.Scope.TenantID = "tenant-a"
	update.Scope.OrganizationID = "organization-a"
	update.Metadata = map[string]any{"safe": map[string]any{"value": "original"}}

	envelope, err := codec.Encode(update)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if envelope.Type != MessageType || envelope.Kind != messaging.KindEvent ||
		envelope.SchemaVersion != EnvelopeSchemaVersion || envelope.ContentType != ContentTypeJSON {
		t.Fatalf("unexpected envelope metadata: %+v", envelope)
	}
	decoded, err := codec.Decode(envelope)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.EventID != update.EventID || decoded.DispatchID != update.DispatchID ||
		decoded.CorrelationID != update.CorrelationID || decoded.Scope != update.Scope {
		t.Fatalf("decoded update mismatch: %+v", decoded)
	}
	decoded.Metadata["safe"].(map[string]any)["value"] = "changed"
	if update.Metadata["safe"].(map[string]any)["value"] != "original" {
		t.Fatal("codec retained caller-owned metadata")
	}
}

func TestCodecRejectsStrictBoundaryViolations(t *testing.T) {
	codec := requireCodec(t)
	base, err := codec.Encode(validUpdate("strict", 1))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	tests := map[string]func(messaging.Envelope) messaging.Envelope{
		"kind": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.Kind = messaging.KindCommand
			return envelope
		},
		"type": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.Type = "other"
			return envelope
		},
		"schema": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.SchemaVersion = "2"
			return envelope
		},
		"content type": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.ContentType = "text/plain"
			return envelope
		},
		"size": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.Payload = []byte(strings.Repeat("x", defaultMaxPayloadBytes+1))
			return envelope
		},
		"application header": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.Headers[HeaderApplicationID] = "other-app"
			return envelope
		},
		"environment header": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.Headers[HeaderEnvironmentID] = "production"
			return envelope
		},
		"tenant header": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.Headers[HeaderTenantID] = "tenant-a"
			return envelope
		},
		"lineage": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.ID = "different-event"
			return envelope
		},
		"trailing JSON": func(envelope messaging.Envelope) messaging.Envelope {
			envelope.Payload = append(envelope.Payload, []byte(` {}`)...)
			return envelope
		},
		"unknown JSON field": func(envelope messaging.Envelope) messaging.Envelope {
			var payload map[string]any
			if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
				t.Fatalf("unmarshal fixture: %v", err)
			}
			payload["unexpected"] = true
			envelope.Payload, _ = json.Marshal(payload)
			return envelope
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			envelope := mutate(base.Clone())
			if _, err := codec.Decode(envelope); err == nil {
				t.Fatal("decode should reject envelope")
			}
		})
	}
}

func TestCodecRejectsPayloadIdentityMismatch(t *testing.T) {
	codec := requireCodec(t)
	update := validUpdate("wrong-identity", 1)
	update.Scope.ApplicationID = "other-app"
	if _, err := codec.Encode(update); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("encode error = %v, want identity mismatch", err)
	}

	envelope, err := codec.Encode(validUpdate("decode-identity", 1))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var payload admin.CommandRunUpdate
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	payload.Scope.EnvironmentID = "production"
	envelope.Payload, _ = json.Marshal(payload)
	envelope.Headers[HeaderEnvironmentID] = "production"
	if _, err := codec.Decode(envelope); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("decode error = %v, want identity mismatch", err)
	}
}

func requireCodec(t testing.TB) *Codec {
	t.Helper()
	codec, err := NewCodec(CodecConfig{ApplicationID: "app", EnvironmentID: "test"})
	if err != nil {
		t.Fatalf("new codec: %v", err)
	}
	return codec
}

func validUpdate(runID string, revision uint64) admin.CommandRunUpdate {
	return admin.CommandRunUpdate{
		SchemaVersion: admin.CommandRunSchemaVersion,
		EventID:       "event-" + runID,
		RunID:         runID,
		Revision:      revision,
		CommandID:     "test.command",
		Phase:         admin.CommandRunPhaseSubmitted,
		OccurredAt:    time.Now().UTC().Truncate(time.Microsecond),
		Scope: admin.CommandRunScope{
			ApplicationID: "app",
			EnvironmentID: "test",
		},
	}
}
