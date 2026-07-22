package messaging

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// Codec encodes and decodes complete transport envelopes.
type Codec interface {
	Encode(context.Context, Envelope) ([]byte, error)
	Decode(context.Context, []byte) (Envelope, error)
	ContentType() string
}

const JSONEnvelopeContentType = "application/vnd.goliatone.messaging-envelope+json"

type JSONCodec struct {
	Limits        ValidationLimits
	MaxFrameBytes int
}

func NewJSONCodec() JSONCodec {
	return JSONCodec{Limits: DefaultValidationLimits(), MaxFrameBytes: 6 << 20}
}

func (JSONCodec) ContentType() string { return JSONEnvelopeContentType }

type jsonEnvelope struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	Kind          Kind              `json:"kind"`
	SchemaVersion string            `json:"schema_version"`
	PayloadBase64 string            `json:"payload_base64"`
	ContentType   string            `json:"content_type"`
	Timestamp     time.Time         `json:"timestamp"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	ReplyTo       string            `json:"reply_to,omitempty"`
	Deadline      time.Time         `json:"deadline"`
	Headers       map[string]string `json:"headers,omitempty"`
}

func (c JSONCodec) Encode(_ context.Context, envelope Envelope) ([]byte, error) {
	limits := c.Limits
	if validationLimitsUnset(limits) {
		limits = DefaultValidationLimits()
	}
	if err := envelope.ValidateWith(limits); err != nil {
		return nil, err
	}
	wire := jsonEnvelope{
		ID: envelope.ID, Type: envelope.Type, Kind: envelope.Kind,
		SchemaVersion: envelope.SchemaVersion,
		PayloadBase64: base64.StdEncoding.EncodeToString(envelope.Payload),
		ContentType:   envelope.ContentType, Timestamp: envelope.Timestamp,
		CorrelationID: envelope.CorrelationID, CausationID: envelope.CausationID,
		ReplyTo: envelope.ReplyTo, Deadline: envelope.Deadline,
		Headers: cloneHeaders(envelope.Headers),
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("messaging: encode envelope: %w", err)
	}
	if c.MaxFrameBytes > 0 && len(data) > c.MaxFrameBytes {
		return nil, fmt.Errorf("%w: encoded frame exceeds %d bytes", ErrMessageTooLarge, c.MaxFrameBytes)
	}
	return data, nil
}

func (c JSONCodec) Decode(_ context.Context, data []byte) (Envelope, error) {
	if c.MaxFrameBytes > 0 && len(data) > c.MaxFrameBytes {
		return Envelope{}, fmt.Errorf("%w: encoded frame exceeds %d bytes", ErrMessageTooLarge, c.MaxFrameBytes)
	}
	var wire jsonEnvelope
	if err := json.Unmarshal(data, &wire); err != nil {
		return Envelope{}, fmt.Errorf("%w: decode JSON: %w", ErrInvalidEnvelope, err)
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(wire.PayloadBase64)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: payload_base64: %w", ErrInvalidEnvelope, err)
	}
	envelope := Envelope{
		ID: wire.ID, Type: wire.Type, Kind: wire.Kind,
		SchemaVersion: wire.SchemaVersion, Payload: payload,
		ContentType: wire.ContentType, Timestamp: wire.Timestamp,
		CorrelationID: wire.CorrelationID, CausationID: wire.CausationID,
		ReplyTo: wire.ReplyTo, Deadline: wire.Deadline,
		Headers: cloneHeaders(wire.Headers),
	}
	limits := c.Limits
	if validationLimitsUnset(limits) {
		limits = DefaultValidationLimits()
	}
	if err := envelope.ValidateWith(limits); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func validationLimitsUnset(limits ValidationLimits) bool {
	return limits.MaxPayloadBytes == 0 && limits.MaxHeaderCount == 0 &&
		limits.MaxHeaderKeyBytes == 0 && limits.MaxHeaderValueBytes == 0 &&
		limits.MaxMetadataBytes == 0 && limits.SupportedSchemas == nil
}
