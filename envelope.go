package messaging

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

// Kind identifies the delivery intent of an envelope.
type Kind string

const (
	KindEvent   Kind = "event"
	KindCommand Kind = "command"
	KindQuery   Kind = "query"
	KindReply   Kind = "reply"
)

const ReservedHeaderPrefix = "messaging."

var (
	ErrInvalidEnvelope = errors.New("messaging: invalid envelope")
	ErrSchemaMismatch  = errors.New("messaging: schema mismatch")
	ErrMessageTooLarge = errors.New("messaging: message too large")
)

// Envelope is the transport-neutral message exchanged across driver boundaries.
// Use NewEnvelope or Clone before handing values to independently-owned code.
type Envelope struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	Kind          Kind              `json:"kind"`
	SchemaVersion string            `json:"schema_version"`
	Payload       []byte            `json:"-"`
	ContentType   string            `json:"content_type"`
	Timestamp     time.Time         `json:"timestamp"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	ReplyTo       string            `json:"reply_to,omitempty"`
	Deadline      time.Time         `json:"deadline,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
}

// NewEnvelope constructs an envelope and defensively copies mutable fields.
func NewEnvelope(id, messageType string, kind Kind, schemaVersion, contentType string, payload []byte, headers map[string]string) Envelope {
	return Envelope{
		ID:            id,
		Type:          messageType,
		Kind:          kind,
		SchemaVersion: schemaVersion,
		Payload:       append([]byte(nil), payload...),
		ContentType:   contentType,
		Timestamp:     time.Now().UTC(),
		Headers:       cloneHeaders(headers),
	}
}

// Clone returns a deep copy suitable for crossing an ownership boundary.
func (e Envelope) Clone() Envelope {
	e.Payload = append([]byte(nil), e.Payload...)
	e.Headers = cloneHeaders(e.Headers)
	return e
}

func cloneHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return nil
	}
	out := make(map[string]string, len(headers))
	maps.Copy(out, headers)
	return out
}

// ValidationLimits bounds data that is decoded or forwarded by the framework.
type ValidationLimits struct {
	MaxPayloadBytes     int
	MaxHeaderCount      int
	MaxHeaderKeyBytes   int
	MaxHeaderValueBytes int
	MaxMetadataBytes    int
	SupportedSchemas    map[string]struct{}
}

func DefaultValidationLimits() ValidationLimits {
	return ValidationLimits{
		MaxPayloadBytes:     4 << 20,
		MaxHeaderCount:      64,
		MaxHeaderKeyBytes:   128,
		MaxHeaderValueBytes: 4096,
		MaxMetadataBytes:    32 << 10,
	}
}

// Validate checks the envelope using conservative default limits.
func (e Envelope) Validate() error { return e.ValidateWith(DefaultValidationLimits()) }

func (e Envelope) ValidateWith(limits ValidationLimits) error {
	if err := e.validateIdentity(); err != nil {
		return err
	}
	if err := e.validateSchemaAndTiming(limits); err != nil {
		return err
	}
	return e.validateSizeLimits(limits)
}

func (e Envelope) validateIdentity() error {
	if strings.TrimSpace(e.ID) == "" || strings.TrimSpace(e.Type) == "" {
		return fmt.Errorf("%w: id and type are required", ErrInvalidEnvelope)
	}
	switch e.Kind {
	case KindEvent, KindCommand, KindQuery, KindReply:
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidEnvelope, e.Kind)
	}
	if strings.TrimSpace(e.SchemaVersion) == "" || strings.TrimSpace(e.ContentType) == "" {
		return fmt.Errorf("%w: schema_version and content_type are required", ErrInvalidEnvelope)
	}
	if e.Kind == KindReply && strings.TrimSpace(e.CorrelationID) == "" {
		return fmt.Errorf("%w: reply correlation_id is required", ErrInvalidEnvelope)
	}
	return nil
}

func (e Envelope) validateSchemaAndTiming(limits ValidationLimits) error {
	if len(limits.SupportedSchemas) > 0 {
		if _, ok := limits.SupportedSchemas[e.SchemaVersion]; !ok {
			return fmt.Errorf("%w: unsupported schema %q", ErrSchemaMismatch, e.SchemaVersion)
		}
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalidEnvelope)
	}
	if !e.Deadline.IsZero() && !e.Deadline.After(e.Timestamp) {
		return fmt.Errorf("%w: deadline must be after timestamp", ErrInvalidEnvelope)
	}
	return nil
}

func (e Envelope) validateSizeLimits(limits ValidationLimits) error {
	if limits.MaxPayloadBytes > 0 && len(e.Payload) > limits.MaxPayloadBytes {
		return fmt.Errorf("%w: payload exceeds %d bytes", ErrMessageTooLarge, limits.MaxPayloadBytes)
	}
	if limits.MaxHeaderCount > 0 && len(e.Headers) > limits.MaxHeaderCount {
		return fmt.Errorf("%w: too many headers", ErrInvalidEnvelope)
	}
	metadataBytes, err := e.validateHeaders(limits)
	if err != nil {
		return err
	}
	metadataBytes += len(e.ID) + len(e.Type) + len(e.SchemaVersion) + len(e.ContentType) + len(e.CorrelationID) + len(e.CausationID) + len(e.ReplyTo)
	if limits.MaxMetadataBytes > 0 && metadataBytes > limits.MaxMetadataBytes {
		return fmt.Errorf("%w: metadata exceeds %d bytes", ErrInvalidEnvelope, limits.MaxMetadataBytes)
	}
	return nil
}

func (e Envelope) validateHeaders(limits ValidationLimits) (int, error) {
	metadataBytes := 0
	for key, value := range e.Headers {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return 0, fmt.Errorf("%w: empty header name", ErrInvalidEnvelope)
		}
		if strings.HasPrefix(strings.ToLower(trimmed), ReservedHeaderPrefix) {
			return 0, fmt.Errorf("%w: reserved header %q", ErrInvalidEnvelope, key)
		}
		if limits.MaxHeaderKeyBytes > 0 && len(key) > limits.MaxHeaderKeyBytes {
			return 0, fmt.Errorf("%w: header key too large", ErrInvalidEnvelope)
		}
		if limits.MaxHeaderValueBytes > 0 && len(value) > limits.MaxHeaderValueBytes {
			return 0, fmt.Errorf("%w: header value too large", ErrInvalidEnvelope)
		}
		metadataBytes += len(key) + len(value)
	}
	return metadataBytes, nil
}
