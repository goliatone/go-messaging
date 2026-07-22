package goadmin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	admin "github.com/goliatone/go-admin/pkg/admin"
	messaging "github.com/goliatone/go-messaging"
)

const (
	// MessageType is the stable logical type for command-run lifecycle events.
	MessageType = "go-admin.debug.command-run.updated"
	// EnvelopeSchemaVersion is the go-messaging envelope schema version.
	EnvelopeSchemaVersion = "1"
	// ContentTypeJSON is the only accepted command-run envelope content type.
	ContentTypeJSON = "application/json"

	HeaderApplicationID  = "go-admin.application-id"
	HeaderEnvironmentID  = "go-admin.environment-id"
	HeaderTenantID       = "go-admin.tenant-id"
	HeaderOrganizationID = "go-admin.organization-id"

	defaultMaxPayloadBytes = 64 << 10
)

// CodecConfig defines the trusted process identity and bounded contract limits.
type CodecConfig struct {
	ApplicationID   string
	EnvironmentID   string
	MaxPayloadBytes int
	ContractLimits  admin.CommandRunContractLimits
}

// Codec maps the provider-neutral command-run contract to go-messaging envelopes.
type Codec struct {
	applicationID   string
	environmentID   string
	maxPayloadBytes int
	contractLimits  admin.CommandRunContractLimits
	validation      messaging.ValidationLimits
}

// NewCodec constructs a strict command-run envelope boundary.
func NewCodec(config CodecConfig) (*Codec, error) {
	config.ApplicationID = strings.TrimSpace(config.ApplicationID)
	config.EnvironmentID = strings.TrimSpace(config.EnvironmentID)
	if config.ApplicationID == "" || config.EnvironmentID == "" {
		return nil, fmt.Errorf("%w: application and environment are required", ErrInvalidConfig)
	}
	if config.MaxPayloadBytes < 0 {
		return nil, fmt.Errorf("%w: max payload bytes must not be negative", ErrInvalidConfig)
	}
	if config.MaxPayloadBytes == 0 {
		config.MaxPayloadBytes = defaultMaxPayloadBytes
	}
	validation := messaging.DefaultValidationLimits()
	validation.MaxPayloadBytes = config.MaxPayloadBytes
	validation.SupportedSchemas = map[string]struct{}{EnvelopeSchemaVersion: {}}
	return &Codec{
		applicationID:   config.ApplicationID,
		environmentID:   config.EnvironmentID,
		maxPayloadBytes: config.MaxPayloadBytes,
		contractLimits:  config.ContractLimits,
		validation:      validation,
	}, nil
}

// Encode validates and serializes one update without retaining caller-owned data.
func (c *Codec) Encode(update admin.CommandRunUpdate) (messaging.Envelope, error) {
	if c == nil {
		return messaging.Envelope{}, ErrInvalidConfig
	}
	normalized, err := admin.NormalizeCommandRunUpdate(update, c.contractLimits)
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("%w: command-run contract", ErrEnvelopeRejected)
	}
	if err := c.validateIdentity(normalized.Scope); err != nil {
		return messaging.Envelope{}, err
	}
	payload, err := json.Marshal(normalized)
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("%w: command-run JSON", ErrEnvelopeRejected)
	}
	if len(payload) > c.maxPayloadBytes {
		return messaging.Envelope{}, fmt.Errorf("%w: payload too large", ErrEnvelopeRejected)
	}
	envelope := messaging.NewEnvelope(
		normalized.EventID,
		MessageType,
		messaging.KindEvent,
		EnvelopeSchemaVersion,
		ContentTypeJSON,
		payload,
		scopeHeaders(normalized.Scope),
	)
	envelope.Timestamp = normalized.OccurredAt
	envelope.CorrelationID = normalized.CorrelationID
	envelope.CausationID = normalized.DispatchID
	if err := envelope.ValidateWith(c.validation); err != nil {
		return messaging.Envelope{}, fmt.Errorf("%w: envelope contract", ErrEnvelopeRejected)
	}
	return envelope, nil
}

// Decode validates an untrusted envelope before returning an isolated update.
func (c *Codec) Decode(envelope messaging.Envelope) (admin.CommandRunUpdate, error) {
	if c == nil {
		return admin.CommandRunUpdate{}, ErrInvalidConfig
	}
	if err := envelope.ValidateWith(c.validation); err != nil {
		return admin.CommandRunUpdate{}, fmt.Errorf("%w: envelope contract", ErrEnvelopeRejected)
	}
	if envelope.Kind != messaging.KindEvent || envelope.Type != MessageType ||
		envelope.SchemaVersion != EnvelopeSchemaVersion || envelope.ContentType != ContentTypeJSON {
		return admin.CommandRunUpdate{}, fmt.Errorf("%w: unsupported envelope metadata", ErrEnvelopeRejected)
	}

	decoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	decoder.DisallowUnknownFields()
	var update admin.CommandRunUpdate
	if err := decoder.Decode(&update); err != nil {
		return admin.CommandRunUpdate{}, fmt.Errorf("%w: invalid command-run JSON", ErrEnvelopeRejected)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return admin.CommandRunUpdate{}, fmt.Errorf("%w: invalid command-run JSON", ErrEnvelopeRejected)
	}
	normalized, err := admin.NormalizeCommandRunUpdate(update, c.contractLimits)
	if err != nil {
		return admin.CommandRunUpdate{}, fmt.Errorf("%w: command-run contract", ErrEnvelopeRejected)
	}
	if err := c.validateIdentity(normalized.Scope); err != nil {
		return admin.CommandRunUpdate{}, err
	}
	if !scopeHeadersMatch(envelope.Headers, normalized.Scope) {
		return admin.CommandRunUpdate{}, fmt.Errorf("%w: scope headers", ErrIdentityMismatch)
	}
	if envelope.ID != normalized.EventID || envelope.CorrelationID != normalized.CorrelationID ||
		envelope.CausationID != normalized.DispatchID || !envelope.Timestamp.Equal(normalized.OccurredAt) {
		return admin.CommandRunUpdate{}, fmt.Errorf("%w: envelope lineage", ErrEnvelopeRejected)
	}
	return normalized.Clone(), nil
}

func (c *Codec) validateIdentity(scope admin.CommandRunScope) error {
	scope = scope.Normalize()
	if scope.ApplicationID != c.applicationID || scope.EnvironmentID != c.environmentID {
		return fmt.Errorf("%w: application or environment", ErrIdentityMismatch)
	}
	return nil
}

func scopeHeaders(scope admin.CommandRunScope) map[string]string {
	scope = scope.Normalize()
	return map[string]string{
		HeaderApplicationID:  scope.ApplicationID,
		HeaderEnvironmentID:  scope.EnvironmentID,
		HeaderTenantID:       scope.TenantID,
		HeaderOrganizationID: scope.OrganizationID,
	}
}

func scopeHeadersMatch(headers map[string]string, scope admin.CommandRunScope) bool {
	expected := scopeHeaders(scope)
	for key, value := range expected {
		if headers[key] != value {
			return false
		}
	}
	return true
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("unexpected trailing JSON value")
}
