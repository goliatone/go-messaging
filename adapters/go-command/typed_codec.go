package commandadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	command "github.com/goliatone/go-command"
)

// TypedCodec maps a registered go-command message to and from an envelope
// payload. It deliberately receives the exported registration rather than
// maintaining another message registry.
type TypedCodec interface {
	Encode(context.Context, command.MessageRegistration, any) ([]byte, error)
	Decode(context.Context, command.MessageRegistration, []byte) (any, error)
}

// JSONTypedCodec encodes the message value directly as JSON. MaxPayloadBytes
// is checked before decoding and after encoding when it is positive.
type JSONTypedCodec struct {
	MaxPayloadBytes int
}

func (c JSONTypedCodec) Encode(_ context.Context, registration command.MessageRegistration, message any) ([]byte, error) {
	if err := validateRegistration(registration); err != nil {
		return nil, err
	}
	if !messageCompatible(registration.RequestType(), reflect.TypeOf(message)) {
		return nil, command.NewDynamicMessageTypeMismatchError(registration, message)
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("go-command adapter: encode typed message: %w", err)
	}
	if c.MaxPayloadBytes > 0 && len(payload) > c.MaxPayloadBytes {
		return nil, fmt.Errorf("go-command adapter: typed payload exceeds %d bytes", c.MaxPayloadBytes)
	}
	return payload, nil
}

func (c JSONTypedCodec) Decode(_ context.Context, registration command.MessageRegistration, payload []byte) (any, error) {
	if err := validateRegistration(registration); err != nil {
		return nil, err
	}
	if c.MaxPayloadBytes > 0 && len(payload) > c.MaxPayloadBytes {
		return nil, fmt.Errorf("go-command adapter: typed payload exceeds %d bytes", c.MaxPayloadBytes)
	}
	message := registration.NewMessage()
	if message == nil {
		return nil, command.NewRegistrationInvalidError("registration message factory returned nil", map[string]any{"registration_id": registration.ID()})
	}

	value := reflect.ValueOf(message)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, command.NewRegistrationInvalidError("registration message factory returned a typed nil", map[string]any{"registration_id": registration.ID()})
		}
		if err := json.Unmarshal(payload, message); err != nil {
			return nil, fmt.Errorf("go-command adapter: decode typed message: %w", err)
		}
		return message, nil
	}

	target := reflect.New(value.Type())
	target.Elem().Set(value)
	if err := json.Unmarshal(payload, target.Interface()); err != nil {
		return nil, fmt.Errorf("go-command adapter: decode typed message: %w", err)
	}
	return target.Elem().Interface(), nil
}

func validateRegistration(registration command.MessageRegistration) error {
	if registration == nil || isTypedNil(registration) {
		return command.NewRegistrationNotFoundError("", "")
	}
	if registration.RequestType() == nil {
		return command.NewRegistrationInvalidError("registration request type is required", map[string]any{"registration_id": registration.ID()})
	}
	return nil
}

func messageCompatible(expected, actual reflect.Type) bool {
	if expected == nil || actual == nil {
		return false
	}
	if actual.AssignableTo(expected) {
		return true
	}
	if expected.Kind() == reflect.Interface && actual.Implements(expected) {
		return true
	}
	if actual.Kind() == reflect.Pointer && actual.Elem().AssignableTo(expected) {
		return true
	}
	return expected.Kind() == reflect.Pointer && actual.AssignableTo(expected.Elem())
}

func isTypedNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
