package messaging

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrUnknownRoute           = errors.New("messaging: unknown route")
	ErrUnknownDriver          = errors.New("messaging: unknown driver")
	ErrUnsupportedCapability  = errors.New("messaging: unsupported capability")
	ErrPublishRejected        = errors.New("messaging: publish rejected")
	ErrPublishAmbiguous       = errors.New("messaging: publish outcome ambiguous")
	ErrNotPublished           = errors.New("messaging: definitely not published")
	ErrSubscriptionNotReady   = errors.New("messaging: subscription not ready")
	ErrSubscriptionClosed     = errors.New("messaging: subscription closed")
	ErrReplyTimeout           = errors.New("messaging: reply timeout")
	ErrCorrelation            = errors.New("messaging: reply correlation failure")
	ErrAcknowledgement        = errors.New("messaging: acknowledgement failure")
	ErrUnsupportedDisposition = errors.New("messaging: unsupported disposition")
	ErrDeadLetter             = errors.New("messaging: dead-letter failure")
	ErrHandlerPanic           = errors.New("messaging: handler panic")
	ErrObservedOperation      = errors.New("messaging: observed operation failed")
)

func safeObservationError(err error) error {
	if err == nil {
		return nil
	}
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		class := safeObservationClass(transportErr.Class)
		if class == nil {
			class = ErrObservedOperation
		}
		return &TransportError{
			Class: class, Transport: transportErr.Transport,
			Operation: transportErr.Operation, Temporary: transportErr.Temporary,
		}
	}
	if class := safeObservationClass(err); class != nil {
		return class
	}
	return ErrObservedOperation
}

func safeObservationClass(err error) error {
	for _, candidate := range []error{
		ErrInvalidEnvelope, ErrSchemaMismatch, ErrMessageTooLarge,
		ErrUnknownRoute, ErrUnknownDriver, ErrUnsupportedCapability,
		ErrPublishRejected, ErrPublishAmbiguous, ErrNotPublished,
		ErrSubscriptionNotReady, ErrSubscriptionClosed, ErrReplyTimeout,
		ErrCorrelation, ErrAcknowledgement, ErrUnsupportedDisposition, ErrDeadLetter,
		ErrHandlerPanic, context.Canceled, context.DeadlineExceeded,
	} {
		if errors.Is(err, candidate) {
			return candidate
		}
	}
	return nil
}

type TransportError struct {
	Class     error
	Transport string
	Operation string
	Temporary bool
	Cause     error
}

func (e *TransportError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Class == nil {
		return fmt.Sprintf("messaging: %s %s failed", e.Transport, e.Operation)
	}
	return fmt.Sprintf("%v: transport=%s operation=%s", e.Class, e.Transport, e.Operation)
}

func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.Cause != nil {
		return e.Cause
	}
	return e.Class
}

func (e *TransportError) Is(target error) bool {
	return e != nil && (errors.Is(e.Class, target) || errors.Is(e.Cause, target))
}
