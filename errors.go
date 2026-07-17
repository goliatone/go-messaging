package messaging

import (
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
	ErrUnsupportedDisposition = errors.New("messaging: unsupported disposition")
	ErrDeadLetter             = errors.New("messaging: dead-letter failure")
	ErrHandlerPanic           = errors.New("messaging: handler panic")
)

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
