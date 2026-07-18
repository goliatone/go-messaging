package commandadapter

import (
	"errors"
	"time"

	gerrors "github.com/goliatone/go-errors"
	messaging "github.com/goliatone/go-messaging"
)

var ErrEnvelopeDeadlineExpired = errors.New("go-command adapter: transported envelope deadline expired")

const (
	TextCodeIdempotencyInProgress   = "IDEMPOTENCY_IN_PROGRESS"
	TextCodeIdempotencyConflict     = "IDEMPOTENCY_CONFLICT"
	TextCodeEnvelopeDeadlineExpired = "ENVELOPE_DEADLINE_EXPIRED"
)

type ErrorMapper interface {
	Map(error, int) messaging.HandleResult
}

type DefaultErrorMapper struct {
	InvalidDisposition messaging.Disposition
	ExpiredDisposition messaging.Disposition
}

type adapterErrorBoundary uint8

const (
	adapterErrorBoundaryNone adapterErrorBoundary = iota
	adapterErrorBoundaryMessaging
	adapterErrorBoundaryClaimInProgress
	adapterErrorBoundaryClaimConflict
	adapterErrorBoundaryEnvelopeDeadline
)

type adapterErrorProjection struct {
	structured *gerrors.Error
	retryable  *gerrors.RetryableError
}

func (p adapterErrorProjection) asError() error {
	if p.retryable != nil {
		return p.retryable
	}
	return p.structured
}

func (m DefaultErrorMapper) Map(err error, attempt int) messaging.HandleResult {
	if err == nil {
		return messaging.Complete()
	}
	projected := projectAdapterError(err)
	if retryable := projected.retryable; retryable != nil && retryable.IsRetryable() {
		delay := time.Duration(0)
		if attempt > 0 {
			delay = retryable.RetryDelay(attempt)
		}
		return messaging.Retry(err, delay)
	}
	if structured := projected.structured; structured != nil {
		switch structured.TextCode {
		case TextCodeEnvelopeDeadlineExpired:
			return terminalDisposition(err, m.ExpiredDisposition)
		case messaging.TextCodeInvalidEnvelope, messaging.TextCodeSchemaMismatch, messaging.TextCodeMessageTooLarge:
			return terminalDisposition(err, m.InvalidDisposition)
		case messaging.TextCodePublishAmbiguous, messaging.TextCodePublishRejected:
			return messaging.Reject(err)
		}
		return mapStructuredError(err, structured)
	}
	return messaging.Retry(err, 0)
}

func projectAdapterError(err error) adapterErrorProjection {
	if err == nil {
		return adapterErrorProjection{}
	}

	switch firstAdapterErrorBoundary(err) {
	case adapterErrorBoundaryClaimInProgress:
		projected := gerrors.NewRetryable(ErrClaimInProgress.Error(), gerrors.CategoryConflict).
			WithTextCode(TextCodeIdempotencyInProgress).
			WithRetryDelay(0).
			WithLocation(nil)
		projected.Source = err
		return adapterErrorProjection{retryable: projected}
	case adapterErrorBoundaryClaimConflict:
		projected := gerrors.NewWithLocation(ErrClaimConflict.Error(), gerrors.CategoryConflict, nil).
			WithTextCode(TextCodeIdempotencyConflict)
		projected.Source = err
		return adapterErrorProjection{structured: projected}
	case adapterErrorBoundaryEnvelopeDeadline:
		projected := gerrors.NewWithLocation(ErrEnvelopeDeadlineExpired.Error(), gerrors.CategoryOperation, nil).
			WithTextCode(TextCodeEnvelopeDeadlineExpired)
		projected.Source = err
		return adapterErrorProjection{structured: projected}
	}

	if retryable := messaging.AsRetryableError(err); retryable != nil {
		return adapterErrorProjection{retryable: retryable}
	}
	return adapterErrorProjection{structured: messaging.AsGoError(err)}
}

// firstAdapterErrorBoundary resolves adapter-local sentinels without looking
// through a messaging or go-errors boundary. This keeps local mappings from
// overriding a more authoritative outer classification while still handling
// ordinary fmt wrappers and joined deadline errors.
func firstAdapterErrorBoundary(err error) adapterErrorBoundary {
	if err == nil {
		return adapterErrorBoundaryNone
	}
	if boundary := directAdapterErrorBoundary(err); boundary != adapterErrorBoundaryNone {
		return boundary
	}

	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if boundary := firstAdapterErrorBoundary(child); boundary != adapterErrorBoundaryNone {
				return boundary
			}
		}
		return adapterErrorBoundaryNone
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return firstAdapterErrorBoundary(wrapped.Unwrap())
	}
	return localAdapterErrorBoundary(err)
}

func directAdapterErrorBoundary(err error) adapterErrorBoundary {
	switch current := err.(type) { //nolint:errorlint // Direct inspection prevents causes from overriding outer policy.
	case *messaging.MessageError:
		if current != nil {
			return adapterErrorBoundaryMessaging
		}
	case *messaging.TransportError:
		if current != nil {
			return adapterErrorBoundaryMessaging
		}
	case *gerrors.RetryableError:
		if current != nil {
			return adapterErrorBoundaryMessaging
		}
	case *gerrors.Error:
		if current != nil {
			return adapterErrorBoundaryMessaging
		}
	}
	return adapterErrorBoundaryNone
}

func localAdapterErrorBoundary(err error) adapterErrorBoundary {
	switch {
	case errors.Is(err, ErrClaimInProgress):
		return adapterErrorBoundaryClaimInProgress
	case errors.Is(err, ErrClaimConflict):
		return adapterErrorBoundaryClaimConflict
	case errors.Is(err, ErrEnvelopeDeadlineExpired):
		return adapterErrorBoundaryEnvelopeDeadline
	default:
		return adapterErrorBoundaryNone
	}
}

func terminalDisposition(err error, disposition messaging.Disposition) messaging.HandleResult {
	if disposition == messaging.DispositionDeadLetter {
		return messaging.DeadLetter(err)
	}
	return messaging.Reject(err)
}

func mapStructuredError(err error, structured *gerrors.Error) messaging.HandleResult {
	switch structured.Category {
	case gerrors.CategoryValidation, gerrors.CategoryBadInput, gerrors.CategoryAuth, gerrors.CategoryAuthz, gerrors.CategoryNotFound, gerrors.CategoryConflict:
		return messaging.Reject(err)
	default:
		return messaging.Retry(err, 0)
	}
}
