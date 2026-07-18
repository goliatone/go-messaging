package commandadapter

import (
	"context"
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

func (m DefaultErrorMapper) Map(err error, attempt int) messaging.HandleResult {
	if err == nil {
		return messaging.Complete()
	}
	if result, ok := m.mapKnownError(err); ok {
		return result
	}
	projected := projectAdapterError(err)
	var retryable *gerrors.RetryableError
	if gerrors.As(projected, &retryable) && retryable.IsRetryable() {
		delay := time.Duration(0)
		if attempt > 0 {
			delay = retryable.RetryDelay(attempt)
		}
		return messaging.Retry(err, delay)
	}
	var structured *gerrors.Error
	if gerrors.As(projected, &structured) {
		return mapStructuredError(err, structured)
	}
	return messaging.Retry(err, 0)
}

func (m DefaultErrorMapper) mapKnownError(err error) (messaging.HandleResult, bool) {
	switch {
	case errors.Is(err, ErrClaimInProgress):
		return messaging.Retry(err, 0), true
	case errors.Is(err, ErrClaimConflict):
		return messaging.Reject(err), true
	case errors.Is(err, ErrEnvelopeDeadlineExpired):
		return terminalDisposition(err, m.ExpiredDisposition), true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return messaging.Retry(err, 0), true
	case errors.Is(err, messaging.ErrInvalidEnvelope), errors.Is(err, messaging.ErrSchemaMismatch), errors.Is(err, messaging.ErrMessageTooLarge):
		return terminalDisposition(err, m.InvalidDisposition), true
	case errors.Is(err, messaging.ErrPublishAmbiguous), errors.Is(err, messaging.ErrPublishRejected):
		return messaging.Reject(err), true
	case errors.Is(err, messaging.ErrNotPublished):
		return messaging.Retry(err, 0), true
	default:
		return messaging.HandleResult{}, false
	}
}

func projectAdapterError(err error) error {
	if err == nil {
		return nil
	}
	var retryable *gerrors.RetryableError
	if errors.As(err, &retryable) {
		return retryable
	}
	var structured *gerrors.Error
	if errors.As(err, &structured) {
		return structured
	}

	switch {
	case errors.Is(err, ErrClaimInProgress):
		projected := gerrors.NewRetryable(ErrClaimInProgress.Error(), gerrors.CategoryConflict).
			WithTextCode(TextCodeIdempotencyInProgress).
			WithRetryDelay(0).
			WithLocation(nil)
		projected.Source = err
		return projected
	case errors.Is(err, ErrClaimConflict):
		projected := gerrors.NewWithLocation(ErrClaimConflict.Error(), gerrors.CategoryConflict, nil).
			WithTextCode(TextCodeIdempotencyConflict)
		projected.Source = err
		return projected
	case errors.Is(err, ErrEnvelopeDeadlineExpired):
		projected := gerrors.NewWithLocation(ErrEnvelopeDeadlineExpired.Error(), gerrors.CategoryOperation, nil).
			WithTextCode(TextCodeEnvelopeDeadlineExpired)
		projected.Source = err
		return projected
	}

	if retryable := messaging.AsRetryableError(err); retryable != nil {
		return retryable
	}
	return messaging.AsGoError(err)
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
