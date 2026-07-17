package commandadapter

import (
	"context"
	"errors"
	"time"

	gerrors "github.com/goliatone/go-errors"
	messaging "github.com/goliatone/go-messaging"
)

type ErrorMapper interface {
	Map(error, int) messaging.HandleResult
}

type DefaultErrorMapper struct {
	InvalidDisposition messaging.Disposition
}

func (m DefaultErrorMapper) Map(err error, attempt int) messaging.HandleResult {
	if err == nil {
		return messaging.Complete()
	}
	if errors.Is(err, ErrClaimInProgress) {
		return messaging.Retry(err, 0)
	}
	if errors.Is(err, ErrClaimConflict) {
		return messaging.Reject(err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return messaging.Retry(err, 0)
	}
	if errors.Is(err, messaging.ErrInvalidEnvelope) || errors.Is(err, messaging.ErrSchemaMismatch) || errors.Is(err, messaging.ErrMessageTooLarge) {
		if m.InvalidDisposition == messaging.DispositionDeadLetter {
			return messaging.DeadLetter(err)
		}
		return messaging.Reject(err)
	}
	var retryable *gerrors.RetryableError
	if gerrors.As(err, &retryable) && retryable.IsRetryable() {
		delay := time.Duration(0)
		if attempt > 0 {
			delay = retryable.RetryDealy(attempt)
		}
		return messaging.Retry(err, delay)
	}
	var structured *gerrors.Error
	if gerrors.As(err, &structured) {
		switch structured.Category {
		case gerrors.CategoryValidation, gerrors.CategoryBadInput, gerrors.CategoryAuth, gerrors.CategoryAuthz, gerrors.CategoryNotFound, gerrors.CategoryConflict:
			return messaging.Reject(err)
		default:
			return messaging.Retry(err, 0)
		}
	}
	return messaging.Retry(err, 0)
}
