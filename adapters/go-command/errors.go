package commandadapter

import (
	"context"
	"errors"
	"time"

	gerrors "github.com/goliatone/go-errors"
	messaging "github.com/goliatone/go-messaging"
)

var ErrEnvelopeDeadlineExpired = errors.New("go-command adapter: transported envelope deadline expired")

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
	if errors.Is(err, ErrClaimInProgress) {
		return messaging.Retry(err, 0)
	}
	if errors.Is(err, ErrClaimConflict) {
		return messaging.Reject(err)
	}
	if errors.Is(err, ErrEnvelopeDeadlineExpired) {
		return terminalDisposition(err, m.ExpiredDisposition)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return messaging.Retry(err, 0)
	}
	if errors.Is(err, messaging.ErrInvalidEnvelope) || errors.Is(err, messaging.ErrSchemaMismatch) || errors.Is(err, messaging.ErrMessageTooLarge) {
		return terminalDisposition(err, m.InvalidDisposition)
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
		return mapStructuredError(err, structured)
	}
	return messaging.Retry(err, 0)
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
