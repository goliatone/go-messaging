package commandadapter

import (
	"context"
	"errors"
	"testing"

	gerrors "github.com/goliatone/go-errors"
	messaging "github.com/goliatone/go-messaging"
)

func TestDefaultErrorMapperClassifiesWithoutInspectingPayloads(t *testing.T) {
	mapper := DefaultErrorMapper{InvalidDisposition: messaging.DispositionDeadLetter}
	if got := mapper.Map(messaging.ErrInvalidEnvelope, 1); got.Disposition != messaging.DispositionDeadLetter {
		t.Fatalf("invalid disposition %#v", got)
	}
	if got := mapper.Map(gerrors.New("denied", gerrors.CategoryAuthz), 1); got.Disposition != messaging.DispositionReject {
		t.Fatalf("auth disposition %#v", got)
	}
	if got := mapper.Map(gerrors.New("down", gerrors.CategoryExternal), 1); got.Disposition != messaging.DispositionRetry {
		t.Fatalf("external disposition %#v", got)
	}
	if got := mapper.Map(messaging.ErrPublishAmbiguous, 1); got.Disposition != messaging.DispositionReject {
		t.Fatalf("ambiguous publication disposition %#v", got)
	}
	if got := mapper.Map(messaging.ErrNotPublished, 1); got.Disposition != messaging.DispositionRetry {
		t.Fatalf("definite publication disposition %#v", got)
	}
	if got := mapper.Map(ErrClaimInProgress, 1); got.Disposition != messaging.DispositionRetry {
		t.Fatalf("claim disposition %#v", got)
	}
	expired := expiredEnvelopeDeadline(context.DeadlineExceeded)
	if got := mapper.Map(expired, 1); got.Disposition != messaging.DispositionReject {
		t.Fatalf("expired disposition %#v", got)
	}
	deadLetterMapper := DefaultErrorMapper{ExpiredDisposition: messaging.DispositionDeadLetter}
	if got := deadLetterMapper.Map(expired, 1); got.Disposition != messaging.DispositionDeadLetter {
		t.Fatalf("expired dead-letter disposition %#v", got)
	}
	if got := mapper.Map(errors.New("unknown"), 1); got.Disposition != messaging.DispositionRetry {
		t.Fatalf("unknown disposition %#v", got)
	}
}

func TestProjectAdapterErrorMapsLocalSentinels(t *testing.T) {
	tests := []struct {
		err       error
		target    error
		textCode  string
		retryable bool
	}{
		{ErrClaimInProgress, ErrClaimInProgress, TextCodeIdempotencyInProgress, true},
		{ErrClaimConflict, ErrClaimConflict, TextCodeIdempotencyConflict, false},
		{expiredEnvelopeDeadline(context.DeadlineExceeded), ErrEnvelopeDeadlineExpired, TextCodeEnvelopeDeadlineExpired, false},
	}
	for _, test := range tests {
		t.Run(test.textCode, func(t *testing.T) {
			projected := projectAdapterError(test.err)
			var structured *gerrors.Error
			if !errors.As(projected, &structured) || structured.TextCode != test.textCode {
				t.Fatalf("projected error = %#v", projected)
			}
			if !errors.Is(projected, test.target) {
				t.Fatalf("projected error lost source: %v", projected)
			}
			var retryable *gerrors.RetryableError
			gotRetryable := errors.As(projected, &retryable) && retryable.IsRetryable()
			if gotRetryable != test.retryable {
				t.Fatalf("retryable = %v, want %v", gotRetryable, test.retryable)
			}
		})
	}
}
