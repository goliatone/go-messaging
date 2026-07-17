package commandadapter

import (
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
	if got := mapper.Map(ErrClaimInProgress, 1); got.Disposition != messaging.DispositionRetry {
		t.Fatalf("claim disposition %#v", got)
	}
	if got := mapper.Map(errors.New("unknown"), 1); got.Disposition != messaging.DispositionRetry {
		t.Fatalf("unknown disposition %#v", got)
	}
}
