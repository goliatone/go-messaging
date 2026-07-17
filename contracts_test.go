package messaging

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBasicDeliveryIsImmutable(t *testing.T) {
	e := validEnvelope()
	d := NewDelivery(e, DeliveryInfo{Metadata: map[string]string{"provider": "x"}})
	first := d.Envelope()
	first.Payload[0] = 'X'
	first.Headers["trace"] = "changed"
	info := d.Info()
	info.Metadata["provider"] = "changed"
	if d.Envelope().Payload[0] == 'X' || d.Envelope().Headers["trace"] != "abc" || d.Info().Metadata["provider"] != "x" {
		t.Fatal("delivery exposed mutable driver state")
	}
}

func TestTransportErrorClassificationDoesNotExposeCause(t *testing.T) {
	cause := errors.New("password=secret")
	err := &TransportError{Class: ErrPublishAmbiguous, Transport: "valkey", Operation: "publish", Cause: cause}
	if !errors.Is(err, ErrPublishAmbiguous) {
		t.Fatal("classification was not retained")
	}
	if got := err.Error(); got == "" || got == cause.Error() {
		t.Fatalf("unsafe error: %q", got)
	}
}

func TestObservationErrorDoesNotExposeUnknownCause(t *testing.T) {
	err := safeObservationError(errors.New("credential=secret"))
	if !errors.Is(err, ErrObservedOperation) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe observation error: %v", err)
	}
}

func TestObservationTransportErrorDropsProviderCause(t *testing.T) {
	cause := errors.New("password=secret")
	err := safeObservationError(&TransportError{Class: ErrPublishAmbiguous, Transport: "valkey", Operation: "publish", Cause: cause})
	if !errors.Is(err, ErrPublishAmbiguous) || errors.Is(err, cause) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe observation transport error: %v", err)
	}
}

func TestObservationTransportErrorNormalizesUnknownClass(t *testing.T) {
	err := safeObservationError(&TransportError{Class: errors.New("credential=secret"), Transport: "custom", Operation: "publish"})
	if !errors.Is(err, ErrObservedOperation) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe observation class: %v", err)
	}
}

func TestMessageErrorClassifiesRecoverableConsumptionFailureWithoutExposingCause(t *testing.T) {
	cause := errors.New("payload=secret")
	err := NewMessageError(cause)
	if !IsMessageError(err) || !errors.Is(err, cause) || err.Error() != ErrMessageHandling.Error() {
		t.Fatalf("message error = %v", err)
	}
}

func TestDispositionConstructors(t *testing.T) {
	err := errors.New("try again")
	got := Retry(err, time.Second)
	if got.Disposition != DispositionRetry || got.RetryAfter != time.Second || !errors.Is(got.Err, err) {
		t.Fatalf("got %#v", got)
	}
}

func TestInvokeHandlerContainsPanic(t *testing.T) {
	result := InvokeHandler(context.Background(), func(context.Context, Delivery) HandleResult {
		panic("payload=secret")
	}, NewDelivery(validEnvelope(), DeliveryInfo{}))
	if result.Disposition != DispositionReject || !errors.Is(result.Err, ErrHandlerPanic) || strings.Contains(result.Err.Error(), "secret") {
		t.Fatalf("unsafe panic result: %#v", result)
	}
}
