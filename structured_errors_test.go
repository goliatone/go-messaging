package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	gerrors "github.com/goliatone/go-errors"
)

func TestAsGoErrorMapsStableMessagingClasses(t *testing.T) {
	tests := []struct {
		err      error
		category gerrors.Category
		textCode string
	}{
		{ErrInvalidEnvelope, gerrors.CategoryValidation, TextCodeInvalidEnvelope},
		{ErrSchemaMismatch, gerrors.CategoryValidation, TextCodeSchemaMismatch},
		{ErrMessageTooLarge, gerrors.CategoryBadInput, TextCodeMessageTooLarge},
		{ErrUnknownRoute, gerrors.CategoryRouting, TextCodeUnknownRoute},
		{ErrUnknownDriver, gerrors.CategoryRouting, TextCodeUnknownDriver},
		{ErrUnsupportedCapability, gerrors.CategoryRouting, TextCodeUnsupportedCapability},
		{ErrPublishRejected, gerrors.CategoryOperation, TextCodePublishRejected},
		{ErrPublishAmbiguous, gerrors.CategoryExternal, TextCodePublishAmbiguous},
		{ErrNotPublished, gerrors.CategoryExternal, TextCodeNotPublished},
		{ErrSubscriptionNotReady, gerrors.CategoryExternal, TextCodeSubscriptionNotReady},
		{ErrSubscriptionClosed, gerrors.CategoryExternal, TextCodeSubscriptionClosed},
		{ErrReplyTimeout, gerrors.CategoryOperation, TextCodeReplyTimeout},
		{ErrCorrelation, gerrors.CategoryOperation, TextCodeCorrelationFailure},
		{ErrAcknowledgement, gerrors.CategoryExternal, TextCodeAcknowledgementFailed},
		{ErrUnsupportedDisposition, gerrors.CategoryOperation, TextCodeUnsupportedDisposition},
		{ErrDeadLetter, gerrors.CategoryExternal, TextCodeDeadLetterFailed},
		{ErrHandlerPanic, gerrors.CategoryHandler, TextCodeHandlerPanic},
		{ErrMessageHandling, gerrors.CategoryHandler, TextCodeMessageHandlingFailed},
		{ErrObservedOperation, gerrors.CategoryInternal, TextCodeObservationFailed},
		{context.Canceled, gerrors.CategoryOperation, TextCodeOperationCanceled},
		{context.DeadlineExceeded, gerrors.CategoryOperation, TextCodeOperationDeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.textCode, func(t *testing.T) {
			originalMessage := test.err.Error()
			wrapped := fmt.Errorf("operation context: %w", test.err)
			structured := AsGoError(wrapped)
			if structured == nil {
				t.Fatal("expected structured error")
			}
			if structured.Category != test.category || structured.TextCode != test.textCode {
				t.Fatalf("structured error = %#v", structured)
			}
			if structured.Code != 0 || structured.Location != nil {
				t.Fatalf("transport-neutral projection carried code/location: %#v", structured)
			}
			if !errors.Is(structured, test.err) {
				t.Fatalf("projected error lost errors.Is(%v)", test.err)
			}
			if test.err.Error() != originalMessage {
				t.Fatalf("original error changed from %q to %q", originalMessage, test.err.Error())
			}
		})
	}
}

func TestAsGoErrorPreservesTypedClassificationAndRedactsCause(t *testing.T) {
	cause := errors.New("password=secret")
	transportErr := &TransportError{
		Class: ErrPublishAmbiguous, Transport: "valkey", Operation: "publish",
		Temporary: true, Cause: cause,
	}
	structured := AsGoError(transportErr)
	if structured.Category != gerrors.CategoryExternal || structured.TextCode != TextCodePublishAmbiguous {
		t.Fatalf("structured error = %#v", structured)
	}
	if strings.Contains(structured.Error(), "secret") {
		t.Fatalf("structured error exposed cause: %v", structured)
	}
	if !errors.Is(structured, ErrPublishAmbiguous) || !errors.Is(structured, cause) {
		t.Fatalf("structured error lost compatibility: %v", structured)
	}
	var projectedTransport *TransportError
	if !errors.As(structured, &projectedTransport) || projectedTransport != transportErr {
		t.Fatalf("structured error lost TransportError: %v", structured)
	}
	wantMetadata := map[string]any{"transport": "valkey", "operation": "publish", "temporary": true}
	if fmt.Sprint(structured.Metadata) != fmt.Sprint(wantMetadata) {
		t.Fatalf("metadata = %#v, want %#v", structured.Metadata, wantMetadata)
	}
	if retryable := AsRetryableError(transportErr); retryable != nil {
		t.Fatalf("ambiguous publication must not be retryable: %v", retryable)
	}
}

func TestAsGoErrorClassifiesMessageWrapperBeforeItsCause(t *testing.T) {
	messageErr := NewMessageError(fmt.Errorf("%w: invalid payload", ErrInvalidEnvelope))
	structured := AsGoError(messageErr)
	if structured.Category != gerrors.CategoryHandler || structured.TextCode != TextCodeMessageHandlingFailed {
		t.Fatalf("structured error = %#v", structured)
	}
	if !errors.Is(structured, ErrMessageHandling) || !errors.Is(structured, ErrInvalidEnvelope) {
		t.Fatalf("structured error lost message/cause classification: %v", structured)
	}
}

func TestAsRetryableErrorUsesOnlySafeClasses(t *testing.T) {
	retryableClasses := []error{ErrNotPublished, ErrSubscriptionNotReady, ErrAcknowledgement, ErrDeadLetter}
	for _, err := range retryableClasses {
		retryable := AsRetryableError(err)
		if retryable == nil || !retryable.IsRetryable() {
			t.Fatalf("expected retryable projection for %v", err)
		}
		if retryable.RetryDelay(3) != 0 {
			t.Fatalf("retry delay for %v = %s, want immediate policy", err, retryable.RetryDelay(3))
		}
		if !errors.Is(retryable, err) {
			t.Fatalf("retryable projection lost %v", err)
		}
	}
	for _, err := range []error{ErrPublishAmbiguous, ErrPublishRejected, ErrSubscriptionClosed, context.Canceled, context.DeadlineExceeded} {
		if retryable := AsRetryableError(err); retryable != nil {
			t.Fatalf("unexpected retryable projection for %v: %v", err, retryable)
		}
	}
}

func TestAsGoErrorPreservesExistingStructuredErrors(t *testing.T) {
	existing := gerrors.New("denied", gerrors.CategoryAuthz).WithTextCode("DENIED")
	if got := AsGoError(existing); got != existing {
		t.Fatalf("existing structured error was replaced: %p != %p", got, existing)
	}
	retryable := gerrors.NewRetryableExternal("down")
	if got := AsRetryableError(retryable); got != retryable {
		t.Fatalf("existing retryable error was replaced: %p != %p", got, retryable)
	}
}

func TestUnknownErrorProjectionIsSafeAndCompatible(t *testing.T) {
	cause := errors.New("credential=secret")
	structured := AsGoError(cause)
	if structured.Category != gerrors.CategoryInternal || structured.TextCode != TextCodeInternalError {
		t.Fatalf("structured error = %#v", structured)
	}
	if strings.Contains(structured.Error(), "secret") {
		t.Fatalf("structured error exposed cause: %v", structured)
	}
	if !errors.Is(structured, cause) {
		t.Fatal("structured error lost original cause")
	}
}

func TestErrorSlogAttributesWhitelistMetadata(t *testing.T) {
	cause := errors.New("token=secret")
	err := &TransportError{
		Class: ErrSubscriptionNotReady, Transport: "valkey", Operation: "subscribe",
		Temporary: true, Cause: cause,
	}
	attrs := ErrorSlogAttributes(err)
	serialized := fmt.Sprint(attrs)
	if strings.Contains(serialized, "secret") {
		t.Fatalf("logging attributes exposed cause: %s", serialized)
	}
	for _, expected := range []string{TextCodeSubscriptionNotReady, "external", "valkey", "subscribe", "temporary"} {
		if !strings.Contains(serialized, expected) {
			t.Fatalf("logging attributes %q missing %q", serialized, expected)
		}
	}

	existing := gerrors.New("unsafe", gerrors.CategoryExternal).
		WithTextCode("UNSAFE").
		WithRequestID("request-secret").
		WithMetadata(map[string]any{"token": "secret", "transport": "custom"})
	serialized = fmt.Sprint(ErrorSlogAttributes(existing))
	if strings.Contains(serialized, "secret") || strings.Contains(serialized, "token") {
		t.Fatalf("logging attributes retained unsafe structured data: %s", serialized)
	}
	if !strings.Contains(serialized, "custom") {
		t.Fatalf("logging attributes dropped whitelisted metadata: %s", serialized)
	}

	unsafeTransport := &TransportError{
		Class: ErrSubscriptionNotReady, Transport: "valkey password=secret",
		Operation: "subscribe", Cause: cause,
	}
	serialized = fmt.Sprint(ErrorSlogAttributes(unsafeTransport))
	if strings.Contains(serialized, "secret") || strings.Contains(serialized, "password") {
		t.Fatalf("logging attributes retained unsafe transport label: %s", serialized)
	}
}
