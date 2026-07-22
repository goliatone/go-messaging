package messaging

import (
	"context"
	"errors"
	"log/slog"
	"time"

	gerrors "github.com/goliatone/go-errors"
)

const (
	TextCodeInvalidEnvelope           = "INVALID_ENVELOPE"
	TextCodeSchemaMismatch            = "SCHEMA_MISMATCH"
	TextCodeMessageTooLarge           = "MESSAGE_TOO_LARGE"
	TextCodeUnknownRoute              = "UNKNOWN_ROUTE"
	TextCodeUnknownDriver             = "UNKNOWN_DRIVER"
	TextCodeUnsupportedCapability     = "UNSUPPORTED_CAPABILITY"
	TextCodePublishRejected           = "PUBLISH_REJECTED"
	TextCodePublishAmbiguous          = "PUBLISH_AMBIGUOUS"
	TextCodeNotPublished              = "NOT_PUBLISHED"
	TextCodeSubscriptionNotReady      = "SUBSCRIPTION_NOT_READY"
	TextCodeSubscriptionClosed        = "SUBSCRIPTION_CLOSED"
	TextCodeReplyTimeout              = "REPLY_TIMEOUT"
	TextCodeCorrelationFailure        = "CORRELATION_FAILURE"
	TextCodeAcknowledgementFailed     = "ACKNOWLEDGEMENT_FAILED"
	TextCodeUnsupportedDisposition    = "UNSUPPORTED_DISPOSITION"
	TextCodeDeadLetterFailed          = "DEAD_LETTER_FAILED"
	TextCodeHandlerPanic              = "HANDLER_PANIC"
	TextCodeMessageHandlingFailed     = "MESSAGE_HANDLING_FAILED"
	TextCodeObservationFailed         = "OBSERVATION_FAILED"
	TextCodeOperationCanceled         = "OPERATION_CANCELED"
	TextCodeOperationDeadlineExceeded = "OPERATION_DEADLINE_EXCEEDED"
	TextCodeInternalError             = "MESSAGING_INTERNAL_ERROR"
)

type structuredErrorPolicy struct {
	category   gerrors.Category
	textCode   string
	message    string
	retryable  bool
	retryDelay time.Duration
	severity   gerrors.Severity
}

type safeStructuredSource struct {
	message string
	cause   error
}

type structuredErrorBoundary struct {
	policy     structuredErrorPolicy
	metadata   map[string]any
	structured *gerrors.Error
	retryable  *gerrors.RetryableError
}

func (e *safeStructuredSource) Error() string {
	if e == nil {
		return "messaging: operation failed"
	}
	return e.message
}

func (e *safeStructuredSource) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// AsGoError projects err into the shared structured error contract without
// changing the error returned by the messaging API. Existing go-errors values
// pass through unchanged. Package errors retain errors.Is/errors.As behavior
// through a source wrapper whose Error string is safe to expose.
func AsGoError(err error) *gerrors.Error {
	if err == nil {
		return nil
	}
	if boundary, ok := firstStructuredErrorBoundary(err); ok {
		switch {
		case boundary.structured != nil:
			return boundary.structured
		case boundary.retryable != nil:
			if boundary.retryable.BaseError != nil {
				return boundary.retryable.BaseError
			}
			return newStructuredErrorProjection(err, policyForClass(err), nil)
		default:
			return newStructuredErrorProjection(err, boundary.policy, boundary.metadata)
		}
	}

	policy, metadata := structuredPolicyFor(err)
	return newStructuredErrorProjection(err, policy, metadata)
}

func newStructuredErrorProjection(err error, policy structuredErrorPolicy, metadata map[string]any) *gerrors.Error {
	structured := gerrors.NewWithLocation(policy.message, policy.category, nil).
		WithTextCode(policy.textCode).
		WithSeverity(policy.severity)
	structured.Source = &safeStructuredSource{message: policy.message, cause: err}
	if len(metadata) > 0 {
		structured = structured.WithMetadata(metadata)
	}
	return structured
}

// AsRetryableError projects err as retryable only when its stable messaging
// class proves that retrying is safe. In particular, ambiguous publications
// are never retryable even when a transport marks the failure temporary. A
// non-nil return value is guaranteed to report IsRetryable() == true.
func AsRetryableError(err error) *gerrors.RetryableError {
	if err == nil {
		return nil
	}
	if boundary, ok := firstStructuredErrorBoundary(err); ok {
		switch {
		case boundary.retryable != nil:
			if boundary.retryable.IsRetryable() {
				return boundary.retryable
			}
			return nil
		case boundary.structured != nil:
			return nil //nolint:nilerr // An existing structured error is an explicit non-retry boundary.
		case !boundary.policy.retryable:
			return nil
		default:
			return newRetryableErrorProjection(err, boundary.policy, boundary.metadata)
		}
	}

	policy, metadata := structuredPolicyFor(err)
	if !policy.retryable {
		return nil
	}
	return newRetryableErrorProjection(err, policy, metadata)
}

func newRetryableErrorProjection(err error, policy structuredErrorPolicy, metadata map[string]any) *gerrors.RetryableError {
	retryable := gerrors.NewRetryable(policy.message, policy.category).
		WithTextCode(policy.textCode).
		WithRetryDelay(policy.retryDelay).
		WithSeverity(policy.severity).
		WithLocation(nil)
	retryable.Source = &safeStructuredSource{message: policy.message, cause: err}
	if len(metadata) > 0 {
		retryable = retryable.WithMetadata(metadata)
	}
	return retryable
}

// firstStructuredErrorBoundary finds the outermost owner of structured error
// semantics. Messaging wrappers intentionally stop traversal so their stable
// class cannot be overridden by a provider cause. Existing go-errors values
// also stop traversal so application-supplied structure passes through.
func firstStructuredErrorBoundary(err error) (structuredErrorBoundary, bool) {
	if err == nil {
		return structuredErrorBoundary{}, false
	}

	switch current := err.(type) { //nolint:errorlint // Direct inspection preserves outer-to-inner boundary precedence.
	case *MessageError:
		if current != nil {
			return structuredErrorBoundary{policy: policyForClass(ErrMessageHandling)}, true
		}
	case *TransportError:
		if current != nil {
			return structuredErrorBoundary{
				policy: policyForClass(current.Class),
				metadata: safeStructuredMetadata(map[string]any{
					"transport": current.Transport,
					"operation": current.Operation,
					"temporary": current.Temporary,
				}),
			}, true
		}
	case *gerrors.RetryableError:
		if current != nil {
			return structuredErrorBoundary{retryable: current}, true
		}
	case *gerrors.Error:
		if current != nil {
			return structuredErrorBoundary{structured: current}, true
		}
	}

	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if boundary, found := firstStructuredErrorBoundary(child); found {
				return boundary, true
			}
		}
		return structuredErrorBoundary{}, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return firstStructuredErrorBoundary(wrapped.Unwrap())
	}
	return structuredErrorBoundary{}, false
}

// ErrorSlogAttributes returns structured attributes safe for library logging.
// It deliberately drops sources, stack/location data, request IDs, validation
// payloads, and metadata outside the messaging whitelist.
func ErrorSlogAttributes(err error) []slog.Attr {
	structured := AsGoError(err)
	if structured == nil {
		return nil
	}
	safe := structured.Clone()
	safe.Source = nil
	safe.StackTrace = nil
	safe.Location = nil
	safe.RequestID = ""
	safe.ValidationErrors = nil
	metadata := safeStructuredMetadata(safe.Metadata)
	// Render the package-owned allowlist ourselves. go-errors v0.12 and later
	// deliberately omit metadata from the default secure slog renderer, while
	// older versions included the whole map. Clearing it before delegation keeps
	// the result safe and makes this public helper stable across both behaviors.
	safe.Metadata = nil
	attrs := gerrors.ToSlogAttributes(safe)
	if len(metadata) > 0 {
		attrs = append(attrs, slog.Any("metadata", metadata))
	}
	return attrs
}

func structuredPolicyFor(err error) (structuredErrorPolicy, map[string]any) {
	var messageErr *MessageError
	if errors.As(err, &messageErr) {
		return policyForClass(ErrMessageHandling), nil
	}

	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		policy := policyForClass(transportErr.Class)
		return policy, safeStructuredMetadata(map[string]any{
			"transport": transportErr.Transport,
			"operation": transportErr.Operation,
			"temporary": transportErr.Temporary,
		})
	}

	return policyForClass(err), nil
}

func policyForClass(err error) structuredErrorPolicy {
	policies := []struct {
		target error
		policy structuredErrorPolicy
	}{
		{ErrInvalidEnvelope, newStructuredPolicy(gerrors.CategoryValidation, TextCodeInvalidEnvelope, ErrInvalidEnvelope.Error())},
		{ErrSchemaMismatch, newStructuredPolicy(gerrors.CategoryValidation, TextCodeSchemaMismatch, ErrSchemaMismatch.Error())},
		{ErrMessageTooLarge, newStructuredPolicy(gerrors.CategoryBadInput, TextCodeMessageTooLarge, ErrMessageTooLarge.Error())},
		{ErrUnknownRoute, newStructuredPolicy(gerrors.CategoryRouting, TextCodeUnknownRoute, ErrUnknownRoute.Error())},
		{ErrUnknownDriver, newStructuredPolicy(gerrors.CategoryRouting, TextCodeUnknownDriver, ErrUnknownDriver.Error())},
		{ErrUnsupportedCapability, newStructuredPolicy(gerrors.CategoryRouting, TextCodeUnsupportedCapability, ErrUnsupportedCapability.Error())},
		{ErrPublishRejected, newStructuredPolicy(gerrors.CategoryOperation, TextCodePublishRejected, ErrPublishRejected.Error())},
		{ErrPublishAmbiguous, newStructuredPolicy(gerrors.CategoryExternal, TextCodePublishAmbiguous, ErrPublishAmbiguous.Error())},
		{ErrNotPublished, newRetryableStructuredPolicy(gerrors.CategoryExternal, TextCodeNotPublished, ErrNotPublished.Error())},
		{ErrSubscriptionNotReady, newRetryableStructuredPolicy(gerrors.CategoryExternal, TextCodeSubscriptionNotReady, ErrSubscriptionNotReady.Error())},
		{ErrSubscriptionClosed, newStructuredPolicy(gerrors.CategoryExternal, TextCodeSubscriptionClosed, ErrSubscriptionClosed.Error())},
		{ErrReplyTimeout, newStructuredPolicy(gerrors.CategoryOperation, TextCodeReplyTimeout, ErrReplyTimeout.Error())},
		{ErrCorrelation, newStructuredPolicy(gerrors.CategoryOperation, TextCodeCorrelationFailure, ErrCorrelation.Error())},
		{ErrAcknowledgement, newRetryableStructuredPolicy(gerrors.CategoryExternal, TextCodeAcknowledgementFailed, ErrAcknowledgement.Error())},
		{ErrUnsupportedDisposition, newStructuredPolicy(gerrors.CategoryOperation, TextCodeUnsupportedDisposition, ErrUnsupportedDisposition.Error())},
		{ErrDeadLetter, newRetryableStructuredPolicy(gerrors.CategoryExternal, TextCodeDeadLetterFailed, ErrDeadLetter.Error())},
		{ErrHandlerPanic, newStructuredPolicy(gerrors.CategoryHandler, TextCodeHandlerPanic, ErrHandlerPanic.Error()).withSeverity(gerrors.SeverityCritical)},
		{ErrMessageHandling, newStructuredPolicy(gerrors.CategoryHandler, TextCodeMessageHandlingFailed, ErrMessageHandling.Error())},
		{ErrObservedOperation, newStructuredPolicy(gerrors.CategoryInternal, TextCodeObservationFailed, ErrObservedOperation.Error()).withSeverity(gerrors.SeverityWarning)},
		{context.Canceled, newStructuredPolicy(gerrors.CategoryOperation, TextCodeOperationCanceled, context.Canceled.Error())},
		{context.DeadlineExceeded, newStructuredPolicy(gerrors.CategoryOperation, TextCodeOperationDeadlineExceeded, context.DeadlineExceeded.Error())},
	}
	for _, candidate := range policies {
		if errors.Is(err, candidate.target) {
			return candidate.policy
		}
	}
	return newStructuredPolicy(gerrors.CategoryInternal, TextCodeInternalError, "messaging: operation failed")
}

func newStructuredPolicy(category gerrors.Category, textCode, message string) structuredErrorPolicy {
	return structuredErrorPolicy{
		category: category,
		textCode: textCode,
		message:  message,
		severity: gerrors.SeverityError,
	}
}

func newRetryableStructuredPolicy(category gerrors.Category, textCode, message string) structuredErrorPolicy {
	policy := newStructuredPolicy(category, textCode, message)
	policy.retryable = true
	return policy
}

func (p structuredErrorPolicy) withSeverity(severity gerrors.Severity) structuredErrorPolicy {
	p.severity = severity
	return p
}

func safeStructuredMetadata(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	safe := make(map[string]any, 3)
	for _, key := range []string{"transport", "operation", "temporary"} {
		value, ok := metadata[key]
		if !ok {
			continue
		}
		switch key {
		case "transport", "operation":
			if text, ok := value.(string); ok {
				if label := safeStructuredLabel(text); label != "" {
					safe[key] = label
				}
			}
		case "temporary":
			if temporary, ok := value.(bool); ok {
				safe[key] = temporary
			}
		}
	}
	if len(safe) == 0 {
		return nil
	}
	return safe
}

func safeStructuredLabel(value string) string {
	if value == "" || len(value) > 64 {
		return ""
	}
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		switch char {
		case '.', '_', '-':
			continue
		default:
			return ""
		}
	}
	return value
}
