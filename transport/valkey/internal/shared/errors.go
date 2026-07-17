package shared

import (
	"context"
	"errors"

	messaging "github.com/goliatone/go-messaging"
	valkey "github.com/valkey-io/valkey-go"
)

func Classify(operation string, err error) error {
	if err == nil {
		return nil
	}
	if operation == "publish" || operation == "xadd" {
		_, classified := PublicationFailure(operation, err, true)
		return classified
	}
	class := messaging.ErrSubscriptionNotReady
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, valkey.ErrClosing) {
		class = messaging.ErrSubscriptionClosed
	}
	return &messaging.TransportError{Class: class, Transport: "valkey", Operation: operation, Temporary: true, Cause: err}
}

// PublicationFailure classifies a failed write according to whether the
// provider operation was attempted. Once Do has been called, cancellation,
// timeouts, and connection failures are conservative/ambiguous because the
// command may already be queued or accepted by Valkey.
func PublicationFailure(operation string, err error, attempted bool) (messaging.PublishOutcome, error) {
	if err == nil {
		return messaging.PublishAccepted, nil
	}
	class := messaging.ErrNotPublished
	outcome := messaging.PublishDefinitelyNotPublished
	if attempted {
		class = messaging.ErrPublishAmbiguous
		outcome = messaging.PublishAmbiguous
	}
	return outcome, &messaging.TransportError{
		Class: class, Transport: "valkey", Operation: operation,
		Temporary: true, Cause: err,
	}
}
