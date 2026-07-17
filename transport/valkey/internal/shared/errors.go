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
	class := messaging.ErrSubscriptionNotReady
	if operation == "publish" || operation == "xadd" {
		class = messaging.ErrPublishAmbiguous
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, valkey.ErrClosing) {
		if operation == "publish" || operation == "xadd" {
			class = messaging.ErrNotPublished
		} else {
			class = messaging.ErrSubscriptionClosed
		}
	}
	return &messaging.TransportError{Class: class, Transport: "valkey", Operation: operation, Temporary: true, Cause: err}
}
