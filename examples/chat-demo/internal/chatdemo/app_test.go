package chatdemo

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	messaging "github.com/goliatone/go-messaging"
)

func TestObserverLogsMetadataWithoutPayloadOrCredentials(t *testing.T) {
	var output bytes.Buffer
	observer := newObserver(&output)
	observer.Observe(context.Background(), messaging.Observation{
		Operation: messaging.OperationPublish, LogicalRoute: ChatRoute,
		Transport: "valkey-pubsub", Outcome: "failed",
		Err: errors.New("NOAUTH invalid password for redis://user:secret@provider"),
	})
	logLine := output.String()
	if !strings.Contains(logLine, "route="+ChatRoute) || !strings.Contains(logLine, "outcome=failed") {
		t.Fatalf("missing observation metadata: %q", logLine)
	}
	for _, secret := range []string{"message text", "NOAUTH", "secret", "redis://", "provider"} {
		if strings.Contains(logLine, secret) {
			t.Fatalf("log exposed %q: %s", secret, logLine)
		}
	}
	if !strings.Contains(logLine, "error=\"messaging operation failed\"") {
		t.Fatalf("missing safe failure diagnostic: %q", logLine)
	}
}

func TestSafeErrorPreservesCauseWithoutExposingIt(t *testing.T) {
	cause := errors.New("NOAUTH secret provider response")
	err := safeError(messagingUnavailableMessage, cause)
	if err.Error() != messagingUnavailableMessage {
		t.Fatalf("error = %q", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("safe error did not preserve its cause")
	}
}
