package chatdemo

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

func TestRunSendUnavailableValkeyDoesNotExposeProviderDetails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	err := runSend(ctx, []string{"--sender", "alice", "--valkey", "127.0.0.1:0", "hello"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("runSend unexpectedly succeeded")
	}
	combined := err.Error() + "\n" + stderr.String()
	if !strings.Contains(combined, messagingUnavailableMessage) {
		t.Fatalf("missing safe error: %q", combined)
	}
	for _, detail := range []string{"dial tcp", "127.0.0.1:0", "connect: connection refused", "NOAUTH"} {
		if strings.Contains(combined, detail) {
			t.Fatalf("provider detail %q escaped: %q", detail, combined)
		}
	}
}

func TestWriteChatMessageText(t *testing.T) {
	view := ChatMessageView{ID: "id", Sender: "alice", Text: "hello", Timestamp: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)}
	var output bytes.Buffer
	if err := WriteChatMessage(&output, view, false); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.Contains(got, "2026-07-17T12:00:00Z") || !strings.Contains(got, "alice") || !strings.Contains(got, "hello") {
		t.Fatalf("unexpected text output %q", got)
	}
}

func TestWriteChatMessageJSON(t *testing.T) {
	view := ChatMessageView{ID: "id", Sender: "alice", Text: "hello", Client: "cli", Timestamp: time.Now().UTC()}
	var output bytes.Buffer
	if err := WriteChatMessage(&output, view, true); err != nil {
		t.Fatal(err)
	}
	var decoded ChatMessageView
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != view.ID || decoded.Sender != view.Sender || decoded.Text != view.Text || decoded.Client != view.Client {
		t.Fatalf("decoded %#v, want %#v", decoded, view)
	}
}

func TestRoutingOutcome(t *testing.T) {
	if got := routingOutcome(messaging.RoutingResult{Results: []messaging.PublishResult{{Outcome: messaging.PublishRejected}}}, nil); got != messaging.PublishRejected {
		t.Fatalf("outcome %q", got)
	}
	if got := routingOutcome(messaging.RoutingResult{}, messaging.ErrPublishAmbiguous); got != messaging.PublishAmbiguous {
		t.Fatalf("outcome %q", got)
	}
}

type monitoredSubscription struct {
	errors <-chan error
}

func (*monitoredSubscription) Ready() <-chan struct{} {
	ready := make(chan struct{})
	close(ready)
	return ready
}

func (s *monitoredSubscription) Errors() <-chan error      { return s.errors }
func (*monitoredSubscription) Close(context.Context) error { return nil }

func TestMonitorMessagingTreatsAllClosedChannelsAsFailure(t *testing.T) {
	driverErrors := make(chan error)
	subscriptionErrors := make(chan error)
	close(driverErrors)
	close(subscriptionErrors)
	err := monitorMessaging(context.Background(), driverErrors, []messaging.Subscription{
		&monitoredSubscription{errors: subscriptionErrors},
	})
	if err == nil || err.Error() != subscriptionFailedMessage {
		t.Fatalf("monitor error = %v", err)
	}
}

func TestMonitorMessagingWaitsForEveryChannelToClose(t *testing.T) {
	driverErrors := make(chan error)
	subscriptionErrors := make(chan error)
	close(driverErrors)
	result := make(chan error, 1)
	go func() {
		result <- monitorMessaging(context.Background(), driverErrors, []messaging.Subscription{
			&monitoredSubscription{errors: subscriptionErrors},
		})
	}()
	select {
	case err := <-result:
		t.Fatalf("monitor returned before every channel closed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(subscriptionErrors)
	select {
	case err := <-result:
		if err == nil || err.Error() != subscriptionFailedMessage {
			t.Fatalf("monitor error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("monitor did not report closed channels")
	}
}

func TestMonitorMessagingCancellationIsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := monitorMessaging(ctx, make(chan error), nil); err != nil {
		t.Fatalf("canceled monitor returned %v", err)
	}
}
