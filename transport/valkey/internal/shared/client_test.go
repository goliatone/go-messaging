package shared

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
	valkey "github.com/valkey-io/valkey-go"
)

func TestPublicationFailureIsAmbiguousAfterAttempt(t *testing.T) {
	cause := errors.New("password=secret")
	outcome, err := PublicationFailure("xadd", cause, true)
	if outcome != messaging.PublishAmbiguous || !errors.Is(err, messaging.ErrPublishAmbiguous) || !errors.Is(err, cause) {
		t.Fatalf("outcome=%q err=%v", outcome, err)
	}
	structured := messaging.AsGoError(err)
	if structured.Category.String() != "external" || structured.TextCode != messaging.TextCodePublishAmbiguous {
		t.Fatalf("structured error = %#v", structured)
	}
	if strings.Contains(structured.Error(), "secret") {
		t.Fatalf("structured error exposed provider cause: %v", structured)
	}
	if structured.Metadata["transport"] != "valkey" || structured.Metadata["operation"] != "xadd" || structured.Metadata["temporary"] != true {
		t.Fatalf("metadata = %#v", structured.Metadata)
	}
	if retryable := messaging.AsRetryableError(err); retryable != nil {
		t.Fatalf("attempted publication must not be automatically retryable: %v", retryable)
	}
}

func TestPublicationFailureIsDefiniteBeforeAttempt(t *testing.T) {
	cause := errors.New("address=secret")
	outcome, err := PublicationFailure("publish", cause, false)
	if outcome != messaging.PublishDefinitelyNotPublished || !errors.Is(err, messaging.ErrNotPublished) || !errors.Is(err, cause) {
		t.Fatalf("outcome=%q err=%v", outcome, err)
	}
	retryable := messaging.AsRetryableError(err)
	if retryable == nil || !retryable.IsRetryable() || retryable.TextCode != messaging.TextCodeNotPublished {
		t.Fatalf("pre-attempt publication projection = %#v", retryable)
	}
	if strings.Contains(retryable.Error(), "secret") || !errors.Is(retryable, cause) {
		t.Fatalf("retryable projection is unsafe or incompatible: %v", retryable)
	}
}

func TestLifecycleClassificationProjectsStableRetryPolicy(t *testing.T) {
	providerErr := errors.New("credential=secret")
	notReady := Classify("subscribe", providerErr)
	structured := messaging.AsGoError(notReady)
	if structured.TextCode != messaging.TextCodeSubscriptionNotReady || strings.Contains(structured.Error(), "secret") {
		t.Fatalf("not-ready projection = %v", structured)
	}
	if retryable := messaging.AsRetryableError(notReady); retryable == nil || !retryable.IsRetryable() {
		t.Fatalf("not-ready failure should be retryable: %#v", retryable)
	}

	closed := Classify("receive", context.Canceled)
	structured = messaging.AsGoError(closed)
	if structured.TextCode != messaging.TextCodeSubscriptionClosed {
		t.Fatalf("closed projection = %#v", structured)
	}
	if retryable := messaging.AsRetryableError(closed); retryable != nil {
		t.Fatalf("closed subscription must not imply restart: %v", retryable)
	}
}

type contextCapturingClient struct {
	valkey.Client
	deadline time.Time
}

func (c *contextCapturingClient) Do(ctx context.Context, _ valkey.Completed) valkey.ValkeyResult {
	c.deadline, _ = ctx.Deadline()
	return valkey.ValkeyResult{}
}

func TestClientOptionAppliesOperationAndReconnectPolicy(t *testing.T) {
	config := DefaultConfig("127.0.0.1:6379")
	config.OperationTimeout = 3 * time.Second
	config.ReconnectMin = 25 * time.Millisecond
	config.ReconnectMax = 100 * time.Millisecond
	option := clientOption(config)
	if option.ConnWriteTimeout != config.OperationTimeout {
		t.Fatalf("connection timeout = %s", option.ConnWriteTimeout)
	}
	wants := []time.Duration{25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond}
	for attempt, want := range wants {
		if got := option.RetryDelay(attempt, valkey.Completed{}, nil); got != want {
			t.Fatalf("retry delay %d = %s, want %s", attempt, got, want)
		}
	}
}

func TestOpenWithBoundsOperations(t *testing.T) {
	config := DefaultConfig("127.0.0.1:6379")
	config.OperationTimeout = 40 * time.Millisecond
	captured := &contextCapturingClient{}
	client, err := OpenWith(config, func(Config) (valkey.Client, error) { return captured, nil })
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	client.Do(context.Background(), valkey.Completed{})
	if captured.deadline.Before(before.Add(20*time.Millisecond)) || captured.deadline.After(before.Add(100*time.Millisecond)) {
		t.Fatalf("operation deadline = %s", captured.deadline)
	}
}
