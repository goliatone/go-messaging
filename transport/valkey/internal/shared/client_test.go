package shared

import (
	"context"
	"testing"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

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
