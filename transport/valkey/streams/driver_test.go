package streams

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/internal/contracttest"
)

func TestStreamsContract(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required for the Streams contract suite")
	}
	contracttest.Run(t, func(t *testing.T) (contracttest.DuplexDriver, messaging.Destination, messaging.Source) {
		config := DefaultConfig(address)
		config.ClaimIdle = 50 * time.Millisecond
		config.ClaimInterval = 20 * time.Millisecond
		driver, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		name := "contract-" + time.Now().Format("150405.000000000")
		return driver, messaging.Destination{Name: name}, messaging.Source{Name: name, Group: "contract", Consumer: "worker-1", From: "0"}
	}, contracttest.Options{})
}

func TestCapabilitiesAreDurable(t *testing.T) {
	driver, err := New(DefaultConfig("127.0.0.1:6379"))
	if err != nil {
		t.Fatal(err)
	}
	caps := driver.Capabilities()
	if !caps.Durability || !caps.Acknowledgement || !caps.CompetingConsumers || !caps.Replay {
		t.Fatalf("unexpected capabilities %#v", caps)
	}
}

func TestManualRetryNackSettlesCurrentInvocation(t *testing.T) {
	driver, err := New(DefaultConfig("127.0.0.1:6379"))
	if err != nil {
		t.Fatal(err)
	}
	sub := &subscription{driver: driver, errors: make(chan error, 1)}
	delivery := &streamDelivery{
		BasicDelivery: messaging.NewDelivery(messaging.Envelope{}, messaging.DeliveryInfo{Attempt: 1}),
		subscription:  sub,
	}
	if err := delivery.Nack(context.Background(), messaging.NackOptions{Disposition: messaging.DispositionRetry}); err != nil {
		t.Fatal(err)
	}
	if !delivery.settled.Load() {
		t.Fatal("retry nack must settle the current handler invocation")
	}
}

func TestManualRetryNackReportsUnsupportedExactDelay(t *testing.T) {
	driver, err := New(DefaultConfig("127.0.0.1:6379"))
	if err != nil {
		t.Fatal(err)
	}
	sub := &subscription{driver: driver, errors: make(chan error, 1)}
	delivery := &streamDelivery{
		BasicDelivery: messaging.NewDelivery(messaging.Envelope{}, messaging.DeliveryInfo{Attempt: 1}),
		subscription:  sub,
	}
	err = delivery.Nack(context.Background(), messaging.NackOptions{Disposition: messaging.DispositionRetry, RetryAfter: time.Second})
	if !errors.Is(err, messaging.ErrUnsupportedCapability) {
		t.Fatalf("expected unsupported delay error, got %v", err)
	}
	select {
	case reported := <-sub.errors:
		if !errors.Is(reported, messaging.ErrUnsupportedCapability) {
			t.Fatalf("unexpected report: %v", reported)
		}
	default:
		t.Fatal("unsupported delay was not reported")
	}
}

func TestDeadLetterReasonDoesNotExposeArbitraryErrors(t *testing.T) {
	if got := safeDeadLetterReason(errors.New("payload=secret")); got != "rejected" {
		t.Fatalf("unsafe dead-letter reason %q", got)
	}
	if got := safeDeadLetterReason(context.DeadlineExceeded); got != context.DeadlineExceeded.Error() {
		t.Fatalf("classified reason = %q", got)
	}
}

func TestRetryDeadLettersAfterLimit(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		t.Skip("VALKEY_ADDRESS is required")
	}
	config := DefaultConfig(address)
	config.ClaimIdle = 20 * time.Millisecond
	config.ClaimInterval = 10 * time.Millisecond
	config.Block = 20 * time.Millisecond
	config.MaxDeliveries = 2
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		if closeErr := driver.Close(context.Background()); closeErr != nil {
			t.Errorf("close driver: %v", closeErr)
		}
	})
	name := "retry-" + time.Now().Format("150405.000000000")
	attempts := make(chan int, 4)
	sub, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "0"}, func(_ context.Context, d messaging.Delivery) messaging.HandleResult {
		attempts <- d.Info().Attempt
		return messaging.Retry(context.DeadlineExceeded, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := sub.Close(context.Background()); closeErr != nil {
			t.Errorf("close subscription: %v", closeErr)
		}
	})
	<-sub.Ready()
	envelope := messaging.NewEnvelope("retry-1", "job", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
	if _, err := driver.Publish(ctx, messaging.Destination{Name: name}, envelope); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-attempts:
		case <-ctx.Done():
			t.Fatal("expected redelivery")
		}
	}
}
