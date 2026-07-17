package streams

import (
	"context"
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
