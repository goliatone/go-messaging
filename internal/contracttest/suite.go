package contracttest

import (
	"context"
	"fmt"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

type DuplexDriver interface {
	messaging.PublishDriver
	messaging.ConsumeDriver
}

type Factory func(*testing.T) (DuplexDriver, messaging.Destination, messaging.Source)

type Options struct {
	StartupTimeout  time.Duration
	DeliveryTimeout time.Duration
}

// Run exercises the portable lifecycle and publish/consume contract. Driver
// packages add guarantee-specific tests for acknowledgement, replay, and retry.
func Run(t *testing.T, factory Factory, options Options) {
	t.Helper()
	options = withDefaultOptions(options)
	t.Run("lifecycle", func(t *testing.T) { runLifecycle(t, factory, options) })
	t.Run("publish_consume_and_metadata", func(t *testing.T) { runPublishConsume(t, factory, options) })
	t.Run("shutdown_rejects_new_work", func(t *testing.T) { runShutdown(t, factory, options) })
}

func withDefaultOptions(options Options) Options {
	if options.StartupTimeout <= 0 {
		options.StartupTimeout = 5 * time.Second
	}
	if options.DeliveryTimeout <= 0 {
		options.DeliveryTimeout = 5 * time.Second
	}
	return options
}

func runLifecycle(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, _, _ := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.StartupTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-driver.Ready():
	case <-ctx.Done():
		t.Fatal("driver did not become ready")
	}
	if err := driver.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(ctx); err != nil {
		t.Fatalf("close must be idempotent: %v", err)
	}
}

func runPublishConsume(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, source := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.DeliveryTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := driver.Close(context.Background()); err != nil {
			t.Errorf("close driver: %v", err)
		}
	})
	delivered := make(chan messaging.Delivery, 1)
	subscription, err := driver.Subscribe(ctx, source, func(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
		delivered <- delivery
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := subscription.Close(context.Background()); closeErr != nil {
			t.Errorf("close subscription: %v", closeErr)
		}
	})
	select {
	case <-subscription.Ready():
	case <-ctx.Done():
		t.Fatal("subscription did not become ready")
	}
	envelope := messaging.NewEnvelope("contract-1", "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{"ok":true}`), map[string]string{"contract": "true"})
	result, err := driver.Publish(ctx, destination, envelope)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != messaging.PublishAccepted {
		t.Fatalf("publish outcome %q", result.Outcome)
	}
	select {
	case got := <-delivered:
		if got.Envelope().ID != envelope.ID || got.Info().Destination == "" {
			t.Fatalf("invalid delivery: %#v %#v", got.Envelope(), got.Info())
		}
	case <-ctx.Done():
		t.Fatal(fmt.Errorf("delivery timeout: %w", ctx.Err()))
	}
}

func runShutdown(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, _ := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.StartupTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(ctx); err != nil {
		t.Fatal(err)
	}
	envelope := messaging.NewEnvelope("contract-closed", "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
	result, err := driver.Publish(ctx, destination, envelope)
	if err == nil || result.Outcome == messaging.PublishAccepted {
		t.Fatalf("closed driver accepted publication: result=%#v err=%v", result, err)
	}
}
