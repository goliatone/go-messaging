package contracttest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	StartupTimeout       time.Duration
	DeliveryTimeout      time.Duration
	OversizedPayloadSize int
	SynchronousPublish   bool
}

// Run exercises guarantees shared by all duplex drivers. Capability-dependent
// acknowledgement and redelivery behavior is exercised only when advertised.
func Run(t *testing.T, factory Factory, options Options) {
	t.Helper()
	options = withDefaultOptions(options)
	t.Run("lifecycle", func(t *testing.T) { runLifecycle(t, factory, options) })
	t.Run("canceled_start_does_not_become_ready", func(t *testing.T) { runCanceledStart(t, factory) })
	t.Run("requires_start_before_subscribe", func(t *testing.T) { runRequiresStart(t, factory) })
	t.Run("publish_consume_and_metadata", func(t *testing.T) { runPublishConsume(t, factory, options) })
	t.Run("canceled_publish_is_not_accepted", func(t *testing.T) { runCanceledPublish(t, factory, options) })
	t.Run("in_flight_cancellation_is_not_definitely_unpublished", func(t *testing.T) { runInFlightCancellation(t, factory, options) })
	t.Run("oversized_payload_is_rejected", func(t *testing.T) { runOversizedPublish(t, factory, options) })
	t.Run("closed_subscription_stops_delivery", func(t *testing.T) { runSubscriptionClose(t, factory, options) })
	t.Run("concurrent_subscription_close_stops_callbacks", func(t *testing.T) { runConcurrentSubscriptionClose(t, factory, options) })
	t.Run("retry_disposition_redelivers_when_supported", func(t *testing.T) { runRetryDisposition(t, factory, options) })
	t.Run("shutdown_rejects_new_work", func(t *testing.T) { runShutdown(t, factory, options) })
	t.Run("concurrent_shutdown_is_safe", func(t *testing.T) { runConcurrentShutdown(t, factory, options) })
}

func withDefaultOptions(options Options) Options {
	if options.StartupTimeout <= 0 {
		options.StartupTimeout = 5 * time.Second
	}
	if options.DeliveryTimeout <= 0 {
		options.DeliveryTimeout = 5 * time.Second
	}
	if options.OversizedPayloadSize <= 0 {
		options.OversizedPayloadSize = 5 << 20
	}
	return options
}

func runRequiresStart(t *testing.T, factory Factory) {
	t.Helper()
	driver, _, source := factory(t)
	_, err := driver.Subscribe(context.Background(), source, func(context.Context, messaging.Delivery) messaging.HandleResult {
		return messaging.Complete()
	})
	if !errors.Is(err, messaging.ErrSubscriptionNotReady) {
		t.Fatalf("subscribe before start error = %v", err)
	}
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

func runCanceledStart(t *testing.T, factory Factory) {
	t.Helper()
	driver, _, _ := factory(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := driver.Start(ctx); err == nil {
		t.Fatal("canceled start succeeded")
	}
	select {
	case <-driver.Ready():
		t.Fatal("failed start reported ready")
	default:
	}
	if err := driver.Close(context.Background()); err != nil {
		t.Fatal(err)
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
		assertDeliveryImmutable(t, got, envelope)
	case <-ctx.Done():
		t.Fatal(fmt.Errorf("delivery timeout: %w", ctx.Err()))
	}
}

func assertDeliveryImmutable(t *testing.T, delivery messaging.Delivery, published messaging.Envelope) {
	t.Helper()
	firstEnvelope := delivery.Envelope()
	firstInfo := delivery.Info()
	if firstEnvelope.ID != published.ID || firstInfo.Destination == "" {
		t.Fatalf("invalid delivery: %#v %#v", firstEnvelope, firstInfo)
	}
	firstEnvelope.Payload[0] = 'X'
	firstEnvelope.Headers["contract"] = "changed"
	metadataKey, metadataValue := "", ""
	for key, value := range firstInfo.Metadata {
		metadataKey, metadataValue = key, value
		firstInfo.Metadata[key] = "changed"
		break
	}
	secondEnvelope := delivery.Envelope()
	if string(secondEnvelope.Payload) != `{"ok":true}` || secondEnvelope.Headers["contract"] != "true" {
		t.Fatalf("delivery exposed mutable envelope: %#v", secondEnvelope)
	}
	if metadataKey != "" && delivery.Info().Metadata[metadataKey] != metadataValue {
		t.Fatalf("delivery exposed mutable metadata: %#v", delivery.Info().Metadata)
	}
}

func runInFlightCancellation(t *testing.T, factory Factory, options Options) {
	t.Helper()
	if !options.SynchronousPublish {
		t.Skip("driver publication does not synchronously enter the delivery handler")
	}
	driver, destination, source := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.DeliveryTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupDriver(t, driver)
	entered := make(chan struct{})
	subscription, err := driver.Subscribe(ctx, source, func(ctx context.Context, _ messaging.Delivery) messaging.HandleResult {
		close(entered)
		<-ctx.Done()
		return messaging.Reject(ctx.Err())
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, subscription)
	publishCtx, cancelPublish := context.WithCancel(context.Background())
	type publishResponse struct {
		result messaging.PublishResult
		err    error
	}
	response := make(chan publishResponse, 1)
	go func() {
		result, err := driver.Publish(publishCtx, destination, messaging.NewEnvelope("contract-in-flight", "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil))
		response <- publishResponse{result: result, err: err}
	}()
	select {
	case <-entered:
		cancelPublish()
	case <-ctx.Done():
		t.Fatal("publication did not enter handler")
	}
	select {
	case got := <-response:
		if got.result.Outcome == messaging.PublishDefinitelyNotPublished {
			t.Fatalf("in-flight publication was classified definitely not published: result=%#v err=%v", got.result, got.err)
		}
	case <-ctx.Done():
		t.Fatal("in-flight publication did not return")
	}
}

func runCanceledPublish(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, _ := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.StartupTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupDriver(t, driver)
	canceled, cancelPublish := context.WithCancel(context.Background())
	cancelPublish()
	result, err := driver.Publish(canceled, destination, messaging.NewEnvelope("contract-canceled", "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil))
	if err == nil || result.Outcome == messaging.PublishAccepted {
		t.Fatalf("canceled publication accepted: result=%#v err=%v", result, err)
	}
}

func runOversizedPublish(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, _ := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.StartupTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupDriver(t, driver)
	payload := []byte(strings.Repeat("x", options.OversizedPayloadSize))
	result, err := driver.Publish(ctx, destination, messaging.NewEnvelope("contract-oversized", "contract.message", messaging.KindEvent, "1", "application/octet-stream", payload, nil))
	if !errors.Is(err, messaging.ErrMessageTooLarge) || result.Outcome == messaging.PublishAccepted {
		t.Fatalf("oversized publication result=%#v err=%v", result, err)
	}
}

func runSubscriptionClose(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, source := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.DeliveryTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupDriver(t, driver)
	delivered := make(chan struct{}, 1)
	subscription, err := driver.Subscribe(ctx, source, func(context.Context, messaging.Delivery) messaging.HandleResult {
		delivered <- struct{}{}
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscription.Ready():
	case <-ctx.Done():
		t.Fatal("subscription did not become ready")
	}
	if err := subscription.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Publish(ctx, destination, messaging.NewEnvelope("contract-after-sub-close", "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
		t.Fatal("closed subscription received a delivery")
	case <-time.After(100 * time.Millisecond):
	}
}

func runConcurrentSubscriptionClose(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, source := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.DeliveryTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupDriver(t, driver)
	var delivered atomic.Int64
	subscription, err := driver.Subscribe(ctx, source, func(context.Context, messaging.Delivery) messaging.HandleResult {
		delivered.Add(1)
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscription.Ready():
	case <-ctx.Done():
		t.Fatal("subscription did not become ready")
	}
	stopPublishing, _, publisherDone := startPublishLoop(ctx, driver, destination, "contract-close-race", 5*time.Millisecond, false)
	deadline := time.NewTimer(options.DeliveryTimeout)
	defer deadline.Stop()
	for delivered.Load() == 0 {
		select {
		case <-deadline.C:
			stopPublishing()
			<-publisherDone
			t.Fatal("concurrent close test received no initial delivery")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := subscription.Close(ctx); err != nil {
		stopPublishing()
		<-publisherDone
		t.Fatal(err)
	}
	stopPublishing()
	if err := <-publisherDone; err != nil {
		t.Fatal(err)
	}
	afterClose := delivered.Load()
	for index := range 3 {
		id := fmt.Sprintf("contract-after-concurrent-close-%d", index)
		if _, err := driver.Publish(ctx, destination, messaging.NewEnvelope(id, "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if got := delivered.Load(); got != afterClose {
		t.Fatalf("delivery count changed after close: before=%d after=%d", afterClose, got)
	}
}

func runRetryDisposition(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, source := factory(t)
	if !driver.Capabilities().Acknowledgement {
		t.Skip("driver does not advertise acknowledgement/redelivery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), options.DeliveryTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupDriver(t, driver)
	attempts := make(chan int, 2)
	subscription, err := driver.Subscribe(ctx, source, func(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
		attempts <- delivery.Info().Attempt
		if delivery.Info().Attempt == 1 {
			return messaging.Retry(errors.New("contract retry"), 0)
		}
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, subscription)
	select {
	case <-subscription.Ready():
	case <-ctx.Done():
		t.Fatal("subscription did not become ready")
	}
	if _, err := driver.Publish(ctx, destination, messaging.NewEnvelope("contract-retry", "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)); err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 2; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("attempt = %d, want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for attempt %d: %v", want, ctx.Err())
		}
	}
}

func cleanupDriver(t *testing.T, driver messaging.Driver) {
	t.Helper()
	t.Cleanup(func() {
		if err := driver.Close(context.Background()); err != nil {
			t.Errorf("close driver: %v", err)
		}
	})
}

func cleanupSubscription(t *testing.T, subscription messaging.Subscription) {
	t.Helper()
	t.Cleanup(func() {
		if err := subscription.Close(context.Background()); err != nil {
			t.Errorf("close subscription: %v", err)
		}
	})
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

func runConcurrentShutdown(t *testing.T, factory Factory, options Options) {
	t.Helper()
	driver, destination, _ := factory(t)
	ctx, cancel := context.WithTimeout(context.Background(), options.DeliveryTimeout)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}

	stopPublishing, firstAttempt, publisherDone := startPublishLoop(ctx, driver, destination, "contract-shutdown-race", 0, true)

	select {
	case <-firstAttempt:
	case <-ctx.Done():
		stopPublishing()
		<-publisherDone
		t.Fatal("concurrent shutdown test made no publish attempt")
	}
	closeErr := driver.Close(ctx)
	stopPublishing()
	publishErr := <-publisherDone
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if publishErr != nil {
		t.Fatal(publishErr)
	}

	envelope := messaging.NewEnvelope("contract-after-concurrent-shutdown", "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
	result, err := driver.Publish(ctx, destination, envelope)
	if err == nil || result.Outcome == messaging.PublishAccepted {
		t.Fatalf("concurrently closed driver accepted publication: result=%#v err=%v", result, err)
	}
}

func startPublishLoop(ctx context.Context, driver messaging.PublishDriver, destination messaging.Destination, idPrefix string, interval time.Duration, allowAmbiguous bool) (context.CancelFunc, <-chan struct{}, <-chan error) {
	loopCtx, cancel := context.WithCancel(ctx)
	firstAttempt := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		defer close(done)
		var once sync.Once
		var sequence int64
		for {
			sequence++
			id := fmt.Sprintf("%s-%d", idPrefix, sequence)
			result, err := driver.Publish(loopCtx, destination, messaging.NewEnvelope(id, "contract.message", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil))
			once.Do(func() { close(firstAttempt) })
			if err != nil {
				if loopCtx.Err() != nil || result.Outcome == messaging.PublishDefinitelyNotPublished || (allowAmbiguous && result.Outcome == messaging.PublishAmbiguous) {
					done <- nil
					return
				}
				done <- err
				return
			}
			if !waitForNextPublish(loopCtx, interval) {
				done <- nil
				return
			}
		}
	}()
	return cancel, firstAttempt, done
}

func waitForNextPublish(ctx context.Context, interval time.Duration) bool {
	if interval <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
