package streams

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/internal/contracttest"
	valkey "github.com/valkey-io/valkey-go"
)

func TestStreamsContract(t *testing.T) {
	address := requireValkeyAddress(t)
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
	address := requireValkeyAddress(t)
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
	client := newTestClient(t, address)
	waitFor(t, ctx, func() bool {
		length, lengthErr := client.Do(ctx, client.B().Xlen().Key(name+config.DeadLetterSuffix).Build()).AsInt64()
		return lengthErr == nil && length == 1
	})
}

func TestManualRetryNackOverridesCompleteAndDeadLettersAtLimit(t *testing.T) {
	address := requireValkeyAddress(t)
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
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	name := "manual-retry-" + time.Now().Format("150405.000000000")
	attempts := make(chan int, 4)
	sub, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "0"}, func(ctx context.Context, delivery messaging.Delivery) messaging.HandleResult {
		attempts <- delivery.Info().Attempt
		acknowledger, ok := delivery.(messaging.Acknowledger)
		if !ok {
			t.Error("streams delivery does not expose acknowledgements")
			return messaging.Reject(errors.New("missing acknowledger"))
		}
		if nackErr := acknowledger.Nack(ctx, messaging.NackOptions{Disposition: messaging.DispositionRetry, Reason: "payload=secret"}); nackErr != nil {
			t.Errorf("retry nack: %v", nackErr)
		}
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close(context.Background()) })
	<-sub.Ready()
	envelope := messaging.NewEnvelope("manual-retry-1", "job", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
	if _, err := driver.Publish(ctx, messaging.Destination{Name: name}, envelope); err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 2; want++ {
		select {
		case got := <-attempts:
			if got != want {
				t.Fatalf("attempt=%d want=%d", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for attempt %d: %v", want, ctx.Err())
		}
	}
	client := newTestClient(t, address)
	waitFor(t, ctx, func() bool {
		entries, rangeErr := client.Do(ctx, client.B().Xrange().Key(name+config.DeadLetterSuffix).Start("-").End("+").Build()).AsXRange()
		return rangeErr == nil && len(entries) == 1 && entries[0].FieldValues["reason"] == "rejected" && !strings.Contains(entries[0].FieldValues["reason"], "secret")
	})
}

func TestMalformedEntryIsDeadLetteredAndAcknowledged(t *testing.T) {
	address := requireValkeyAddress(t)
	config := DefaultConfig(address)
	config.Block = 20 * time.Millisecond
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	name := "malformed-" + time.Now().Format("150405.000000000")
	sub, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "0"}, func(context.Context, messaging.Delivery) messaging.HandleResult {
		t.Error("malformed entry reached handler")
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close(context.Background()) })
	<-sub.Ready()
	client := newTestClient(t, address)
	if err := client.Do(ctx, client.B().Xadd().Key(name).Id("*").FieldValue().FieldValue(envelopeField, "not-json").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool {
		length, lengthErr := client.Do(ctx, client.B().Xlen().Key(name+config.DeadLetterSuffix).Build()).AsInt64()
		return lengthErr == nil && length == 1
	})
}

func TestPendingOwnershipRecoversToAnotherConsumer(t *testing.T) {
	address := requireValkeyAddress(t)
	config := DefaultConfig(address)
	config.ClaimIdle = 30 * time.Millisecond
	config.ClaimInterval = 10 * time.Millisecond
	config.Block = 20 * time.Millisecond
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	name := "ownership-" + time.Now().Format("150405.000000000")
	firstAttempt := make(chan struct{}, 1)
	first, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "0"}, func(context.Context, messaging.Delivery) messaging.HandleResult {
		firstAttempt <- struct{}{}
		return messaging.Retry(errors.New("retry"), 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	<-first.Ready()
	if _, err := driver.Publish(ctx, messaging.Destination{Name: name}, messaging.NewEnvelope("ownership-1", "job", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstAttempt:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	recovered := make(chan int, 1)
	second, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "two", From: "0"}, func(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
		recovered <- delivery.Info().Attempt
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	<-second.Ready()
	select {
	case attempt := <-recovered:
		if attempt < 2 {
			t.Fatalf("recovered attempt=%d", attempt)
		}
	case <-ctx.Done():
		t.Fatal("pending entry was not recovered by second consumer")
	}
}

func requireValkeyAddress(t *testing.T) string {
	t.Helper()
	address := os.Getenv("VALKEY_ADDRESS")
	if address != "" {
		return address
	}
	if os.Getenv("CI") != "" {
		t.Fatal("VALKEY_ADDRESS is required in CI")
	}
	t.Skip("VALKEY_ADDRESS is required")
	return ""
}

func newTestClient(t *testing.T, address string) valkey.Client {
	t.Helper()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{address}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func waitFor(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
