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

func TestConfigRejectsSubMillisecondBrokerDurations(t *testing.T) {
	config := DefaultConfig("127.0.0.1:6379")
	config.Block = time.Nanosecond
	if _, err := New(config); err == nil {
		t.Fatal("expected sub-millisecond block rejection")
	}
	config = DefaultConfig("127.0.0.1:6379")
	config.ClaimIdle = time.Nanosecond
	if _, err := New(config); err == nil {
		t.Fatal("expected sub-millisecond claim idle rejection")
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
	if !delivery.attempted.Load() {
		t.Fatal("retry nack must suppress fallback handler settlement")
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

func TestMissingPendingMetadataIsNotTreatedAsFirstAttempt(t *testing.T) {
	if count, err := parseDeliveryCount("1-0", nil); err == nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestAckCountMustConfirmExactlyOneEntry(t *testing.T) {
	if err := validateAckCount("1-0", 1); err != nil {
		t.Fatal(err)
	}
	for _, count := range []int64{0, 2} {
		err := validateAckCount("1-0", count)
		if !errors.Is(err, messaging.ErrAcknowledgement) {
			t.Fatalf("count=%d err=%v", count, err)
		}
		structured := messaging.AsGoError(err)
		if structured.TextCode != messaging.TextCodeAcknowledgementFailed || structured.Metadata["transport"] != "valkey.streams" || structured.Metadata["operation"] != "xack" {
			t.Fatalf("count=%d structured=%#v", count, structured)
		}
		if retryable := messaging.AsRetryableError(err); retryable == nil || !retryable.IsRetryable() {
			t.Fatalf("count=%d retryable=%#v", count, retryable)
		}
	}
}

func TestAckRejectsAlreadySettledEntry(t *testing.T) {
	address := requireValkeyAddress(t)
	driver, err := New(DefaultConfig(address))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
	stream := "ack-count-" + time.Now().Format("150405.000000000")
	group := "workers"
	if createErr := driver.client.Do(ctx, driver.client.B().XgroupCreate().Key(stream).Group(group).Id("0").Mkstream().Build()).Error(); createErr != nil {
		t.Fatal(createErr)
	}
	id, err := driver.client.Do(ctx, driver.client.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue(envelopeField, "value").Build()).ToString()
	if err != nil {
		t.Fatal(err)
	}
	if readErr := driver.client.Do(ctx, driver.client.B().Xreadgroup().Group(group, "one").Count(1).Streams().Key(stream).Id(">").Build()).Error(); readErr != nil {
		t.Fatal(readErr)
	}
	sub := &subscription{client: driver.client, source: messaging.Source{Name: stream, Group: group}}
	if ackErr := sub.ack(ctx, id); ackErr != nil {
		t.Fatal(ackErr)
	}
	if ackErr := sub.ack(ctx, id); !errors.Is(ackErr, messaging.ErrAcknowledgement) {
		t.Fatalf("second ack err=%v", ackErr)
	}
}

type settlementStub struct {
	ackErr          error
	ackCalls        int
	deadLetterCalls int
}

func (s *settlementStub) ack(context.Context, string) error {
	s.ackCalls++
	return s.ackErr
}

func (s *settlementStub) deadLetter(context.Context, string, string, error) error {
	s.deadLetterCalls++
	return nil
}

func TestDeadLetterAckFailureCannotFallThroughToComplete(t *testing.T) {
	driver, err := New(DefaultConfig("127.0.0.1:6379"))
	if err != nil {
		t.Fatal(err)
	}
	driver.config.MaxDeliveries = 2
	sub := &subscription{driver: driver, errors: make(chan error, 1)}
	store := &settlementStub{ackErr: errors.New("ack failed")}
	delivery := &streamDelivery{
		BasicDelivery: messaging.NewDelivery(messaging.Envelope{}, messaging.DeliveryInfo{Attempt: 2}),
		subscription:  sub,
		settlement:    store,
	}
	err = delivery.Nack(context.Background(), messaging.NackOptions{Disposition: messaging.DispositionRetry})
	if err == nil || !delivery.attempted.Load() || delivery.settled.Load() {
		t.Fatalf("err=%v attempted=%v settled=%v", err, delivery.attempted.Load(), delivery.settled.Load())
	}
	if store.deadLetterCalls != 1 || store.ackCalls != 1 {
		t.Fatalf("dead-letter calls=%d ack calls=%d", store.deadLetterCalls, store.ackCalls)
	}
	// handleEntry checks attempted, not settled, so a handler returning Complete
	// cannot acknowledge the original entry after this failure.
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
	if _, publishErr := driver.Publish(ctx, messaging.Destination{Name: name}, envelope); publishErr != nil {
		t.Fatal(publishErr)
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
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
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
	cleanupSubscription(t, sub)
	<-sub.Ready()
	envelope := messaging.NewEnvelope("manual-retry-1", "job", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
	if _, publishErr := driver.Publish(ctx, messaging.Destination{Name: name}, envelope); publishErr != nil {
		t.Fatal(publishErr)
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
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
	name := "malformed-" + time.Now().Format("150405.000000000")
	sub, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "0"}, func(context.Context, messaging.Delivery) messaging.HandleResult {
		t.Error("malformed entry reached handler")
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, sub)
	<-sub.Ready()
	client := newTestClient(t, address)
	if addErr := client.Do(ctx, client.B().Xadd().Key(name).Id("*").FieldValue().FieldValue(envelopeField, "not-json").Build()).Error(); addErr != nil {
		t.Fatal(addErr)
	}
	waitFor(t, ctx, func() bool {
		length, lengthErr := client.Do(ctx, client.B().Xlen().Key(name+config.DeadLetterSuffix).Build()).AsInt64()
		return lengthErr == nil && length == 1
	})
	oversized := strings.Repeat("x", config.Valkey.MaxMessageBytes+1)
	if addErr := client.Do(ctx, client.B().Xadd().Key(name).Id("*").FieldValue().FieldValue(envelopeField, oversized).Build()).Error(); addErr != nil {
		t.Fatal(addErr)
	}
	waitFor(t, ctx, func() bool {
		length, lengthErr := client.Do(ctx, client.B().Xlen().Key(name+config.DeadLetterSuffix).Build()).AsInt64()
		return lengthErr == nil && length == 2
	})
}

func TestInvalidIngressHandlerCannotConsumeDurableEntry(t *testing.T) {
	address := requireValkeyAddress(t)
	config := DefaultConfig(address)
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
	name := "invalid-ingress-" + time.Now().Format("150405.000000000")
	envelope := messaging.NewEnvelope("invalid-ingress-1", "job", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
	if _, publishErr := driver.Publish(ctx, messaging.Destination{Name: name}, envelope); publishErr != nil {
		t.Fatal(publishErr)
	}
	registry, err := messaging.NewDriverRegistry(map[string]messaging.Driver{"streams": driver})
	if err != nil {
		t.Fatal(err)
	}
	_, err = messaging.NewIngress(registry, []messaging.IngressBinding{{
		Name: "invalid", LogicalRoute: "jobs", Driver: "streams",
		Source:   messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "0"},
		Handlers: []messaging.Handler{nil},
	}})
	if !errors.Is(err, messaging.ErrUnknownRoute) {
		t.Fatalf("NewIngress() error = %v", err)
	}
	client := newTestClient(t, address)
	length, err := client.Do(ctx, client.B().Xlen().Key(name).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	if length != 1 {
		t.Fatalf("stream length = %d, want durable entry untouched", length)
	}
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
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
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
	if _, publishErr := driver.Publish(ctx, messaging.Destination{Name: name}, messaging.NewEnvelope("ownership-1", "job", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)); publishErr != nil {
		t.Fatal(publishErr)
	}
	select {
	case <-firstAttempt:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if closeErr := first.Close(ctx); closeErr != nil {
		t.Fatal(closeErr)
	}
	recovered := make(chan int, 1)
	second, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "two", From: "0"}, func(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
		recovered <- delivery.Info().Attempt
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, second)
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

func TestStreamsRecoverAfterServerDisconnect(t *testing.T) {
	address := requireValkeyAddress(t)
	config := DefaultConfig(address)
	config.Block = 20 * time.Millisecond
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
	name := "stream-reconnect-" + time.Now().Format("150405.000000000")
	delivered := make(chan struct{}, 1)
	sub, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "0"}, func(context.Context, messaging.Delivery) messaging.HandleResult {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, sub)
	<-sub.Ready()
	client := newTestClient(t, address)
	if killErr := client.Do(ctx, client.B().ClientKill().TypeNormal().SkipmeYes().Build()).Error(); killErr != nil {
		t.Fatal(killErr)
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-delivered:
			return
		case <-ticker.C:
			envelope := messaging.NewEnvelope("stream-reconnect", "event", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
			if _, publishErr := driver.Publish(ctx, messaging.Destination{Name: name}, envelope); publishErr != nil && ctx.Err() != nil {
				t.Fatal(publishErr)
			}
		case <-ctx.Done():
			t.Fatal("streams driver did not recover after server disconnect")
		}
	}
}

func TestStreamStartPositionSkipsExistingEntries(t *testing.T) {
	address := requireValkeyAddress(t)
	config := DefaultConfig(address)
	config.Block = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	name := "start-position-" + time.Now().Format("150405.000000000")
	client := newTestClient(t, address)
	oldEnvelope := messaging.NewEnvelope("old", "event", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
	oldWire, err := config.Codec.Encode(ctx, oldEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	if addErr := client.Do(ctx, client.B().Xadd().Key(name).Id("*").FieldValue().FieldValue(envelopeField, string(oldWire)).Build()).Error(); addErr != nil {
		t.Fatal(addErr)
	}
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
	delivered := make(chan string, 1)
	sub, err := driver.Subscribe(ctx, messaging.Source{Name: name, Group: "workers", Consumer: "one", From: "$"}, func(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
		delivered <- delivery.Envelope().ID
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, sub)
	<-sub.Ready()
	if _, publishErr := driver.Publish(ctx, messaging.Destination{Name: name}, messaging.NewEnvelope("new", "event", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)); publishErr != nil {
		t.Fatal(publishErr)
	}
	select {
	case id := <-delivered:
		if id != "new" {
			t.Fatalf("delivered existing entry %q", id)
		}
	case <-ctx.Done():
		t.Fatal("new entry was not delivered")
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

func cleanupDriver(t *testing.T, driver *Driver) {
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
