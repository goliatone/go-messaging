package chatdemo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

type testDriver struct {
	mu         sync.Mutex
	ready      chan struct{}
	errors     chan error
	published  messaging.Envelope
	source     messaging.Source
	handler    messaging.Handler
	outcome    messaging.PublishOutcome
	publishErr error
	startErr   error
}

func newTestDriver() *testDriver {
	return &testDriver{ready: make(chan struct{}), errors: make(chan error)}
}

func (*testDriver) Capabilities() messaging.Capabilities { return messaging.Capabilities{Fanout: true} }
func (d *testDriver) Start(context.Context) error {
	if d.startErr != nil {
		return d.startErr
	}
	close(d.ready)
	return nil
}
func (d *testDriver) Ready() <-chan struct{}    { return d.ready }
func (d *testDriver) Errors() <-chan error      { return d.errors }
func (*testDriver) Close(context.Context) error { return nil }
func (d *testDriver) Publish(_ context.Context, destination messaging.Destination, envelope messaging.Envelope) (messaging.PublishResult, error) {
	d.mu.Lock()
	d.published = envelope.Clone()
	d.mu.Unlock()
	outcome := d.outcome
	if outcome == "" {
		outcome = messaging.PublishAccepted
	}
	return messaging.PublishResult{Outcome: outcome, Transport: "test", Destination: destination.Name}, d.publishErr
}
func (d *testDriver) Subscribe(_ context.Context, source messaging.Source, handler messaging.Handler) (messaging.Subscription, error) {
	d.mu.Lock()
	d.source, d.handler = source, handler
	d.mu.Unlock()
	return readySubscription(), nil
}
func (d *testDriver) deliver(envelope messaging.Envelope) messaging.HandleResult {
	d.mu.Lock()
	handler := d.handler
	d.mu.Unlock()
	return handler(context.Background(), messaging.NewDelivery(envelope, messaging.DeliveryInfo{}))
}

func TestBrokerStartSanitizesProviderFailure(t *testing.T) {
	cause := errors.New("NOAUTH invalid password from 10.0.0.8:6379")
	driver := newTestDriver()
	driver.startErr = cause
	broker, err := newBroker(driver, "provider-channel", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.Start(context.Background())
	if err == nil || err.Error() != messagingUnavailableMessage {
		t.Fatalf("start error = %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("sanitized start error did not preserve its cause")
	}
}

type testSubscription struct{ ready chan struct{} }

func readySubscription() *testSubscription {
	ready := make(chan struct{})
	close(ready)
	return &testSubscription{ready: ready}
}
func (s *testSubscription) Ready() <-chan struct{}    { return s.ready }
func (*testSubscription) Errors() <-chan error        { return make(chan error) }
func (*testSubscription) Close(context.Context) error { return nil }

func TestDefaultBrokerConfigUsesDemoValkeyPort(t *testing.T) {
	if got := DefaultBrokerConfig().ValkeyAddress; got != "127.0.0.1:6399" {
		t.Fatalf("default Valkey address %q, want %q", got, "127.0.0.1:6399")
	}
}

func TestBrokerPublishesContractThroughLogicalRoute(t *testing.T) {
	driver := newTestDriver()
	broker, err := newBroker(driver, "provider-channel", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	envelope, result, err := broker.Publish(context.Background(), ChatMessage{Sender: "alice", Text: "hello", Client: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Route != ChatRoute || len(result.Results) != 1 || result.Results[0].Destination != "provider-channel" {
		t.Fatalf("unexpected routing result %#v", result)
	}
	if envelope.Type != ChatEventType || envelope.Kind != messaging.KindEvent || envelope.SchemaVersion != ChatSchema || envelope.ContentType != ChatContentType {
		t.Fatalf("unexpected envelope %#v", envelope)
	}
	driver.mu.Lock()
	published := driver.published
	driver.mu.Unlock()
	if published.ID != envelope.ID {
		t.Fatalf("published ID %q, want %q", published.ID, envelope.ID)
	}
}

func TestBrokerIngressFiltersContractBeforeHandler(t *testing.T) {
	driver := newTestDriver()
	broker, err := newBroker(driver, "provider-channel", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	ingress, err := broker.NewIngress(func(context.Context, messaging.Delivery) messaging.HandleResult {
		called++
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingress.Subscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	wrong := messaging.NewEnvelope("wrong", "other.type", messaging.KindEvent, ChatSchema, ChatContentType, []byte(`{}`), nil)
	if result := driver.deliver(wrong); result.Disposition != messaging.DispositionReject || !errors.Is(result.Err, messaging.ErrInvalidEnvelope) {
		t.Fatalf("unexpected rejected result %#v", result)
	}
	if called != 0 {
		t.Fatalf("handler called %d times for rejected envelope", called)
	}
	valid := messaging.NewEnvelope("valid", ChatEventType, messaging.KindEvent, ChatSchema, ChatContentType, []byte(`{"sender":"alice","text":"hello"}`), nil)
	if result := driver.deliver(valid); result.Disposition != messaging.DispositionComplete || called != 1 {
		t.Fatalf("valid delivery result %#v, called %d", result, called)
	}
}
