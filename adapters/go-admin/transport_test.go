package goadmin

import (
	"context"
	"errors"
	"testing"
	"time"

	coreadmin "github.com/goliatone/go-admin/admin"
	"github.com/goliatone/go-admin/admin/commandruntest"
	admin "github.com/goliatone/go-admin/pkg/admin"
	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/internal/testkit"
)

const (
	testDriverName = "memory"
	testRouteName  = "command-runs"
	testChannel    = "go-admin.app.test.command-runs"
)

func TestTransportContract(t *testing.T) {
	commandruntest.RunTransportContract(t, func(tb testing.TB) coreadmin.CommandRunTransport {
		transport, _ := newMemoryTransport(tb, true, true)
		return transport
	})
}

func TestTransportReportsSafeHandlerAndDecodeFailures(t *testing.T) {
	transport, driver := newMemoryTransport(t, true, true)
	subscription, err := transport.SubscribeCommandRuns(context.Background(), admin.CommandRunSelector{Global: true}, func(context.Context, admin.CommandRunUpdate) error {
		return errors.New("secret provider or payload detail")
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer subscription.Close(context.Background())
	awaitReady(t, subscription)
	if err := transport.PublishCommandRun(context.Background(), validUpdate("handler-error", 1)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := awaitError(t, subscription); !errors.Is(got, admin.ErrCommandRunHandlerFailed) || stringsContain(got.Error(), "secret") {
		t.Fatalf("handler error = %v", got)
	}

	envelope, err := transport.codec.Encode(validUpdate("bad-envelope", 1))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	envelope.Headers[HeaderApplicationID] = "other"
	if _, err := driver.Publish(context.Background(), messaging.Destination{Name: testChannel}, envelope); err != nil {
		t.Fatalf("direct publish: %v", err)
	}
	if got := awaitError(t, subscription); !errors.Is(got, ErrIdentityMismatch) {
		t.Fatalf("decode error = %v, want identity mismatch", got)
	}
}

func TestSubscriptionCloseDoesNotCloseHostDriver(t *testing.T) {
	transport, driver := newMemoryTransport(t, true, true)
	subscription, err := transport.SubscribeCommandRuns(context.Background(), admin.CommandRunSelector{Global: true}, func(context.Context, admin.CommandRunUpdate) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	awaitReady(t, subscription)
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-driver.Errors():
		t.Fatal("subscription close closed the host-owned driver")
	default:
	}
	second, err := transport.SubscribeCommandRuns(context.Background(), admin.CommandRunSelector{Global: true}, func(context.Context, admin.CommandRunUpdate) error { return nil })
	if err != nil {
		t.Fatalf("subscribe after close: %v", err)
	}
	awaitReady(t, second)
	_ = second.Close(context.Background())
}

func TestSubscriptionContextCancellationClosesDeliveryAndCancelsHandler(t *testing.T) {
	transport, _ := newMemoryTransport(t, true, true)
	subscriptionCtx, cancelSubscription := context.WithCancel(context.Background())
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	subscription, err := transport.SubscribeCommandRuns(subscriptionCtx, admin.CommandRunSelector{Global: true}, func(ctx context.Context, _ admin.CommandRunUpdate) error {
		close(handlerStarted)
		<-ctx.Done()
		close(handlerCanceled)
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	awaitReady(t, subscription)
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- transport.PublishCommandRun(context.Background(), validUpdate("cancel-handler", 1))
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancelSubscription()
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("subscription cancellation did not cancel handler")
	}
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish did not finish")
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatalf("close after cancellation: %v", err)
	}
}

func TestDirectionalWiring(t *testing.T) {
	publisher, _ := newMemoryTransport(t, true, false)
	if _, err := publisher.SubscribeCommandRuns(context.Background(), admin.CommandRunSelector{Global: true}, func(context.Context, admin.CommandRunUpdate) error { return nil }); !errors.Is(err, ErrSubscriberDisabled) {
		t.Fatalf("publisher-only subscribe error = %v", err)
	}

	subscriber, _ := newMemoryTransport(t, false, true)
	if err := subscriber.PublishCommandRun(context.Background(), validUpdate("disabled", 1)); !errors.Is(err, ErrPublisherDisabled) {
		t.Fatalf("subscriber-only publish error = %v", err)
	}
}

func TestSubscriptionErrorClassification(t *testing.T) {
	for name, test := range map[string]struct {
		err  error
		want error
	}{
		"lifecycle": {err: errors.New("provider detail"), want: ErrSubscriptionFailed},
		"unknown message": {
			err: messaging.NewMessageError(errors.New("provider payload detail")), want: ErrDeliveryFailed,
		},
		"adapter echo": {
			err: messaging.NewMessageError(ErrScopeRejected), want: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := classifySubscriptionError(test.err)
			if !errors.Is(got, test.want) || (test.want == nil && got != nil) {
				t.Fatalf("classification = %v, want %v", got, test.want)
			}
		})
	}
}

func TestAdapterErrorsMapToGoAdminDiagnosticCategories(t *testing.T) {
	for name, test := range map[string]struct {
		err  error
		want error
	}{
		"publish":      {ErrPublishFailed, admin.ErrCommandRunPublishFailed},
		"subscription": {ErrSubscriptionFailed, admin.ErrCommandRunSubscriptionFailed},
		"envelope":     {ErrEnvelopeRejected, admin.ErrCommandRunEnvelopeRejected},
		"identity":     {ErrIdentityMismatch, admin.ErrCommandRunScopeRejected},
		"scope":        {ErrScopeRejected, admin.ErrCommandRunScopeRejected},
		"delivery":     {ErrDeliveryFailed, admin.ErrCommandRunDeliveryDropped},
	} {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(test.err, test.want) {
				t.Fatalf("error %v does not map to %v", test.err, test.want)
			}
		})
	}
}

func newMemoryTransport(tb testing.TB, publisher, subscriber bool) (*Transport, *testkit.MemoryDriver) {
	tb.Helper()
	driver := testkit.NewMemoryDriver()
	if err := driver.Start(context.Background()); err != nil {
		tb.Fatalf("start memory driver: %v", err)
	}
	tb.Cleanup(func() { _ = driver.Close(context.Background()) })
	drivers, err := messaging.NewDriverRegistry(map[string]messaging.Driver{testDriverName: driver})
	if err != nil {
		tb.Fatalf("driver registry: %v", err)
	}
	config := TransportConfig{Drivers: drivers, Codec: requireCodec(tb)}
	if publisher {
		config.PublishRoute = testRouteName
		config.Router, err = messaging.NewRouter(drivers, []messaging.Route{{
			Name: testRouteName, Strategy: messaging.StrategyPrimary,
			Bindings: []messaging.RouteBinding{{Driver: testDriverName, Destination: messaging.Destination{Name: testChannel}}},
			Kinds:    []messaging.Kind{messaging.KindEvent}, Types: []string{MessageType},
		}}, nil)
		if err != nil {
			tb.Fatalf("router: %v", err)
		}
	}
	if subscriber {
		config.Sources = []SourceBinding{{
			Name: "command-runs", LogicalRoute: testRouteName, Driver: testDriverName,
			Source: messaging.Source{Name: testChannel},
		}}
	}
	transport, err := NewTransport(config)
	if err != nil {
		tb.Fatalf("new transport: %v", err)
	}
	return transport, driver
}

func awaitReady(t testing.TB, subscription admin.CommandRunSubscription) {
	t.Helper()
	select {
	case <-subscription.Ready():
	case err := <-subscription.Errors():
		t.Fatalf("failed before ready: %v", err)
	case <-time.After(time.Second):
		t.Fatal("subscription did not become ready")
	}
}

func awaitError(t testing.TB, subscription admin.CommandRunSubscription) error {
	t.Helper()
	select {
	case err := <-subscription.Errors():
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription error")
		return nil
	}
}

func stringsContain(value, fragment string) bool {
	for index := 0; index+len(fragment) <= len(value); index++ {
		if value[index:index+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
