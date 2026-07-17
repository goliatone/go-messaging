package messaging

import (
	"context"
	"errors"
	"testing"
)

type subscriptionStub struct{ ready chan struct{} }

func (s *subscriptionStub) Ready() <-chan struct{}    { return s.ready }
func (*subscriptionStub) Errors() <-chan error        { return nil }
func (*subscriptionStub) Close(context.Context) error { return nil }

type consumeStub struct {
	stubDriver
	handler Handler
}

func (d *consumeStub) Subscribe(_ context.Context, _ Source, handler Handler) (Subscription, error) {
	d.handler = handler
	ready := make(chan struct{})
	close(ready)
	return &subscriptionStub{ready: ready}, nil
}

func TestIngressAllowsSameLogicalMessageAcrossDrivers(t *testing.T) {
	one, two := &consumeStub{}, &consumeStub{}
	registry, err := NewDriverRegistry(map[string]Driver{"one": one, "two": two})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	handler := func(context.Context, Delivery) HandleResult { calls++; return Complete() }
	ingress, err := NewIngress(registry, []IngressBinding{
		{Name: "a", LogicalRoute: "orders", Driver: "one", Source: Source{Name: "a"}, AcceptedTypes: []string{"orders.created"}, Handlers: []Handler{handler}},
		{Name: "b", LogicalRoute: "orders", Driver: "two", Source: Source{Name: "b"}, AcceptedTypes: []string{"orders.created"}, Handlers: []Handler{handler}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingress.Subscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	delivery := NewDelivery(validEnvelope(), DeliveryInfo{})
	one.handler(context.Background(), delivery)
	two.handler(context.Background(), delivery)
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestIngressRejectsUnsupportedDisposition(t *testing.T) {
	driver := &consumeStub{}
	registry, registryErr := NewDriverRegistry(map[string]Driver{"ephemeral": driver})
	if registryErr != nil {
		t.Fatal(registryErr)
	}
	_, err := NewIngress(registry, []IngressBinding{{Name: "a", LogicalRoute: "a", Driver: "ephemeral", Source: Source{Name: "a"}, Handlers: []Handler{func(context.Context, Delivery) HandleResult { return Complete() }}, RequiredDispositions: []Disposition{DispositionRetry}}})
	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}

func TestIngressRejectsInvalidWirePolicyAtStartup(t *testing.T) {
	driver := &consumeStub{}
	registry, err := NewDriverRegistry(map[string]Driver{"consumer": driver})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewIngress(registry, []IngressBinding{{
		Name: "invalid", LogicalRoute: "invalid", Driver: "consumer", Source: Source{Name: "invalid"},
		AcceptedSchemas: []string{""}, Handlers: []Handler{func(context.Context, Delivery) HandleResult { return Complete() }},
	}})
	if !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("got %v", err)
	}
}

func newObservedIngress(t *testing.T) (*consumeStub, *[]Observation) {
	t.Helper()
	driver := &consumeStub{}
	registry, err := NewDriverRegistry(map[string]Driver{"consumer": driver})
	if err != nil {
		t.Fatal(err)
	}
	var observations []Observation
	ingress, err := NewIngress(registry, []IngressBinding{{
		Name: "orders", LogicalRoute: "orders", Driver: "consumer", Source: Source{Name: "orders"},
		AcceptedKinds:        []Kind{KindEvent},
		AcceptedTypes:        []string{"orders.created"},
		AcceptedContentTypes: []string{"application/json"},
		AcceptedSchemas:      []string{"1"},
		Handlers:             []Handler{func(context.Context, Delivery) HandleResult { return Retry(context.DeadlineExceeded, 0) }},
	}}, WithIngressObserver(ObserverFunc(func(_ context.Context, observation Observation) {
		observations = append(observations, observation)
	})))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingress.Subscribe(context.Background()); err != nil {
		t.Fatal(err)
	}
	return driver, &observations
}

func TestIngressObservesHandlerOutcome(t *testing.T) {
	driver, observations := newObservedIngress(t)
	accepted := validEnvelope()
	accepted.Kind = KindEvent
	accepted.Type = "orders.created"
	result := driver.handler(context.Background(), NewDelivery(accepted, DeliveryInfo{Attempt: 3}))
	if result.Disposition != DispositionRetry {
		t.Fatalf("accepted result = %#v", result)
	}
	if len(*observations) != 1 {
		t.Fatalf("observations = %#v", *observations)
	}
	if got := (*observations)[0]; got.Operation != OperationConsume || got.LogicalRoute != "orders" || got.Transport != "consumer" || got.Destination != "orders" || got.Attempt != 3 || got.Outcome != string(DispositionRetry) || !errors.Is(got.Err, context.DeadlineExceeded) {
		t.Fatalf("accepted observation = %#v", got)
	}
}

func TestIngressRejectsSchemaOutsideWirePolicy(t *testing.T) {
	driver, observations := newObservedIngress(t)
	rejected := validEnvelope()
	rejected.Kind = KindEvent
	rejected.Type = "orders.created"
	rejected.SchemaVersion = "2"
	result := driver.handler(context.Background(), NewDelivery(rejected, DeliveryInfo{Attempt: 1}))
	if result.Disposition != DispositionReject || !errors.Is(result.Err, ErrInvalidEnvelope) {
		t.Fatalf("rejected result = %#v", result)
	}
	if len(*observations) != 1 {
		t.Fatalf("observations = %#v", *observations)
	}
	if got := (*observations)[0]; got.Outcome != string(DispositionReject) || !errors.Is(got.Err, ErrInvalidEnvelope) {
		t.Fatalf("rejected observation = %#v", got)
	}
}
