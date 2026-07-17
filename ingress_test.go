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
