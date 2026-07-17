package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type publishStub struct {
	stubDriver
	mu      sync.Mutex
	outcome PublishOutcome
	calls   int
}

func (d *publishStub) Publish(_ context.Context, destination Destination, envelope Envelope) (PublishResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.outcome == "" {
		d.outcome = PublishAccepted
	}
	result := PublishResult{Outcome: d.outcome, Destination: destination.Name}
	switch d.outcome {
	case PublishAmbiguous:
		return result, ErrPublishAmbiguous
	case PublishRejected:
		return result, ErrPublishRejected
	}
	return result, nil
}

func newTestRouter(t *testing.T, drivers map[string]Driver, routes []Route) *Router {
	t.Helper()
	registry, err := NewDriverRegistry(drivers)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(registry, routes, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRouterPrimaryAndSnapshotIsolation(t *testing.T) {
	driver := &publishStub{}
	route := Route{Name: "events", Strategy: StrategyPrimary, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "events"}}}, Kinds: []Kind{KindEvent}}
	r := newTestRouter(t, map[string]Driver{"one": driver}, []Route{route})
	copy, _ := r.Route("events")
	copy.Bindings[0].Destination.Name = "changed"
	result, err := r.Publish(context.Background(), "events", validEnvelope())
	if err != nil || result.Results[0].Destination != "events" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestRouterFailoverStopsOnAmbiguous(t *testing.T) {
	first := &publishStub{outcome: PublishAmbiguous}
	second := &publishStub{}
	route := Route{Name: "jobs", Strategy: StrategyFailover, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "a"}}, {Driver: "two", Destination: Destination{Name: "b"}}}}
	r := newTestRouter(t, map[string]Driver{"one": first, "two": second}, []Route{route})
	_, err := r.Publish(context.Background(), "jobs", validEnvelope())
	if !errors.Is(err, ErrPublishAmbiguous) || second.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, second.calls)
	}
}

func TestRouterRejectsUnsafeCommandFanout(t *testing.T) {
	driver := &publishStub{}
	registry, _ := NewDriverRegistry(map[string]Driver{"one": driver})
	_, err := NewRouter(registry, []Route{{Name: "commands", Strategy: StrategyFanout, Kinds: []Kind{KindCommand}, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "a"}}}}}, nil)
	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}

func TestFailedRouteReplacementPreservesGeneration(t *testing.T) {
	driver := &publishStub{}
	r := newTestRouter(t, map[string]Driver{"one": driver}, []Route{{Name: "events", Strategy: StrategyPrimary, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "a"}}}}})
	if err := r.ReplaceRoutes([]Route{{Name: "bad"}}); err == nil {
		t.Fatal("expected validation error")
	}
	if _, ok := r.Route("events"); !ok {
		t.Fatal("previous snapshot was lost")
	}
}

func TestDriverReplacementCannotPanicRouter(t *testing.T) {
	driver := &publishStub{}
	registry, _ := NewDriverRegistry(map[string]Driver{"one": driver})
	r, _ := NewRouter(registry, []Route{{Name: "events", Strategy: StrategyPrimary, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "a"}}}}}, nil)
	if err := registry.Replace(map[string]Driver{"one": &stubDriver{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(context.Background(), "events", validEnvelope()); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}
