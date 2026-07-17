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

type publishResultStub struct {
	stubDriver
	result PublishResult
	err    error
	calls  int
}

func (d *publishResultStub) Publish(_ context.Context, destination Destination, _ Envelope) (PublishResult, error) {
	d.calls++
	result := d.result.Clone()
	if result.Destination == "" {
		result.Destination = destination.Name
	}
	return result, d.err
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
	registry, registryErr := NewDriverRegistry(map[string]Driver{"one": driver})
	if registryErr != nil {
		t.Fatal(registryErr)
	}
	_, err := NewRouter(registry, []Route{{Name: "commands", Strategy: StrategyFanout, Kinds: []Kind{KindCommand}, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "a"}}}}}, nil)
	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}

func TestRouterRejectsUnusableFiltersAtStartup(t *testing.T) {
	driver := &publishStub{}
	registry, err := NewDriverRegistry(map[string]Driver{"one": driver})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		kinds []Kind
		types []string
	}{
		{name: "invalid kind", kinds: []Kind{Kind("invalid")}},
		{name: "duplicate kind", kinds: []Kind{KindEvent, KindEvent}},
		{name: "empty type", types: []string{""}},
		{name: "whitespace type", types: []string{"  "}},
		{name: "non canonical type", types: []string{" event.created "}},
		{name: "duplicate type", types: []string{"event.created", "event.created"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, routeErr := NewRouter(registry, []Route{{
				Name: "invalid", Strategy: StrategyPrimary, Kinds: test.kinds, Types: test.types,
				Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "events"}}},
			}}, nil)
			if !errors.Is(routeErr, ErrInvalidEnvelope) {
				t.Fatalf("got %v", routeErr)
			}
		})
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
	registry, err := NewDriverRegistry(map[string]Driver{"one": driver})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(registry, []Route{{Name: "events", Strategy: StrategyPrimary, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "a"}}}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Replace(map[string]Driver{"one": &stubDriver{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(context.Background(), "events", validEnvelope()); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}

func TestRouterClassifiesReplyObservation(t *testing.T) {
	driver := &publishStub{}
	registry, err := NewDriverRegistry(map[string]Driver{"one": driver})
	if err != nil {
		t.Fatal(err)
	}
	var observation Observation
	r, err := NewRouter(registry, []Route{{Name: "replies", Strategy: StrategyPrimary, Kinds: []Kind{KindReply}, Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "replies"}}}}}, ObserverFunc(func(_ context.Context, got Observation) {
		observation = got
	}))
	if err != nil {
		t.Fatal(err)
	}
	reply := validEnvelope()
	reply.Kind = KindReply
	reply.CorrelationID = "corr"
	if _, err := r.Publish(context.Background(), "replies", reply); err != nil {
		t.Fatal(err)
	}
	if observation.Operation != OperationReply || observation.LogicalRoute != "replies" || observation.Transport != "one" || observation.Destination != "replies" || observation.CorrelationID != "corr" {
		t.Fatalf("observation = %#v", observation)
	}
}

func TestRouterContainsObserverPanicAfterAcceptedPublish(t *testing.T) {
	driver := &publishStub{}
	registry, err := NewDriverRegistry(map[string]Driver{"one": driver})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(registry, []Route{{
		Name: "events", Strategy: StrategyPrimary,
		Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "events"}}},
	}}, ObserverFunc(func(context.Context, Observation) { panic("observer failed") }))
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Publish(context.Background(), "events", validEnvelope())
	if err != nil || len(result.Results) != 1 || result.Results[0].Outcome != PublishAccepted || driver.calls != 1 {
		t.Fatalf("result=%#v err=%v calls=%d", result, err, driver.calls)
	}
}

func TestPublishResultOutcomeError(t *testing.T) {
	tests := []struct {
		name    string
		outcome PublishOutcome
		want    error
	}{
		{name: "accepted", outcome: PublishAccepted},
		{name: "rejected", outcome: PublishRejected, want: ErrPublishRejected},
		{name: "definitely not published", outcome: PublishDefinitelyNotPublished, want: ErrNotPublished},
		{name: "ambiguous", outcome: PublishAmbiguous, want: ErrPublishAmbiguous},
		{name: "missing", want: ErrNotPublished},
		{name: "invalid", outcome: PublishOutcome("unexpected"), want: ErrNotPublished},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := (PublishResult{Outcome: test.outcome}).OutcomeError()
			if !errors.Is(err, test.want) || (test.want == nil && err != nil) {
				t.Fatalf("OutcomeError() = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRouterNormalizesDriverOutcomesAcrossStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy Strategy
		outcome  PublishOutcome
		want     error
	}{
		{name: "primary rejected", strategy: StrategyPrimary, outcome: PublishRejected, want: ErrPublishRejected},
		{name: "primary missing", strategy: StrategyPrimary, want: ErrNotPublished},
		{name: "primary invalid", strategy: StrategyPrimary, outcome: PublishOutcome("invalid"), want: ErrNotPublished},
		{name: "fanout rejected", strategy: StrategyFanout, outcome: PublishRejected, want: ErrPublishRejected},
		{name: "fanout missing", strategy: StrategyFanout, want: ErrNotPublished},
		{name: "mirror primary rejected", strategy: StrategyMirror, outcome: PublishRejected, want: ErrPublishRejected},
		{name: "mirror primary missing", strategy: StrategyMirror, want: ErrNotPublished},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := &publishResultStub{result: PublishResult{Outcome: test.outcome}}
			route := Route{
				Name:     "events",
				Strategy: test.strategy,
				Bindings: []RouteBinding{{Driver: "one", Destination: Destination{Name: "events"}}},
			}
			r := newTestRouter(t, map[string]Driver{"one": driver}, []Route{route})
			result, err := r.Publish(context.Background(), "events", validEnvelope())
			if !errors.Is(err, test.want) {
				t.Fatalf("Publish() error = %v, want %v", err, test.want)
			}
			if len(result.Results) != 1 || result.Results[0].Outcome != test.outcome {
				t.Fatalf("Publish() result = %#v, want original outcome %q", result, test.outcome)
			}
		})
	}
}

func TestRouterFailoverNormalizesNilDriverErrors(t *testing.T) {
	first := &publishResultStub{result: PublishResult{Outcome: PublishRejected}}
	second := &publishResultStub{result: PublishResult{Outcome: PublishAccepted}}
	route := Route{
		Name:     "events",
		Strategy: StrategyFailover,
		Bindings: []RouteBinding{
			{Driver: "one", Destination: Destination{Name: "first"}},
			{Driver: "two", Destination: Destination{Name: "second"}},
		},
	}
	r := newTestRouter(t, map[string]Driver{"one": first, "two": second}, []Route{route})
	result, err := r.Publish(context.Background(), "events", validEnvelope())
	if err != nil || first.calls != 1 || second.calls != 1 || len(result.Results) != 2 {
		t.Fatalf("result=%#v err=%v calls=(%d,%d)", result, err, first.calls, second.calls)
	}
}

func TestRouterMirrorIgnoresNormalizedMirrorFailure(t *testing.T) {
	primary := &publishResultStub{result: PublishResult{Outcome: PublishAccepted}}
	mirror := &publishResultStub{result: PublishResult{Outcome: PublishRejected}}
	route := Route{
		Name:     "events",
		Strategy: StrategyMirror,
		Bindings: []RouteBinding{
			{Driver: "primary", Destination: Destination{Name: "primary"}},
			{Driver: "mirror", Destination: Destination{Name: "mirror"}},
		},
	}
	r := newTestRouter(t, map[string]Driver{"primary": primary, "mirror": mirror}, []Route{route})
	result, err := r.Publish(context.Background(), "events", validEnvelope())
	if err != nil || result.Mirrored != 0 || len(result.Results) != 2 || result.Results[1].Outcome != PublishRejected {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}
