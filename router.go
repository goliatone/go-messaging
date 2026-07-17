package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type routeSnapshot struct{ routes map[string]Route }

type RoutingResult struct {
	Route    string
	Results  []PublishResult
	Mirrored int
}

// Router resolves logical routes against an immutable configuration generation.
type Router struct {
	drivers  *DriverRegistry
	routes   atomic.Pointer[routeSnapshot]
	observer Observer
}

func NewRouter(drivers *DriverRegistry, routes []Route, observer Observer) (*Router, error) {
	if drivers == nil {
		return nil, fmt.Errorf("%w: driver registry is required", ErrUnknownDriver)
	}
	r := &Router{drivers: drivers, observer: protectObserver(observer)}
	if err := r.ReplaceRoutes(routes); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Router) ReplaceRoutes(routes []Route) error {
	next := make(map[string]Route, len(routes))
	for _, route := range routes {
		if _, exists := next[route.Name]; exists {
			return fmt.Errorf("%w: duplicate route %q", ErrUnknownRoute, route.Name)
		}
		if err := route.validate(r.drivers); err != nil {
			return err
		}
		next[route.Name] = route.clone()
	}
	r.routes.Store(&routeSnapshot{routes: next})
	return nil
}

func (r *Router) Route(name string) (Route, bool) {
	snapshot := r.routes.Load()
	if snapshot == nil {
		return Route{}, false
	}
	route, ok := snapshot.routes[name]
	return route.clone(), ok
}

func (r *Router) Publish(ctx context.Context, logicalRoute string, envelope Envelope) (RoutingResult, error) {
	route, err := r.routeForPublish(logicalRoute, envelope)
	if err != nil {
		return RoutingResult{}, err
	}
	if route.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, route.Timeout)
		defer cancel()
	}
	return r.publishRoute(ctx, route, envelope)
}

func (r *Router) routeForPublish(logicalRoute string, envelope Envelope) (Route, error) {
	snapshot := r.routes.Load()
	if snapshot == nil {
		return Route{}, fmt.Errorf("%w: %s", ErrUnknownRoute, logicalRoute)
	}
	route, ok := snapshot.routes[logicalRoute]
	if !ok {
		return Route{}, fmt.Errorf("%w: %s", ErrUnknownRoute, logicalRoute)
	}
	if err := envelope.Validate(); err != nil {
		return Route{}, err
	}
	if !route.matches(envelope) {
		return Route{}, fmt.Errorf("%w: envelope does not match route %q", ErrUnknownRoute, logicalRoute)
	}
	if route.MaxMessageBytes > 0 && len(envelope.Payload) > route.MaxMessageBytes {
		return Route{}, ErrMessageTooLarge
	}
	if route.Strategy == StrategyFanout && envelope.Kind == KindCommand && (!route.AllowCommandFanout || route.IdempotencyPolicy == "") {
		return Route{}, fmt.Errorf("%w: command fanout", ErrUnsupportedCapability)
	}
	return route, nil
}

func (r *Router) publishRoute(ctx context.Context, route Route, envelope Envelope) (RoutingResult, error) {
	switch route.Strategy {
	case StrategyPrimary:
		return r.publishPrimary(ctx, route, envelope)
	case StrategyFailover:
		return r.publishFailover(ctx, route, envelope)
	case StrategyFanout:
		return r.publishFanout(ctx, route, envelope)
	case StrategyMirror:
		return r.publishMirror(ctx, route, envelope)
	default:
		return RoutingResult{Route: route.Name}, ErrUnknownRoute
	}
}

func (r *Router) publishPrimary(ctx context.Context, route Route, envelope Envelope) (RoutingResult, error) {
	result := RoutingResult{Route: route.Name}
	published, err := r.publishBinding(ctx, route, route.Bindings[0], envelope)
	result.Results = append(result.Results, published)
	return result, err
}

func (r *Router) publishFailover(ctx context.Context, route Route, envelope Envelope) (RoutingResult, error) {
	result := RoutingResult{Route: route.Name}
	for _, binding := range route.Bindings {
		published, err := r.publishBinding(ctx, route, binding, envelope)
		result.Results = append(result.Results, published)
		if err == nil && published.Outcome == PublishAccepted {
			return result, nil
		}
		outcome := classifiedOutcome(published.Outcome, err)
		ambiguous := outcome == PublishAmbiguous
		if ambiguous && (!route.AllowAmbiguousFailover || route.IdempotencyPolicy == "") {
			return result, errOr(err, ErrPublishAmbiguous)
		}
		if outcome != PublishRejected && outcome != PublishDefinitelyNotPublished && !ambiguous {
			return result, errOr(err, ErrNotPublished)
		}
	}
	return result, ErrNotPublished
}

func (r *Router) publishFanout(ctx context.Context, route Route, envelope Envelope) (RoutingResult, error) {
	result := RoutingResult{Route: route.Name}
	var joined error
	for _, binding := range route.Bindings {
		published, err := r.publishBinding(ctx, route, binding, envelope)
		result.Results = append(result.Results, published)
		joined = errors.Join(joined, err)
	}
	return result, joined
}

func (r *Router) publishMirror(ctx context.Context, route Route, envelope Envelope) (RoutingResult, error) {
	result := RoutingResult{Route: route.Name}
	primary, err := r.publishBinding(ctx, route, route.Bindings[0], envelope)
	result.Results = append(result.Results, primary)
	if err != nil || primary.Outcome != PublishAccepted {
		return result, errOr(err, ErrNotPublished)
	}
	for _, binding := range route.Bindings[1:] {
		published, mirrorErr := r.publishBinding(ctx, route, binding, envelope)
		result.Results = append(result.Results, published)
		if mirrorErr == nil && published.Outcome == PublishAccepted {
			result.Mirrored++
		}
	}
	return result, nil
}

func (r *Router) publishBinding(ctx context.Context, route Route, binding RouteBinding, envelope Envelope) (PublishResult, error) {
	driver, ok := r.drivers.Lookup(binding.Driver)
	if !ok {
		return PublishResult{Outcome: PublishDefinitelyNotPublished}, fmt.Errorf("%w: %s", ErrUnknownDriver, binding.Driver)
	}
	publisher, ok := driver.(PublishDriver)
	if !ok {
		return PublishResult{Outcome: PublishDefinitelyNotPublished}, fmt.Errorf("%w: driver %s cannot publish", ErrUnsupportedCapability, binding.Driver)
	}
	started := time.Now()
	result, err := publisher.Publish(ctx, binding.Destination, envelope.Clone())
	if err == nil {
		err = result.OutcomeError()
	}
	operation := OperationPublish
	if envelope.Kind == KindReply {
		operation = OperationReply
	}
	r.observer.Observe(ctx, Observation{Operation: operation, LogicalRoute: route.Name, Kind: envelope.Kind, MessageType: envelope.Type, Transport: binding.Driver, Destination: binding.Destination.Name, CorrelationID: envelope.CorrelationID, Outcome: string(result.Outcome), Latency: time.Since(started), Err: safeObservationError(err)})
	return result.Clone(), err
}

func errOr(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

func classifiedOutcome(outcome PublishOutcome, err error) PublishOutcome {
	if outcome != "" {
		return outcome
	}
	switch {
	case errors.Is(err, ErrPublishAmbiguous):
		return PublishAmbiguous
	case errors.Is(err, ErrPublishRejected):
		return PublishRejected
	case errors.Is(err, ErrNotPublished), errors.Is(err, ErrUnknownDriver), errors.Is(err, ErrUnsupportedCapability):
		return PublishDefinitelyNotPublished
	default:
		return outcome
	}
}
