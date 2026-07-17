package messaging

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

type Strategy string

const (
	StrategyPrimary  Strategy = "primary"
	StrategyFailover Strategy = "failover"
	StrategyFanout   Strategy = "fanout"
	StrategyMirror   Strategy = "mirror"
)

type RouteBinding struct {
	Driver      string
	Destination Destination
}

type Route struct {
	Name                   string
	Strategy               Strategy
	Bindings               []RouteBinding
	Kinds                  []Kind
	Types                  []string
	Required               []Capability
	Timeout                time.Duration
	MaxMessageBytes        int
	IdempotencyPolicy      string
	AllowCommandFanout     bool
	AllowAmbiguousFailover bool
}

func (r Route) clone() Route {
	r.Bindings = append([]RouteBinding(nil), r.Bindings...)
	r.Kinds = append([]Kind(nil), r.Kinds...)
	r.Types = append([]string(nil), r.Types...)
	r.Required = append([]Capability(nil), r.Required...)
	return r
}

func (r Route) validate(registry *DriverRegistry) error {
	if err := r.validateShape(); err != nil {
		return err
	}
	if err := r.validateBindings(registry); err != nil {
		return err
	}
	return r.validateSafety()
}

func (r Route) validateShape() error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("%w: route name is required", ErrUnknownRoute)
	}
	switch r.Strategy {
	case StrategyPrimary, StrategyFailover, StrategyFanout, StrategyMirror:
	default:
		return fmt.Errorf("%w: route %q has invalid strategy %q", ErrUnknownRoute, r.Name, r.Strategy)
	}
	if len(r.Bindings) == 0 {
		return fmt.Errorf("%w: route %q has no bindings", ErrUnknownRoute, r.Name)
	}
	if r.Strategy == StrategyPrimary && len(r.Bindings) != 1 {
		return fmt.Errorf("%w: primary route %q requires exactly one binding", ErrUnknownRoute, r.Name)
	}
	owner := fmt.Sprintf("route %q", r.Name)
	if err := validateKindFilter(owner, r.Kinds); err != nil {
		return err
	}
	if err := validateStringFilter(owner, "type", r.Types); err != nil {
		return err
	}
	return nil
}

func (r Route) validateBindings(registry *DriverRegistry) error {
	for _, binding := range r.Bindings {
		if strings.TrimSpace(binding.Driver) == "" || strings.TrimSpace(binding.Destination.Name) == "" {
			return fmt.Errorf("%w: route %q has incomplete binding", ErrUnknownRoute, r.Name)
		}
		driver, err := registry.Require(binding.Driver, r.Required...)
		if err != nil {
			return fmt.Errorf("route %q: %w", r.Name, err)
		}
		if _, ok := driver.(PublishDriver); !ok {
			return fmt.Errorf("route %q driver %q: %w", r.Name, binding.Driver, ErrUnsupportedCapability)
		}
	}
	return nil
}

func (r Route) validateSafety() error {
	if r.Strategy == StrategyFanout && containsKind(r.Kinds, KindCommand) && (!r.AllowCommandFanout || strings.TrimSpace(r.IdempotencyPolicy) == "") {
		return fmt.Errorf("%w: command fanout route %q requires explicit permission and idempotency policy", ErrUnsupportedCapability, r.Name)
	}
	if r.AllowAmbiguousFailover && strings.TrimSpace(r.IdempotencyPolicy) == "" {
		return fmt.Errorf("%w: ambiguous failover route %q requires idempotency policy", ErrUnsupportedCapability, r.Name)
	}
	if r.Timeout < 0 || r.MaxMessageBytes < 0 {
		return fmt.Errorf("%w: route %q has invalid limits", ErrUnknownRoute, r.Name)
	}
	return nil
}

func (r Route) matches(envelope Envelope) bool {
	if len(r.Kinds) > 0 && !containsKind(r.Kinds, envelope.Kind) {
		return false
	}
	if len(r.Types) > 0 {
		return slices.Contains(r.Types, envelope.Type)
	}
	return true
}

func containsKind(kinds []Kind, want Kind) bool {
	return slices.Contains(kinds, want)
}

func validateKindFilter(owner string, kinds []Kind) error {
	seen := make(map[Kind]struct{}, len(kinds))
	for _, kind := range kinds {
		switch kind {
		case KindCommand, KindQuery, KindEvent, KindReply:
		default:
			return fmt.Errorf("%w: %s has invalid kind %q", ErrInvalidEnvelope, owner, kind)
		}
		if _, duplicate := seen[kind]; duplicate {
			return fmt.Errorf("%w: %s has duplicate kind %q", ErrInvalidEnvelope, owner, kind)
		}
		seen[kind] = struct{}{}
	}
	return nil
}

func validateStringFilter(owner, label string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.TrimSpace(value)
		if normalized == "" {
			return fmt.Errorf("%w: %s has an empty accepted %s", ErrInvalidEnvelope, owner, label)
		}
		if normalized != value {
			return fmt.Errorf("%w: %s has a non-canonical accepted %s %q", ErrInvalidEnvelope, owner, label, value)
		}
		if _, duplicate := seen[normalized]; duplicate {
			return fmt.Errorf("%w: %s has duplicate accepted %s %q", ErrInvalidEnvelope, owner, label, normalized)
		}
		seen[normalized] = struct{}{}
	}
	return nil
}
