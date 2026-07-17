package messaging

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
)

type IngressBinding struct {
	Name                 string
	LogicalRoute         string
	Driver               string
	Source               Source
	AcceptedKinds        []Kind
	AcceptedTypes        []string
	Codecs               []Codec
	Handlers             []Handler
	RequiredCapabilities []Capability
	RequiredDispositions []Disposition
}

func (b IngressBinding) clone() IngressBinding {
	b.AcceptedKinds = append([]Kind(nil), b.AcceptedKinds...)
	b.AcceptedTypes = append([]string(nil), b.AcceptedTypes...)
	b.Codecs = append([]Codec(nil), b.Codecs...)
	b.Handlers = append([]Handler(nil), b.Handlers...)
	b.RequiredCapabilities = append([]Capability(nil), b.RequiredCapabilities...)
	b.RequiredDispositions = append([]Disposition(nil), b.RequiredDispositions...)
	return b
}

func (b IngressBinding) accepts(envelope Envelope) bool {
	if len(b.AcceptedKinds) > 0 && !containsKind(b.AcceptedKinds, envelope.Kind) {
		return false
	}
	if len(b.AcceptedTypes) > 0 {
		return slices.Contains(b.AcceptedTypes, envelope.Type)
	}
	return true
}

type ingressSnapshot struct{ bindings map[string]IngressBinding }

type Ingress struct {
	drivers  *DriverRegistry
	snapshot atomic.Pointer[ingressSnapshot]
}

func NewIngress(drivers *DriverRegistry, bindings []IngressBinding) (*Ingress, error) {
	if drivers == nil {
		return nil, fmt.Errorf("%w: driver registry is required", ErrUnknownDriver)
	}
	i := &Ingress{drivers: drivers}
	if err := i.ReplaceBindings(bindings); err != nil {
		return nil, err
	}
	return i, nil
}

func (i *Ingress) ReplaceBindings(bindings []IngressBinding) error {
	next := make(map[string]IngressBinding, len(bindings))
	for _, binding := range bindings {
		if strings.TrimSpace(binding.Name) == "" || strings.TrimSpace(binding.LogicalRoute) == "" || strings.TrimSpace(binding.Source.Name) == "" {
			return fmt.Errorf("%w: ingress binding name, route, and source are required", ErrUnknownRoute)
		}
		if len(binding.Handlers) == 0 {
			return fmt.Errorf("%w: ingress binding %q requires a handler", ErrUnknownRoute, binding.Name)
		}
		if _, exists := next[binding.Name]; exists {
			return fmt.Errorf("%w: duplicate ingress binding %q", ErrUnknownRoute, binding.Name)
		}
		required := append([]Capability(nil), binding.RequiredCapabilities...)
		for _, disposition := range binding.RequiredDispositions {
			required = append(required, CapabilityForDisposition(disposition)...)
		}
		driver, err := i.drivers.Require(binding.Driver, required...)
		if err != nil {
			return fmt.Errorf("ingress %q: %w", binding.Name, err)
		}
		if _, ok := driver.(ConsumeDriver); !ok {
			return fmt.Errorf("ingress %q: %w", binding.Name, ErrUnsupportedCapability)
		}
		next[binding.Name] = binding.clone()
	}
	i.snapshot.Store(&ingressSnapshot{bindings: next})
	return nil
}

// Subscribe starts every binding from one immutable generation.
func (i *Ingress) Subscribe(ctx context.Context) ([]Subscription, error) {
	snapshot := i.snapshot.Load()
	if snapshot == nil {
		return nil, nil
	}
	subscriptions := make([]Subscription, 0, len(snapshot.bindings))
	for _, binding := range snapshot.bindings {
		driver, ok := i.drivers.Lookup(binding.Driver)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownDriver, binding.Driver)
		}
		consumer, ok := driver.(ConsumeDriver)
		if !ok {
			return nil, fmt.Errorf("%w: driver %s cannot consume", ErrUnsupportedCapability, binding.Driver)
		}
		bindingCopy := binding.clone()
		subscription, err := consumer.Subscribe(ctx, bindingCopy.Source, func(ctx context.Context, delivery Delivery) HandleResult {
			envelope := delivery.Envelope()
			if !bindingCopy.accepts(envelope) {
				return Reject(fmt.Errorf("%w: ingress policy rejected kind/type", ErrInvalidEnvelope))
			}
			result := Complete()
			for _, handler := range bindingCopy.Handlers {
				result = InvokeHandler(ctx, handler, delivery)
				if result.Disposition != DispositionComplete {
					break
				}
			}
			return result
		})
		if err != nil {
			var closeErr error
			for _, existing := range subscriptions {
				closeErr = errors.Join(closeErr, existing.Close(ctx))
			}
			return nil, errors.Join(err, closeErr)
		}
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions, nil
}
