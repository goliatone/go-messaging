package messaging

import (
	"context"
	"errors"
	"testing"
)

type stubDriver struct{ capabilities Capabilities }

func (d *stubDriver) Capabilities() Capabilities { return d.capabilities }
func (*stubDriver) Start(context.Context) error  { return nil }
func (*stubDriver) Ready() <-chan struct{}       { ch := make(chan struct{}); close(ch); return ch }
func (*stubDriver) Errors() <-chan error         { return nil }
func (*stubDriver) Close(context.Context) error  { return nil }

func TestCapabilitiesValidate(t *testing.T) {
	c := Capabilities{Durability: true}
	if err := c.Validate(CapabilityDurability); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(CapabilityReplay, CapabilityAcknowledgement); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}

func TestDriverRegistryPublishesSnapshots(t *testing.T) {
	first := &stubDriver{capabilities: Capabilities{Durability: true}}
	r, err := NewDriverRegistry(map[string]Driver{"jobs": first})
	if err != nil {
		t.Fatal(err)
	}
	copy := r.All()
	delete(copy, "jobs")
	if _, ok := r.Lookup("jobs"); !ok {
		t.Fatal("caller mutated registry")
	}
	previous := r.All()
	if err := r.Replace(map[string]Driver{"": first}); err == nil {
		t.Fatal("expected invalid replacement")
	}
	if len(previous) != len(r.All()) {
		t.Fatal("failed replacement changed snapshot")
	}
	if _, err := r.Require("jobs", CapabilityReplay); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}
