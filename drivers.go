package messaging

import (
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
)

type driverSnapshot struct{ drivers map[string]Driver }

// DriverRegistry atomically publishes complete immutable driver snapshots.
type DriverRegistry struct {
	snapshot atomic.Pointer[driverSnapshot]
}

func NewDriverRegistry(drivers map[string]Driver) (*DriverRegistry, error) {
	r := &DriverRegistry{}
	if err := r.Replace(drivers); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *DriverRegistry) Replace(drivers map[string]Driver) error {
	next := make(map[string]Driver, len(drivers))
	for name, driver := range drivers {
		name = strings.TrimSpace(name)
		if name == "" || driver == nil {
			return fmt.Errorf("%w: driver name and value are required", ErrUnknownDriver)
		}
		if _, exists := next[name]; exists {
			return fmt.Errorf("%w: duplicate driver %q", ErrUnknownDriver, name)
		}
		next[name] = driver
	}
	r.snapshot.Store(&driverSnapshot{drivers: next})
	return nil
}

func (r *DriverRegistry) Lookup(name string) (Driver, bool) {
	if r == nil {
		return nil, false
	}
	snapshot := r.snapshot.Load()
	if snapshot == nil {
		return nil, false
	}
	driver, ok := snapshot.drivers[name]
	return driver, ok
}

func (r *DriverRegistry) All() map[string]Driver {
	out := map[string]Driver{}
	if r == nil {
		return out
	}
	snapshot := r.snapshot.Load()
	if snapshot == nil {
		return out
	}
	maps.Copy(out, snapshot.drivers)
	return out
}

func (r *DriverRegistry) Require(name string, required ...Capability) (Driver, error) {
	driver, ok := r.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownDriver, name)
	}
	if err := driver.Capabilities().Validate(required...); err != nil {
		return nil, fmt.Errorf("driver %q: %w", name, err)
	}
	return driver, nil
}
