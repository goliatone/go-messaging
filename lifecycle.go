package messaging

import "context"

// Driver implementations must close Ready exactly once after their provider is
// usable. Start, Capabilities and Close must be safe for concurrent callers.
type Driver interface {
	Capabilities() Capabilities
	Start(context.Context) error
	Ready() <-chan struct{}
	Errors() <-chan error
	Close(context.Context) error
}

type LifecycleState string

const (
	LifecycleNew      LifecycleState = "new"
	LifecycleStarting LifecycleState = "starting"
	LifecycleReady    LifecycleState = "ready"
	LifecycleClosing  LifecycleState = "closing"
	LifecycleClosed   LifecycleState = "closed"
	LifecycleFailed   LifecycleState = "failed"
)
