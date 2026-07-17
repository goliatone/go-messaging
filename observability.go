package messaging

import (
	"context"
	"time"
)

type Operation string

const (
	OperationPublish Operation = "publish"
	OperationConsume Operation = "consume"
	OperationReply   Operation = "reply"
)

// Observation intentionally excludes payload and provider credentials.
type Observation struct {
	Operation     Operation
	LogicalRoute  string
	Kind          Kind
	MessageType   string
	Transport     string
	Destination   string
	CorrelationID string
	Attempt       int
	Outcome       string
	Latency       time.Duration
	Err           error
}

type Observer interface {
	Observe(context.Context, Observation)
}

type ObserverFunc func(context.Context, Observation)

func (f ObserverFunc) Observe(ctx context.Context, observation Observation) {
	if f != nil {
		f(ctx, observation)
	}
}

type NopObserver struct{}

func (NopObserver) Observe(context.Context, Observation) {}

type guardedObserver struct{ next Observer }

func (o guardedObserver) Observe(ctx context.Context, observation Observation) {
	defer func() {
		if recovered := recover(); recovered == nil {
			return
		}
	}()
	o.next.Observe(ctx, observation)
}

// protectObserver makes observability best-effort: an observer failure must
// never turn an accepted publication into a caller-visible failure or change a
// consumer's settlement decision.
func protectObserver(observer Observer) Observer {
	if observer == nil {
		return NopObserver{}
	}
	if _, ok := observer.(guardedObserver); ok {
		return observer
	}
	return guardedObserver{next: observer}
}
