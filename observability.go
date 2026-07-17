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
