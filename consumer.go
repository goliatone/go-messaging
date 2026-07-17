package messaging

import (
	"context"
	"time"
)

type Source struct {
	Name     string
	Group    string
	Consumer string
	From     string
}

type Handler func(context.Context, Delivery) HandleResult

type Consumer interface {
	Subscribe(context.Context, Source, Handler) (Subscription, error)
}

type ConsumeDriver interface {
	Driver
	Consumer
}

type Subscription interface {
	Ready() <-chan struct{}
	Errors() <-chan error
	Close(context.Context) error
}

type Disposition string

const (
	DispositionComplete   Disposition = "complete"
	DispositionRetry      Disposition = "retry"
	DispositionReject     Disposition = "reject"
	DispositionDeadLetter Disposition = "dead_letter"
)

type HandleResult struct {
	Disposition Disposition
	RetryAfter  time.Duration
	Err         error
}

func Complete() HandleResult { return HandleResult{Disposition: DispositionComplete} }
func Retry(err error, after time.Duration) HandleResult {
	return HandleResult{Disposition: DispositionRetry, RetryAfter: after, Err: err}
}
func Reject(err error) HandleResult {
	return HandleResult{Disposition: DispositionReject, Err: err}
}
func DeadLetter(err error) HandleResult {
	return HandleResult{Disposition: DispositionDeadLetter, Err: err}
}

// InvokeHandler contains untrusted handler panics at the delivery boundary.
// The panic value is deliberately excluded from the returned error because it
// may contain payload or credential data.
func InvokeHandler(ctx context.Context, handler Handler, delivery Delivery) (result HandleResult) {
	if handler == nil {
		return Reject(ErrHandlerPanic)
	}
	defer func() {
		if recover() != nil {
			result = Reject(ErrHandlerPanic)
		}
	}()
	return handler(ctx, delivery)
}
