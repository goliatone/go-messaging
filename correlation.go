package messaging

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type CorrelationRegistry struct {
	mu             sync.Mutex
	pending        map[string]*pendingReply
	capacity       int
	defaultTimeout time.Duration
	observer       Observer
}

type pendingReply struct {
	expectedType string
	deadline     time.Time
	registeredAt time.Time
	result       chan correlationResult
}

type correlationResult struct {
	envelope Envelope
	err      error
}

type ReplyWaiter struct {
	registry *CorrelationRegistry
	id       string
	pending  *pendingReply
	once     sync.Once
	awaited  atomic.Bool
}

func NewCorrelationRegistry(capacity int, defaultTimeout time.Duration, observers ...Observer) (*CorrelationRegistry, error) {
	if capacity <= 0 || defaultTimeout <= 0 {
		return nil, fmt.Errorf("%w: positive capacity and timeout are required", ErrCorrelation)
	}
	observer := Observer(NopObserver{})
	if len(observers) > 0 && observers[0] != nil {
		observer = observers[0]
	}
	return &CorrelationRegistry{pending: make(map[string]*pendingReply), capacity: capacity, defaultTimeout: defaultTimeout, observer: protectObserver(observer)}, nil
}

// Register must be called before publishing the corresponding request.
func (r *CorrelationRegistry) Register(correlationID, expectedType string, deadline time.Time) (*ReplyWaiter, error) {
	correlationID = strings.TrimSpace(correlationID)
	expectedType = strings.TrimSpace(expectedType)
	if correlationID == "" || expectedType == "" {
		return nil, fmt.Errorf("%w: id and expected type are required", ErrCorrelation)
	}
	if deadline.IsZero() {
		deadline = time.Now().Add(r.defaultTimeout)
	}
	if !deadline.After(time.Now()) {
		return nil, fmt.Errorf("%w: deadline has elapsed", ErrCorrelation)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) >= r.capacity {
		return nil, fmt.Errorf("%w: pending capacity %d reached", ErrCorrelation, r.capacity)
	}
	if _, exists := r.pending[correlationID]; exists {
		return nil, fmt.Errorf("%w: duplicate id %q", ErrCorrelation, correlationID)
	}
	pending := &pendingReply{expectedType: expectedType, deadline: deadline, registeredAt: time.Now(), result: make(chan correlationResult, 1)}
	r.pending[correlationID] = pending
	return &ReplyWaiter{registry: r, id: correlationID, pending: pending}, nil
}

// Deliver resolves exactly one matching waiter. False identifies late, duplicate,
// unknown, or mismatched replies; mismatches leave the real waiter active.
func (r *CorrelationRegistry) Deliver(envelope Envelope) bool {
	if envelope.Kind != KindReply || envelope.CorrelationID == "" {
		r.observeReply(envelope, nil, "invalid", ErrCorrelation)
		return false
	}
	r.mu.Lock()
	pending, ok := r.pending[envelope.CorrelationID]
	now := time.Now()
	if !ok || pending.expectedType != envelope.Type || now.After(pending.deadline) {
		outcome := "unknown_or_duplicate"
		if ok && pending.expectedType != envelope.Type {
			outcome = "mismatched"
		}
		if ok && now.After(pending.deadline) {
			delete(r.pending, envelope.CorrelationID)
			outcome = "late"
		}
		r.mu.Unlock()
		r.observeReply(envelope, pending, outcome, ErrCorrelation)
		return false
	}
	delete(r.pending, envelope.CorrelationID)
	r.mu.Unlock()
	pending.result <- correlationResult{envelope: envelope.Clone()}
	r.observeReply(envelope, pending, "matched", nil)
	return true
}

func (r *CorrelationRegistry) observeReply(envelope Envelope, pending *pendingReply, outcome string, err error) {
	latency := time.Duration(0)
	if pending != nil {
		latency = time.Since(pending.registeredAt)
	}
	r.observer.Observe(context.Background(), Observation{
		Operation: OperationReply, Kind: envelope.Kind, MessageType: envelope.Type,
		CorrelationID: envelope.CorrelationID, Outcome: outcome, Latency: latency, Err: err,
	})
}

func (w *ReplyWaiter) Await(ctx context.Context) (Envelope, error) {
	if w == nil || w.registry == nil {
		return Envelope{}, ErrCorrelation
	}
	if !w.awaited.CompareAndSwap(false, true) {
		return Envelope{}, fmt.Errorf("%w: waiter already consumed", ErrCorrelation)
	}
	timer := time.NewTimer(time.Until(w.pending.deadline))
	defer timer.Stop()
	select {
	case result := <-w.pending.result:
		w.once.Do(func() {})
		return result.envelope.Clone(), result.err
	case <-ctx.Done():
		w.Cancel()
		return Envelope{}, ctx.Err()
	case <-timer.C:
		w.Cancel()
		return Envelope{}, ErrReplyTimeout
	}
}

func (w *ReplyWaiter) Cancel() {
	if w == nil || w.registry == nil {
		return
	}
	w.once.Do(func() {
		removed := false
		w.registry.mu.Lock()
		if current, ok := w.registry.pending[w.id]; ok && current == w.pending {
			delete(w.registry.pending, w.id)
			removed = true
		}
		w.registry.mu.Unlock()
		if removed {
			select {
			case w.pending.result <- correlationResult{err: ErrCorrelation}:
			default:
			}
		}
	})
}

func (r *CorrelationRegistry) Pending() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.pending) }
