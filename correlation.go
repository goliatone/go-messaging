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
}

type pendingReply struct {
	expectedType string
	deadline     time.Time
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

func NewCorrelationRegistry(capacity int, defaultTimeout time.Duration) (*CorrelationRegistry, error) {
	if capacity <= 0 || defaultTimeout <= 0 {
		return nil, fmt.Errorf("%w: positive capacity and timeout are required", ErrCorrelation)
	}
	return &CorrelationRegistry{pending: make(map[string]*pendingReply), capacity: capacity, defaultTimeout: defaultTimeout}, nil
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
	pending := &pendingReply{expectedType: expectedType, deadline: deadline, result: make(chan correlationResult, 1)}
	r.pending[correlationID] = pending
	return &ReplyWaiter{registry: r, id: correlationID, pending: pending}, nil
}

// Deliver resolves exactly one matching waiter. False identifies late, duplicate,
// unknown, or mismatched replies; mismatches leave the real waiter active.
func (r *CorrelationRegistry) Deliver(envelope Envelope) bool {
	if envelope.Kind != KindReply || envelope.CorrelationID == "" {
		return false
	}
	r.mu.Lock()
	pending, ok := r.pending[envelope.CorrelationID]
	if !ok || pending.expectedType != envelope.Type || time.Now().After(pending.deadline) {
		if ok && time.Now().After(pending.deadline) {
			delete(r.pending, envelope.CorrelationID)
		}
		r.mu.Unlock()
		return false
	}
	delete(r.pending, envelope.CorrelationID)
	r.mu.Unlock()
	pending.result <- correlationResult{envelope: envelope.Clone()}
	return true
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
