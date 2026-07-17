package testkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

type MemoryDriver struct {
	mu      sync.RWMutex
	ready   chan struct{}
	errors  chan error
	started bool
	closed  bool
	nextID  int
	subs    map[int]*MemorySubscription
}

func NewMemoryDriver() *MemoryDriver {
	return &MemoryDriver{ready: make(chan struct{}), errors: make(chan error, 1), subs: make(map[int]*MemorySubscription)}
}
func (*MemoryDriver) Capabilities() messaging.Capabilities {
	return messaging.Capabilities{Fanout: true, RequestReply: true}
}
func (d *MemoryDriver) Ready() <-chan struct{} { return d.ready }
func (d *MemoryDriver) Errors() <-chan error   { return d.errors }
func (d *MemoryDriver) Start(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return messaging.ErrSubscriptionClosed
	}
	if !d.started {
		d.started = true
		close(d.ready)
	}
	return nil
}
func (d *MemoryDriver) Close(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	for _, sub := range d.subs {
		sub.closeLocked()
	}
	d.subs = map[int]*MemorySubscription{}
	close(d.errors)
	return nil
}
func (d *MemoryDriver) Subscribe(_ context.Context, source messaging.Source, handler messaging.Handler) (messaging.Subscription, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.started {
		return nil, messaging.ErrSubscriptionNotReady
	}
	if d.closed {
		return nil, messaging.ErrSubscriptionClosed
	}
	d.nextID++
	sub := &MemorySubscription{id: d.nextID, driver: d, source: source, handler: handler, ready: make(chan struct{}), errors: make(chan error, 1)}
	close(sub.ready)
	d.subs[sub.id] = sub
	return sub, nil
}
func (d *MemoryDriver) Publish(ctx context.Context, destination messaging.Destination, envelope messaging.Envelope) (messaging.PublishResult, error) {
	if err := envelope.Validate(); err != nil {
		return messaging.PublishResult{Outcome: messaging.PublishRejected}, err
	}
	d.mu.RLock()
	if !d.started || d.closed {
		d.mu.RUnlock()
		return messaging.PublishResult{Outcome: messaging.PublishDefinitelyNotPublished}, messaging.ErrNotPublished
	}
	subscribers := make([]*MemorySubscription, 0, len(d.subs))
	for _, sub := range d.subs {
		if sub.source.Name == destination.Name {
			subscribers = append(subscribers, sub)
		}
	}
	d.mu.RUnlock()
	for _, sub := range subscribers {
		delivery := messaging.NewDelivery(envelope, messaging.DeliveryInfo{Transport: "memory", Destination: destination.Name, DeliveryID: fmt.Sprintf("memory-%d", time.Now().UnixNano()), Attempt: 1, ReceivedAt: time.Now().UTC()})
		result := sub.handler(ctx, delivery)
		if result.Disposition == messaging.DispositionRetry || result.Disposition == messaging.DispositionDeadLetter {
			select {
			case sub.errors <- messaging.ErrUnsupportedDisposition:
			default:
			}
		}
	}
	count := int64(len(subscribers))
	return messaging.PublishResult{Outcome: messaging.PublishAccepted, Transport: "memory", Destination: destination.Name, RecipientCount: &count}, nil
}

type MemorySubscription struct {
	mu      sync.Mutex
	id      int
	driver  *MemoryDriver
	source  messaging.Source
	handler messaging.Handler
	ready   chan struct{}
	errors  chan error
	closed  bool
}

func (s *MemorySubscription) Ready() <-chan struct{} { return s.ready }
func (s *MemorySubscription) Errors() <-chan error   { return s.errors }
func (s *MemorySubscription) Close(context.Context) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	delete(s.driver.subs, s.id)
	s.closeLocked()
	return nil
}
func (s *MemorySubscription) closeLocked() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.errors)
	}
}
