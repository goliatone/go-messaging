package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/transport/valkey/internal/shared"
	valkey "github.com/valkey-io/valkey-go"
)

type Config struct {
	Valkey    shared.Config
	QueueSize int
	Codec     messaging.Codec
}

func DefaultConfig(addresses ...string) Config {
	return Config{Valkey: shared.DefaultConfig(addresses...), QueueSize: 256, Codec: messaging.NewJSONCodec()}
}

type Driver struct {
	mu     sync.Mutex
	config Config
	client valkey.Client
	ready  chan struct{}
	errors chan error
	closed bool
	subs   map[*subscription]struct{}
}

func New(config Config) (*Driver, error) {
	if err := config.Valkey.Validate(); err != nil {
		return nil, err
	}
	if config.QueueSize <= 0 {
		return nil, fmt.Errorf("valkey pubsub: queue size must be positive")
	}
	if config.Codec == nil {
		return nil, fmt.Errorf("valkey pubsub: codec is required")
	}
	return &Driver{config: config, ready: make(chan struct{}), errors: make(chan error, 16), subs: make(map[*subscription]struct{})}, nil
}

func (*Driver) Capabilities() messaging.Capabilities {
	return messaging.Capabilities{Fanout: true, RequestReply: true}
}
func (d *Driver) Ready() <-chan struct{} { return d.ready }
func (d *Driver) Errors() <-chan error   { return d.errors }

func (d *Driver) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return messaging.ErrSubscriptionClosed
	}
	if d.client != nil {
		return nil
	}
	client, err := shared.Open(d.config.Valkey)
	if err != nil {
		return err
	}
	if err := shared.Ping(ctx, client, d.config.Valkey.ConnectTimeout); err != nil {
		client.Close()
		return err
	}
	d.client = client
	close(d.ready)
	return nil
}

func (d *Driver) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	client := d.client
	d.client = nil
	subs := make([]*subscription, 0, len(d.subs))
	for sub := range d.subs {
		subs = append(subs, sub)
	}
	d.mu.Unlock()
	var joined error
	for _, sub := range subs {
		joined = errors.Join(joined, sub.Close(ctx))
	}
	if client != nil {
		client.Close()
	}
	close(d.errors)
	return joined
}

func (d *Driver) Publish(ctx context.Context, destination messaging.Destination, envelope messaging.Envelope) (messaging.PublishResult, error) {
	if strings.TrimSpace(destination.Name) == "" {
		return messaging.PublishResult{Outcome: messaging.PublishRejected}, fmt.Errorf("%w: empty channel", messaging.ErrPublishRejected)
	}
	d.mu.Lock()
	client := d.client
	closed := d.closed
	d.mu.Unlock()
	if client == nil || closed {
		return messaging.PublishResult{Outcome: messaging.PublishDefinitelyNotPublished}, messaging.ErrNotPublished
	}
	data, err := d.config.Codec.Encode(ctx, envelope.Clone())
	if err != nil {
		return messaging.PublishResult{Outcome: messaging.PublishRejected}, err
	}
	if len(data) > d.config.Valkey.MaxMessageBytes {
		return messaging.PublishResult{Outcome: messaging.PublishRejected}, messaging.ErrMessageTooLarge
	}
	count, err := client.Do(ctx, client.B().Publish().Channel(destination.Name).Message(string(data)).Build()).AsInt64()
	if err != nil {
		return messaging.PublishResult{Outcome: classifiedPublishOutcome(err)}, shared.Classify("publish", err)
	}
	return messaging.PublishResult{Outcome: messaging.PublishAccepted, Transport: "valkey.pubsub", Destination: destination.Name, RecipientCount: &count}, nil
}

func classifiedPublishOutcome(err error) messaging.PublishOutcome {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, valkey.ErrClosing) {
		return messaging.PublishDefinitelyNotPublished
	}
	return messaging.PublishAmbiguous
}

func (d *Driver) Subscribe(ctx context.Context, source messaging.Source, handler messaging.Handler) (messaging.Subscription, error) {
	if strings.TrimSpace(source.Name) == "" || handler == nil {
		return nil, fmt.Errorf("valkey pubsub: source and handler are required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil, messaging.ErrSubscriptionNotReady
	}
	if d.closed {
		return nil, messaging.ErrSubscriptionClosed
	}
	s := newSubscription(d, ctx, d.client, source, handler)
	d.subs[s] = struct{}{}
	s.start()
	return s, nil
}

func (d *Driver) removeSubscription(s *subscription) { d.mu.Lock(); delete(d.subs, s); d.mu.Unlock() }

type inboundMessage struct {
	channel string
	data    []byte
}

type subscription struct {
	driver    *Driver
	source    messaging.Source
	handler   messaging.Handler
	client    valkey.Client
	ctx       context.Context
	cancel    context.CancelFunc
	ready     chan struct{}
	readyOnce sync.Once
	errors    chan error
	done      chan struct{}
	inbound   chan inboundMessage
}

func newSubscription(driver *Driver, parent context.Context, client valkey.Client, source messaging.Source, handler messaging.Handler) *subscription {
	ctx, cancel := context.WithCancel(parent)
	return &subscription{driver: driver, source: source, handler: handler, client: client, ctx: ctx, cancel: cancel, ready: make(chan struct{}), errors: make(chan error, 16), done: make(chan struct{}), inbound: make(chan inboundMessage, driver.config.QueueSize)}
}
func (s *subscription) Ready() <-chan struct{} { return s.ready }
func (s *subscription) Errors() <-chan error   { return s.errors }
func (s *subscription) Close(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *subscription) report(err error) {
	if err == nil {
		return
	}
	select {
	case s.errors <- err:
	default:
	}
}

func (s *subscription) start() { go s.run() }
func (s *subscription) run() {
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); s.process() }()
	receiveCtx := valkey.WithOnSubscriptionHook(s.ctx, func(event valkey.PubSubSubscription) {
		if event.Kind == "subscribe" && event.Channel == s.source.Name {
			s.readyOnce.Do(func() { close(s.ready) })
		}
	})
	err := s.client.Receive(receiveCtx, s.client.B().Subscribe().Channel(s.source.Name).Build(), func(message valkey.PubSubMessage) {
		if len(message.Message) > s.driver.config.Valkey.MaxMessageBytes {
			s.report(messaging.ErrMessageTooLarge)
			return
		}
		item := inboundMessage{channel: message.Channel, data: []byte(message.Message)}
		select {
		case s.inbound <- item:
		default:
			s.report(fmt.Errorf("valkey pubsub: intake queue full"))
		}
	})
	close(s.inbound)
	<-workerDone
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, valkey.ErrClosing) {
		s.report(shared.Classify("subscribe", err))
	}
	close(s.errors)
	s.driver.removeSubscription(s)
	close(s.done)
}

func (s *subscription) process() {
	for item := range s.inbound {
		envelope, err := s.driver.config.Codec.Decode(s.ctx, item.data)
		if err != nil {
			s.report(err)
			continue
		}
		delivery := messaging.NewDelivery(envelope, messaging.DeliveryInfo{Transport: "valkey.pubsub", Destination: item.channel, DeliveryID: envelope.ID, Attempt: 1, ReceivedAt: time.Now().UTC()})
		result := messaging.InvokeHandler(s.ctx, s.handler, delivery)
		if result.Disposition == messaging.DispositionRetry || result.Disposition == messaging.DispositionDeadLetter {
			s.report(messaging.ErrUnsupportedDisposition)
		} else if result.Err != nil {
			s.report(result.Err)
		}
	}
}
