package streams

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/transport/valkey/internal/shared"
	valkey "github.com/valkey-io/valkey-go"
)

const envelopeField = "envelope"

type Config struct {
	Valkey           shared.Config
	Codec            messaging.Codec
	BatchSize        int64
	Block            time.Duration
	ClaimIdle        time.Duration
	ClaimInterval    time.Duration
	MaxDeliveries    int64
	DeadLetterSuffix string
}

func DefaultConfig(addresses ...string) Config {
	return Config{Valkey: shared.DefaultConfig(addresses...), Codec: messaging.NewJSONCodec(), BatchSize: 32, Block: time.Second, ClaimIdle: 30 * time.Second, ClaimInterval: 10 * time.Second, MaxDeliveries: 5, DeadLetterSuffix: ".dead-letter"}
}

func (c Config) validate() error {
	if err := c.Valkey.Validate(); err != nil {
		return err
	}
	if c.Codec == nil || c.BatchSize <= 0 || c.Block < time.Millisecond || c.ClaimIdle < time.Millisecond || c.ClaimInterval <= 0 || c.MaxDeliveries <= 0 || strings.TrimSpace(c.DeadLetterSuffix) == "" {
		return fmt.Errorf("valkey streams: invalid limits or codec")
	}
	return nil
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
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &Driver{config: config, ready: make(chan struct{}), errors: make(chan error, 16), subs: make(map[*subscription]struct{})}, nil
}
func (*Driver) Capabilities() messaging.Capabilities {
	return messaging.Capabilities{Durability: true, Acknowledgement: true, CompetingConsumers: true, Ordering: true, Replay: true, RequestReply: true}
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
	if err = shared.Ping(ctx, client, d.config.Valkey.ConnectTimeout); err != nil {
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
		return messaging.PublishResult{Outcome: messaging.PublishRejected}, fmt.Errorf("%w: empty stream", messaging.ErrPublishRejected)
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
	id, err := client.Do(ctx, client.B().Xadd().Key(destination.Name).Id("*").FieldValue().FieldValue(envelopeField, string(data)).Build()).ToString()
	if err != nil {
		return messaging.PublishResult{Outcome: classifyPublish(err)}, shared.Classify("xadd", err)
	}
	return messaging.PublishResult{Outcome: messaging.PublishAccepted, Transport: "valkey.streams", Destination: destination.Name, ProviderMessageID: id}, nil
}

func classifyPublish(err error) messaging.PublishOutcome {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, valkey.ErrClosing) {
		return messaging.PublishDefinitelyNotPublished
	}
	return messaging.PublishAmbiguous
}

func (d *Driver) Subscribe(ctx context.Context, source messaging.Source, handler messaging.Handler) (messaging.Subscription, error) {
	if strings.TrimSpace(source.Name) == "" || strings.TrimSpace(source.Group) == "" || strings.TrimSpace(source.Consumer) == "" || handler == nil {
		return nil, fmt.Errorf("valkey streams: stream, group, consumer, and handler are required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil, messaging.ErrSubscriptionNotReady
	}
	if d.closed {
		return nil, messaging.ErrSubscriptionClosed
	}
	start := source.From
	if start == "" {
		start = "0"
	}
	err := d.client.Do(ctx, d.client.B().XgroupCreate().Key(source.Name).Group(source.Group).Id(start).Mkstream().Build()).Error()
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return nil, shared.Classify("xgroup-create", err)
	}
	s := newSubscription(d, ctx, d.client, source, handler)
	d.subs[s] = struct{}{}
	close(s.ready)
	go s.run()
	return s, nil
}
func (d *Driver) removeSubscription(s *subscription) { d.mu.Lock(); delete(d.subs, s); d.mu.Unlock() }

type subscription struct {
	driver      *Driver
	client      valkey.Client
	source      messaging.Source
	handler     messaging.Handler
	ctx         context.Context
	cancel      context.CancelFunc
	ready       chan struct{}
	errors      chan error
	done        chan struct{}
	closed      atomic.Bool
	claimCursor string
}

func newSubscription(driver *Driver, parent context.Context, client valkey.Client, source messaging.Source, handler messaging.Handler) *subscription {
	ctx, cancel := context.WithCancel(parent)
	return &subscription{driver: driver, client: client, source: source, handler: handler, ctx: ctx, cancel: cancel, ready: make(chan struct{}), errors: make(chan error, 16), done: make(chan struct{}), claimCursor: "0-0"}
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

func (s *subscription) run() {
	defer func() { s.closed.Store(true); s.driver.removeSubscription(s); close(s.errors); close(s.done) }()
	lastClaim := time.Time{}
	for s.ctx.Err() == nil {
		if time.Since(lastClaim) >= s.driver.config.ClaimInterval {
			entries, err := s.claimPending()
			if err != nil {
				s.report(err)
			} else {
				s.handleEntries(entries)
			}
			lastClaim = time.Now()
		}
		result := s.client.Do(s.ctx, s.client.B().Xreadgroup().Group(s.source.Group, s.source.Consumer).Count(s.driver.config.BatchSize).Block(s.driver.config.Block.Milliseconds()).Streams().Key(s.source.Name).Id(">").Build())
		streams, err := result.AsXRead()
		if err != nil {
			if valkey.IsValkeyNil(err) || errors.Is(err, context.Canceled) || errors.Is(err, valkey.ErrClosing) {
				continue
			}
			s.report(shared.Classify("xreadgroup", err))
			continue
		}
		s.handleEntries(streams[s.source.Name])
	}
}

func (s *subscription) claimPending() ([]valkey.XRangeEntry, error) {
	result := s.client.Do(s.ctx, s.client.B().Xautoclaim().Key(s.source.Name).Group(s.source.Group).Consumer(s.source.Consumer).MinIdleTime(strconv.FormatInt(s.driver.config.ClaimIdle.Milliseconds(), 10)).Start(s.claimCursor).Count(s.driver.config.BatchSize).Build())
	values, err := result.ToArray()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, nil
		}
		return nil, shared.Classify("xautoclaim", err)
	}
	if len(values) < 2 {
		return nil, fmt.Errorf("valkey streams: malformed XAUTOCLAIM response")
	}
	next, err := values[0].ToString()
	if err != nil {
		return nil, fmt.Errorf("valkey streams: parse XAUTOCLAIM cursor: %w", err)
	}
	if next == "" {
		next = "0-0"
	}
	s.claimCursor = next
	entries, err := values[1].AsXRange()
	if err != nil {
		return nil, fmt.Errorf("valkey streams: parse XAUTOCLAIM: %w", err)
	}
	return entries, nil
}

func (s *subscription) handleEntries(entries []valkey.XRangeEntry) {
	for _, entry := range entries {
		if s.ctx.Err() != nil {
			return
		}
		s.handleEntry(entry)
	}
}

func (s *subscription) handleEntry(entry valkey.XRangeEntry) {
	raw, ok := entry.FieldValues[envelopeField]
	attempt, err := s.deliveryCount(entry.ID)
	if err != nil {
		s.report(err)
		return
	}
	if !ok || len(raw) > s.driver.config.Valkey.MaxMessageBytes {
		reason := messaging.ErrInvalidEnvelope
		if len(raw) > s.driver.config.Valkey.MaxMessageBytes {
			reason = messaging.ErrMessageTooLarge
		}
		s.deadLetterAndAck(entry.ID, raw, reason)
		return
	}
	envelope, err := s.driver.config.Codec.Decode(s.ctx, []byte(raw))
	if err != nil {
		s.deadLetterAndAck(entry.ID, raw, err)
		return
	}
	delivery := &streamDelivery{BasicDelivery: messaging.NewDelivery(envelope, messaging.DeliveryInfo{Transport: "valkey.streams", Destination: s.source.Name, DeliveryID: entry.ID, Attempt: int(attempt), ReceivedAt: time.Now().UTC()}), subscription: s, settlement: s, id: entry.ID, raw: raw}
	result := messaging.InvokeHandler(s.ctx, s.handler, delivery)
	if delivery.attempted.Load() {
		return
	}
	s.settleResult(delivery, result, attempt)
}

func (s *subscription) deadLetterAndAck(id, raw string, reason error) {
	if err := s.deadLetter(s.ctx, id, raw, reason); err != nil {
		s.report(err)
		return
	}
	if err := s.ack(s.ctx, id); err != nil {
		s.report(err)
	}
}

func (s *subscription) settleResult(delivery *streamDelivery, result messaging.HandleResult, attempt int64) {
	var err error
	switch result.Disposition {
	case messaging.DispositionComplete:
		err = delivery.Ack(s.ctx)
	case messaging.DispositionRetry:
		if result.RetryAfter > 0 {
			s.report(fmt.Errorf("%w: Valkey Streams does not support exact per-message retry delay", messaging.ErrUnsupportedCapability))
		}
		if attempt >= s.driver.config.MaxDeliveries {
			err = delivery.deadLetter(result.Err)
		}
	case messaging.DispositionReject:
		err = delivery.Ack(s.ctx)
	case messaging.DispositionDeadLetter:
		err = delivery.deadLetter(result.Err)
	default:
		err = messaging.ErrUnsupportedDisposition
	}
	if err != nil {
		s.report(err)
	}
}

func (s *subscription) deliveryCount(id string) (int64, error) {
	result := s.client.Do(s.ctx, s.client.B().Xpending().Key(s.source.Name).Group(s.source.Group).Start(id).End(id).Count(1).Build())
	entries, err := result.ToArray()
	if err != nil {
		return 0, shared.Classify("xpending", err)
	}
	return parseDeliveryCount(id, entries)
}

func parseDeliveryCount(id string, entries []valkey.ValkeyMessage) (int64, error) {
	if len(entries) == 0 {
		return 0, fmt.Errorf("valkey streams: XPENDING omitted delivery metadata for %q", id)
	}
	values, err := entries[0].ToArray()
	if err != nil {
		return 0, fmt.Errorf("valkey streams: parse XPENDING delivery metadata: %w", err)
	}
	if len(values) < 4 {
		return 0, fmt.Errorf("valkey streams: malformed XPENDING delivery metadata")
	}
	count, err := values[3].AsInt64()
	if err != nil {
		return 0, fmt.Errorf("valkey streams: parse XPENDING delivery count: %w", err)
	}
	if count <= 0 {
		return 0, fmt.Errorf("valkey streams: invalid XPENDING delivery count %d", count)
	}
	return count, nil
}
func (s *subscription) ack(ctx context.Context, id string) error {
	_, err := s.client.Do(ctx, s.client.B().Xack().Key(s.source.Name).Group(s.source.Group).Id(id).Build()).AsInt64()
	if err != nil {
		return shared.Classify("xack", err)
	}
	return nil
}
func (s *subscription) deadLetter(ctx context.Context, id, raw string, reason error) error {
	message := safeDeadLetterReason(reason)
	destination := s.source.Name + s.driver.config.DeadLetterSuffix
	_, err := s.client.Do(ctx, s.client.B().Xadd().Key(destination).Id("*").FieldValue().FieldValue("original_stream", s.source.Name).FieldValue("original_id", id).FieldValue("reason", message).FieldValue(envelopeField, raw).Build()).ToString()
	if err != nil {
		return &messaging.TransportError{Class: messaging.ErrDeadLetter, Transport: "valkey.streams", Operation: "dead-letter", Cause: err}
	}
	return nil
}

func safeDeadLetterReason(reason error) string {
	for _, candidate := range []error{
		messaging.ErrInvalidEnvelope,
		messaging.ErrSchemaMismatch,
		messaging.ErrMessageTooLarge,
		messaging.ErrUnsupportedDisposition,
		messaging.ErrHandlerPanic,
		context.Canceled,
		context.DeadlineExceeded,
	} {
		if errors.Is(reason, candidate) {
			return candidate.Error()
		}
	}
	return "rejected"
}

type streamDelivery struct {
	messaging.BasicDelivery
	subscription *subscription
	settlement   streamSettlement
	id           string
	raw          string
	attempted    atomic.Bool
	settled      atomic.Bool
}

type streamSettlement interface {
	ack(context.Context, string) error
	deadLetter(context.Context, string, string, error) error
}

func (d *streamDelivery) settlementStore() streamSettlement {
	if d.settlement != nil {
		return d.settlement
	}
	return d.subscription
}

func (d *streamDelivery) Ack(ctx context.Context) error {
	d.attempted.Store(true)
	if !d.settled.CompareAndSwap(false, true) {
		return nil
	}
	if err := d.settlementStore().ack(ctx, d.id); err != nil {
		d.settled.Store(false)
		return err
	}
	return nil
}
func (d *streamDelivery) Nack(ctx context.Context, options messaging.NackOptions) error {
	switch options.Disposition {
	case messaging.DispositionRetry:
		return d.retry(ctx, options)
	case messaging.DispositionReject:
		return d.Ack(ctx)
	case messaging.DispositionDeadLetter:
		return d.deadLetterContext(ctx, errors.New(options.Reason))
	default:
		return messaging.ErrUnsupportedDisposition
	}
}

func (d *streamDelivery) retry(ctx context.Context, options messaging.NackOptions) error {
	d.attempted.Store(true)
	if !d.settled.CompareAndSwap(false, true) {
		return nil
	}
	var warning error
	if options.RetryAfter > 0 {
		warning = fmt.Errorf("%w: Valkey Streams does not support exact per-message retry delay", messaging.ErrUnsupportedCapability)
		d.subscription.report(warning)
	}
	if int64(d.Info().Attempt) < d.subscription.driver.config.MaxDeliveries {
		return warning
	}
	reason := error(nil)
	if strings.TrimSpace(options.Reason) != "" {
		reason = errors.New(options.Reason)
	}
	if err := d.deadLetterSettled(ctx, reason); err != nil {
		d.settled.Store(false)
		return errors.Join(warning, err)
	}
	return warning
}
func (d *streamDelivery) deadLetter(reason error) error {
	return d.deadLetterContext(d.subscription.ctx, reason)
}

func (d *streamDelivery) deadLetterContext(ctx context.Context, reason error) error {
	d.attempted.Store(true)
	if !d.settled.CompareAndSwap(false, true) {
		return nil
	}
	if err := d.deadLetterSettled(ctx, reason); err != nil {
		d.settled.Store(false)
		return err
	}
	return nil
}

func (d *streamDelivery) deadLetterSettled(ctx context.Context, reason error) error {
	if err := d.settlementStore().deadLetter(ctx, d.id, d.raw, reason); err != nil {
		return err
	}
	return d.settlementStore().ack(ctx, d.id)
}
