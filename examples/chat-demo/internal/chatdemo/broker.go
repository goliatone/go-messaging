package chatdemo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/transport/valkey/pubsub"
)

const DefaultChannel = "go-messaging.demo.chat"

// BrokerConfig contains provider details kept behind the logical chat route.
type BrokerConfig struct {
	ValkeyAddress string
	Channel       string
	QueueSize     int
	RouteTimeout  time.Duration
}

func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		ValkeyAddress: "127.0.0.1:6399",
		Channel:       DefaultChannel,
		QueueSize:     256,
		RouteTimeout:  5 * time.Second,
	}
}

func (c BrokerConfig) withDefaults() BrokerConfig {
	defaults := DefaultBrokerConfig()
	if strings.TrimSpace(c.ValkeyAddress) == "" {
		c.ValkeyAddress = defaults.ValkeyAddress
	}
	if strings.TrimSpace(c.Channel) == "" {
		c.Channel = defaults.Channel
	}
	if c.QueueSize == 0 {
		c.QueueSize = defaults.QueueSize
	}
	if c.RouteTimeout == 0 {
		c.RouteTimeout = defaults.RouteTimeout
	}
	return c
}

func (c BrokerConfig) validate() error {
	if strings.TrimSpace(c.ValkeyAddress) == "" || strings.TrimSpace(c.Channel) == "" {
		return errors.New("valkey address and channel are required")
	}
	if c.QueueSize <= 0 || c.RouteTimeout <= 0 {
		return errors.New("queue size and route timeout must be positive")
	}
	return nil
}

// Broker owns the shared Valkey driver, logical router, and ingress registry.
type Broker struct {
	driver   messaging.Driver
	registry *messaging.DriverRegistry
	router   *messaging.Router
	channel  string
	observer messaging.Observer
}

// NewBroker builds the Valkey Pub/Sub-backed chat messaging runtime.
func NewBroker(config BrokerConfig, observer messaging.Observer) (*Broker, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	driverConfig := pubsub.DefaultConfig(config.ValkeyAddress)
	driverConfig.QueueSize = config.QueueSize
	driverConfig.Valkey.MaxMessageBytes = MaxPayloadSize * 2
	driver, err := pubsub.New(driverConfig)
	if err != nil {
		return nil, safeError(messagingUnavailableMessage, err)
	}
	return newBroker(driver, config.Channel, config.RouteTimeout, observer)
}

func newBroker(driver messaging.Driver, channel string, routeTimeout time.Duration, observer messaging.Observer) (*Broker, error) {
	if driver == nil {
		return nil, errors.New("messaging driver is required")
	}
	registry, err := messaging.NewDriverRegistry(map[string]messaging.Driver{ChatDriverName: driver})
	if err != nil {
		return nil, err
	}
	router, err := messaging.NewRouter(registry, []messaging.Route{{
		Name: ChatRoute, Strategy: messaging.StrategyPrimary,
		Bindings: []messaging.RouteBinding{{Driver: ChatDriverName, Destination: messaging.Destination{Name: channel}}},
		Kinds:    []messaging.Kind{messaging.KindEvent}, Types: []string{ChatEventType},
		Required: []messaging.Capability{messaging.CapabilityFanout},
		Timeout:  routeTimeout, MaxMessageBytes: MaxPayloadSize,
	}}, observer)
	if err != nil {
		return nil, fmt.Errorf("configure chat route: %w", err)
	}
	return &Broker{driver: driver, registry: registry, router: router, channel: channel, observer: observer}, nil
}

func (b *Broker) Start(ctx context.Context) error {
	if b == nil || b.driver == nil {
		return errors.New("messaging broker is not configured")
	}
	if err := b.driver.Start(ctx); err != nil {
		return safeError(messagingUnavailableMessage, err)
	}
	return nil
}

func (b *Broker) Ready() <-chan struct{} {
	if b == nil || b.driver == nil {
		return nil
	}
	return b.driver.Ready()
}

func (b *Broker) Errors() <-chan error {
	if b == nil || b.driver == nil {
		return nil
	}
	return b.driver.Errors()
}

func (b *Broker) Close(ctx context.Context) error {
	if b == nil || b.driver == nil {
		return nil
	}
	return safeError(shutdownFailedMessage, b.driver.Close(ctx))
}

// Publish validates a chat message and sends it through the logical chat route.
func (b *Broker) Publish(ctx context.Context, message ChatMessage) (messaging.Envelope, messaging.RoutingResult, error) {
	if b == nil || b.router == nil {
		return messaging.Envelope{}, messaging.RoutingResult{}, errors.New("messaging broker is not configured")
	}
	payload, err := encodeChatPayload(message)
	if err != nil {
		return messaging.Envelope{}, messaging.RoutingResult{}, err
	}
	id, err := newEnvelopeID()
	if err != nil {
		return messaging.Envelope{}, messaging.RoutingResult{}, err
	}
	envelope := messaging.NewEnvelope(id, ChatEventType, messaging.KindEvent, ChatSchema, ChatContentType, payload, nil)
	result, err := b.router.Publish(ctx, ChatRoute, envelope)
	return envelope, result, safeError(publishFailedMessage, err)
}

// NewIngress creates the single strict chat binding used by server and CLI readers.
func (b *Broker) NewIngress(handler messaging.Handler) (*messaging.Ingress, error) {
	if b == nil || b.registry == nil {
		return nil, errors.New("messaging broker is not configured")
	}
	if handler == nil {
		return nil, errors.New("chat ingress handler is required")
	}
	return messaging.NewIngress(b.registry, []messaging.IngressBinding{{
		Name: "chat-messages", LogicalRoute: ChatRoute, Driver: ChatDriverName,
		Source:        messaging.Source{Name: b.channel},
		AcceptedKinds: []messaging.Kind{messaging.KindEvent}, AcceptedTypes: []string{ChatEventType},
		AcceptedContentTypes: []string{ChatContentType}, AcceptedSchemas: []string{ChatSchema},
		Handlers: []messaging.Handler{handler}, RequiredCapabilities: []messaging.Capability{messaging.CapabilityFanout},
	}}, messaging.WithIngressObserver(b.observer))
}

func newEnvelopeID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate envelope ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
