package chatdemo

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/examples/chat-demo/web"
	"github.com/goliatone/go-router"
)

type servingHTTP interface {
	Serve(string) error
	Shutdown(context.Context) error
}

// ServerConfig controls the browser edge while provider details remain in BrokerConfig.
type ServerConfig struct {
	ListenAddress    string
	AllowedOrigins   []string
	MaxMessageSize   int64
	BroadcastTimeout time.Duration
	ShutdownTimeout  time.Duration
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		ListenAddress: ":8989",
		AllowedOrigins: []string{
			"http://localhost:8989",
			"http://127.0.0.1:8989",
		},
		MaxMessageSize:   MaxPayloadSize,
		BroadcastTimeout: 3 * time.Second,
		ShutdownTimeout:  5 * time.Second,
	}
}

func (c ServerConfig) withDefaults() ServerConfig {
	defaults := DefaultServerConfig()
	if strings.TrimSpace(c.ListenAddress) == "" {
		c.ListenAddress = defaults.ListenAddress
	}
	if len(c.AllowedOrigins) == 0 {
		c.AllowedOrigins = append([]string(nil), defaults.AllowedOrigins...)
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = defaults.MaxMessageSize
	}
	if c.BroadcastTimeout == 0 {
		c.BroadcastTimeout = defaults.BroadcastTimeout
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = defaults.ShutdownTimeout
	}
	return c
}

func (c ServerConfig) validate() error {
	if strings.TrimSpace(c.ListenAddress) == "" || len(c.AllowedOrigins) == 0 {
		return errors.New("listen address and at least one allowed origin are required")
	}
	for _, origin := range c.AllowedOrigins {
		if strings.TrimSpace(origin) == "" || origin == "*" {
			return errors.New("allowed origins must be explicit non-empty URLs")
		}
	}
	if c.MaxMessageSize < 1024 || c.BroadcastTimeout <= 0 || c.ShutdownTimeout <= 0 {
		return errors.New("server size and timeout limits must be positive")
	}
	return nil
}

// ChatServer joins the go-router browser edge to the shared messaging broker.
type ChatServer struct {
	config         ServerConfig
	broker         *Broker
	hub            *router.WSHub
	http           servingHTTP
	closeHub       func() error
	handler        http.Handler
	subscriptions  []messaging.Subscription
	serverReady    atomic.Bool
	messagingReady atomic.Bool
}

func NewChatServer(config ServerConfig, broker *Broker) (*ChatServer, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	if broker == nil {
		return nil, errors.New("chat broker is required")
	}
	hub := router.NewWSHub(func(hubConfig *router.WSHubConfig) {
		hubConfig.MaxMessageSize = config.MaxMessageSize
	})
	server := &ChatServer{config: config, broker: broker, hub: hub, closeHub: hub.Close}
	if err := hub.OnConnect(func(_ context.Context, client router.WSClient, _ any) error {
		return registerChatClient(broker, client)
	}); err != nil {
		return nil, err
	}
	adapter := router.NewHTTPServer()
	routes := adapter.Router()
	routes.Get("/", server.indexHandler)
	routes.Get("/api/health", server.healthHandler)
	wsConfig := router.DefaultWebSocketConfig()
	wsConfig.Origins = append([]string(nil), config.AllowedOrigins...)
	wsConfig.MaxMessageSize = config.MaxMessageSize
	if wsConfig.WriteTimeout > config.ShutdownTimeout {
		wsConfig.WriteTimeout = config.ShutdownTimeout
	}
	routes.Get("/ws/chat", hub.Handler(), router.WebSocketUpgrade(wsConfig))
	routes.Static("/assets", "", router.Static{FS: web.Assets})
	server.http = adapter
	server.handler = adapter.WrappedRouter()
	return server, nil
}

func (s *ChatServer) indexHandler(ctx router.Context) error {
	content, err := web.Assets.ReadFile("index.html")
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).SendString("chat UI is unavailable")
	}
	ctx.SetHeader("Content-Type", "text/html; charset=utf-8")
	return ctx.Send(content)
}

func (s *ChatServer) healthHandler(ctx router.Context) error {
	ready := s.serverReady.Load() && s.messagingReady.Load()
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	return ctx.JSON(status, map[string]any{
		"ready":             ready,
		"server_ready":      s.serverReady.Load(),
		"messaging_ready":   s.messagingReady.Load(),
		"websocket_clients": s.hub.ClientCount(),
		"transport":         "valkey.pubsub",
	})
}

func (s *ChatServer) ingressHandler(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
	view, err := DecodeChatDelivery(delivery)
	if err != nil {
		return messaging.Reject(err)
	}
	// WSHub queues the broadcast and sends to clients asynchronously. Keep the
	// bounded context alive after enqueueing so every client send gets the same
	// delivery window instead of racing an immediate handler-return cancellation.
	broadcastCtx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(s.config.BroadcastTimeout, cancel)
	if err := s.hub.BroadcastJSONWithContext(broadcastCtx, wsEvent{Type: EventMessage, Data: view}); err != nil {
		if timer.Stop() {
			cancel()
		}
		return messaging.Reject(errors.New("WebSocket broadcast failed"))
	}
	return messaging.Complete()
}

// Serve starts messaging before accepting browser traffic and blocks until shutdown.
func (s *ChatServer) Serve(ctx context.Context) (runErr error) {
	runCtx, cancelRun := context.WithCancel(ctx)
	var monitorDone <-chan struct{}
	defer func() {
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancelShutdown()
		runErr = errors.Join(runErr, s.shutdown(shutdownCtx))
		if monitorDone != nil {
			select {
			case <-monitorDone:
			case <-shutdownCtx.Done():
				runErr = errors.Join(runErr, safeError(shutdownFailedMessage, shutdownCtx.Err()))
			}
		}
	}()
	if err := s.broker.Start(runCtx); err != nil {
		return err
	}
	ingress, err := s.broker.NewIngress(s.ingressHandler)
	if err != nil {
		return err
	}
	s.subscriptions, err = ingress.Subscribe(runCtx)
	if err != nil {
		return safeError(messagingUnavailableMessage, err)
	}
	if err := waitForSubscriptions(runCtx, s.subscriptions); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	s.messagingReady.Store(true)
	s.serverReady.Store(true)
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- s.http.Serve(s.config.ListenAddress) }()
	messagingErrors := make(chan error, 1)
	done := make(chan struct{})
	monitorDone = done
	go func() {
		defer close(done)
		messagingErrors <- monitorMessaging(runCtx, s.broker.Errors(), s.subscriptions)
	}()
	select {
	case <-runCtx.Done():
		return nil
	case err := <-messagingErrors:
		return err
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	}
}

func (s *ChatServer) shutdown(ctx context.Context) error {
	s.serverReady.Store(false)
	s.messagingReady.Store(false)
	var joined error
	joined = errors.Join(joined, runCleanup(ctx, func() error { return s.http.Shutdown(ctx) }))
	joined = errors.Join(joined, runCleanup(ctx, s.closeHub))
	for _, subscription := range s.subscriptions {
		current := subscription
		joined = errors.Join(joined, runCleanup(ctx, func() error { return current.Close(ctx) }))
	}
	joined = errors.Join(joined, runCleanup(ctx, func() error { return s.broker.Close(ctx) }))
	return safeError(shutdownFailedMessage, joined)
}

func runCleanup(ctx context.Context, operation func() error) error {
	if operation == nil {
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- operation() }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func runServe(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	brokerDefaults := DefaultBrokerConfig()
	serverDefaults := DefaultServerConfig()
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", serverDefaults.ListenAddress, "HTTP listen address")
	valkeyAddress := flags.String("valkey", brokerDefaults.ValkeyAddress, "Valkey address")
	channel := flags.String("channel", brokerDefaults.Channel, "Valkey Pub/Sub channel")
	origins := flags.String("origins", "", "comma-separated allowed WebSocket origins (defaults to the local listen port)")
	maxMessageSize := flags.Int64("max-message-bytes", serverDefaults.MaxMessageSize, "maximum WebSocket message size")
	shutdownTimeout := flags.Duration("shutdown-timeout", serverDefaults.ShutdownTimeout, "bounded shutdown timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("serve does not accept positional arguments")
	}
	broker, err := NewBroker(BrokerConfig{ValkeyAddress: *valkeyAddress, Channel: *channel}, newObserver(stderr))
	if err != nil {
		return err
	}
	allowedOrigins := splitNonEmpty(*origins)
	if len(allowedOrigins) == 0 {
		allowedOrigins = localOriginsForListen(*listen)
	}
	server, err := NewChatServer(ServerConfig{
		ListenAddress: *listen, AllowedOrigins: allowedOrigins,
		MaxMessageSize: *maxMessageSize, ShutdownTimeout: *shutdownTimeout,
	}, broker)
	if err != nil {
		return err
	}
	if _, writeErr := fmt.Fprintf(stdout, "chat demo listening on %s (live Pub/Sub; no replay)\n", displayURL(*listen)); writeErr != nil {
		return writeErr
	}
	return server.Serve(ctx)
}

func localOriginsForListen(address string) []string {
	_, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return append([]string(nil), DefaultServerConfig().AllowedOrigins...)
	}
	return []string{"http://localhost:" + port, "http://127.0.0.1:" + port}
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func displayURL(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return "http://localhost:8989"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}
