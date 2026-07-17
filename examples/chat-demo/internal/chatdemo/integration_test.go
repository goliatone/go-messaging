package chatdemo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/gorilla/websocket"
	valkey "github.com/valkey-io/valkey-go"
)

//nolint:gocyclo,funlen // One ordered scenario proves browser, CLI, and direct-ingress paths share delivery IDs.
func TestRealValkeyBrowserCLIAndDirectReaderFlows(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("VALKEY_ADDRESS is required in CI")
		}
		t.Skip("VALKEY_ADDRESS is required for real-Valkey integration coverage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	listenAddress := reserveListenAddress(t)
	channel := fmt.Sprintf("go-messaging.demo.chat.test.%d", time.Now().UnixNano())
	origin := "http://" + listenAddress

	serverBroker, err := NewBroker(BrokerConfig{ValkeyAddress: address, Channel: channel}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var serverDiagnostics lockedBuffer
	server, err := NewChatServer(ServerConfig{
		ListenAddress: listenAddress, AllowedOrigins: []string{origin},
		ShutdownTimeout: 3 * time.Second, DiagnosticWriter: &serverDiagnostics,
	}, serverBroker)
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(ctx) }()
	waitForHealthyServer(t, ctx, "http://"+listenAddress+"/api/health", serveErrors)

	first := dialChatWebSocket(t, ctx, listenAddress, origin)
	t.Cleanup(func() {
		if closeErr := first.Close(); closeErr != nil {
			t.Logf("close first WebSocket: %v", closeErr)
		}
	})
	second := dialChatWebSocket(t, ctx, listenAddress, origin)
	t.Cleanup(func() {
		if closeErr := second.Close(); closeErr != nil {
			t.Logf("close second WebSocket: %v", closeErr)
		}
	})
	waitForWebSocketEvents(t, first, EventReady)
	waitForWebSocketEvents(t, second, EventReady)

	readerBroker, err := NewBroker(BrokerConfig{ValkeyAddress: address, Channel: channel}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if startErr := readerBroker.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	views := make(chan ChatMessageView, 4)
	ingress, err := readerBroker.NewIngress(func(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
		view, decodeErr := DecodeChatDelivery(delivery)
		if decodeErr != nil {
			return messaging.Reject(decodeErr)
		}
		views <- view
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	subscriptions, err := ingress.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if waitErr := waitForSubscriptions(ctx, subscriptions); waitErr != nil {
		t.Fatal(waitErr)
	}
	t.Cleanup(func() {
		if closeErr := closeSubscriptions(subscriptions); closeErr != nil {
			t.Errorf("close reader subscriptions: %v", closeErr)
		}
		if closeErr := closeBroker(readerBroker); closeErr != nil {
			t.Errorf("close reader broker: %v", closeErr)
		}
	})
	client := newIntegrationValkeyClient(t, address)
	publishRawIntegrationMessage(t, ctx, client, channel, "not-json")
	wrong := messaging.NewEnvelope("wrong-contract", "other.type", messaging.KindEvent, ChatSchema, ChatContentType, []byte(`{}`), nil)
	wrongWire, err := messaging.NewJSONCodec().Encode(ctx, wrong)
	if err != nil {
		t.Fatal(err)
	}
	publishRawIntegrationMessage(t, ctx, client, channel, string(wrongWire))
	waitForIntegrationCondition(t, ctx, func() bool {
		return strings.Count(serverDiagnostics.String(), messageRejectedMessage) >= 2
	}, "server did not report rejected broker messages")
	select {
	case err := <-serveErrors:
		t.Fatalf("server exited after rejected broker message: %v", err)
	default:
	}

	if err := first.WriteJSON(wsEvent{Type: EventSend, Data: map[string]string{"sender": "browser", "text": "from browser"}}); err != nil {
		t.Fatal(err)
	}
	firstFrames := waitForWebSocketEvents(t, first, EventAccepted, EventMessage)
	secondFrames := waitForWebSocketEvents(t, second, EventMessage)
	browserView := nextIntegrationView(t, views, "from browser")
	if delivered := decodeFrameView(t, firstFrames[EventMessage]); delivered.ID != browserView.ID {
		t.Fatalf("first browser ID %q, direct reader ID %q", delivered.ID, browserView.ID)
	}
	if delivered := decodeFrameView(t, secondFrames[EventMessage]); delivered.ID != browserView.ID {
		t.Fatalf("second browser ID %q, direct reader ID %q", delivered.ID, browserView.ID)
	}
	if closeErr := second.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reconnected := dialChatWebSocket(t, ctx, listenAddress, origin)
	t.Cleanup(func() {
		if closeErr := reconnected.Close(); closeErr != nil {
			t.Logf("close reconnected WebSocket: %v", closeErr)
		}
	})
	waitForWebSocketEvents(t, reconnected, EventReady)

	var sendOutput, sendErrors bytes.Buffer
	if err := runSend(ctx, []string{"--sender", "cli", "--valkey", address, "--channel", channel, "from cli"}, &sendOutput, &sendErrors); err != nil {
		t.Fatalf("runSend: %v\nstderr: %s", err, sendErrors.String())
	}
	if !strings.Contains(sendOutput.String(), "route="+ChatRoute) || !strings.Contains(sendOutput.String(), "outcome=accepted") {
		t.Fatalf("unexpected send output %q", sendOutput.String())
	}
	cliFirst := decodeFrameView(t, waitForWebSocketEvents(t, first, EventMessage)[EventMessage])
	cliSecond := decodeFrameView(t, waitForWebSocketEvents(t, reconnected, EventMessage)[EventMessage])
	cliView := nextIntegrationView(t, views, "from cli")
	if cliFirst.ID != cliView.ID || cliSecond.ID != cliView.ID {
		t.Fatalf("CLI IDs first=%q second=%q reader=%q", cliFirst.ID, cliSecond.ID, cliView.ID)
	}

	cancel()
	select {
	case err := <-serveErrors:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestRealValkeyCLIReaderSurvivesRejectedMessages(t *testing.T) {
	address := os.Getenv("VALKEY_ADDRESS")
	if address == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("VALKEY_ADDRESS is required in CI")
		}
		t.Skip("VALKEY_ADDRESS is required for real-Valkey integration coverage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	readerCtx, stopReader := context.WithCancel(ctx)
	defer stopReader()
	channel := fmt.Sprintf("go-messaging.demo.chat.reader.%d", time.Now().UnixNano())
	var stdout, stderr lockedBuffer
	readerResult := make(chan error, 1)
	go func() {
		readerResult <- runRead(readerCtx, []string{"--valkey", address, "--channel", channel}, &stdout, &stderr)
	}()
	waitForIntegrationCondition(t, ctx, func() bool {
		return strings.Contains(stderr.String(), "reading live messages")
	}, "CLI reader did not become ready")
	client := newIntegrationValkeyClient(t, address)
	publishRawIntegrationMessage(t, ctx, client, channel, "not-json")
	wrong := messaging.NewEnvelope("wrong-reader-contract", "other.type", messaging.KindEvent, ChatSchema, ChatContentType, []byte(`{}`), nil)
	wrongWire, err := messaging.NewJSONCodec().Encode(ctx, wrong)
	if err != nil {
		t.Fatal(err)
	}
	publishRawIntegrationMessage(t, ctx, client, channel, string(wrongWire))
	waitForIntegrationCondition(t, ctx, func() bool {
		return strings.Count(stderr.String(), messageRejectedMessage) >= 2
	}, "CLI reader did not report rejected broker messages")
	select {
	case err := <-readerResult:
		t.Fatalf("CLI reader exited after rejected broker message: %v", err)
	default:
	}
	var sendOutput, sendErrors lockedBuffer
	if err := runSend(ctx, []string{"--sender", "integration", "--valkey", address, "--channel", channel, "still alive"}, &sendOutput, &sendErrors); err != nil {
		t.Fatalf("runSend: %v stderr=%s", err, sendErrors.String())
	}
	waitForIntegrationCondition(t, ctx, func() bool {
		return strings.Contains(stdout.String(), "still alive")
	}, "CLI reader did not receive a valid message after rejections")
	stopReader()
	select {
	case err := <-readerResult:
		if err != nil {
			t.Fatalf("CLI reader shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CLI reader did not shut down")
	}
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func newIntegrationValkeyClient(t *testing.T, address string) valkey.Client {
	t.Helper()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{address}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func publishRawIntegrationMessage(t *testing.T, ctx context.Context, client valkey.Client, channel, message string) {
	t.Helper()
	if err := client.Do(ctx, client.B().Publish().Channel(channel).Message(message).Build()).Error(); err != nil {
		t.Fatal(err)
	}
}

func waitForIntegrationCondition(t *testing.T, ctx context.Context, condition func() bool, failure string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(failure)
		}
	}
}

func reserveListenAddress(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForHealthyServer(t *testing.T, ctx context.Context, url string, serveErrors <-chan error) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case err := <-serveErrors:
			t.Fatalf("chat server exited before health: %v", err)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("chat server did not become healthy")
		}
	}
}

func dialChatWebSocket(t *testing.T, ctx context.Context, address, origin string) *websocket.Conn {
	t.Helper()
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, "ws://"+address+"/ws/chat", http.Header{"Origin": []string{origin}})
	if response != nil {
		t.Cleanup(func() {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Logf("close WebSocket response body: %v", closeErr)
			}
		})
	}
	if err != nil {
		if response != nil {
			t.Fatalf("dial WebSocket: %v (status %s)", err, response.Status)
		}
		t.Fatal(err)
	}
	return conn
}

func waitForWebSocketEvents(t *testing.T, conn *websocket.Conn, eventTypes ...string) map[string]json.RawMessage {
	t.Helper()
	wanted := make(map[string]struct{}, len(eventTypes))
	for _, eventType := range eventTypes {
		wanted[eventType] = struct{}{}
	}
	found := make(map[string]json.RawMessage, len(eventTypes))
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for len(found) < len(wanted) {
		var frame struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read WebSocket events %v: %v", eventTypes, err)
		}
		if _, ok := wanted[frame.Type]; ok {
			found[frame.Type] = frame.Data
		}
	}
	return found
}

func decodeFrameView(t *testing.T, data json.RawMessage) ChatMessageView {
	t.Helper()
	var view ChatMessageView
	if err := json.Unmarshal(data, &view); err != nil {
		t.Fatal(err)
	}
	return view
}

func nextIntegrationView(t *testing.T, views <-chan ChatMessageView, text string) ChatMessageView {
	t.Helper()
	select {
	case view := <-views:
		if view.Text != text {
			t.Fatalf("view text %q, want %q", view.Text, text)
		}
		return view
	case <-time.After(5 * time.Second):
		t.Fatalf("direct reader did not receive %q", text)
		return ChatMessageView{}
	}
}
