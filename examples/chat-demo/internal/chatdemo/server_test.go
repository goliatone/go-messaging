package chatdemo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type testHTTPServer struct {
	serveErr    error
	shutdownErr error
}

func (s *testHTTPServer) Serve(string) error             { return s.serveErr }
func (s *testHTTPServer) Shutdown(context.Context) error { return s.shutdownErr }

func TestHealthAndEmbeddedStaticRoutes(t *testing.T) {
	broker, err := newBroker(newTestDriver(), "chat", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewChatServer(DefaultServerConfig(), broker)
	if err != nil {
		t.Fatal(err)
	}

	health := httptest.NewRecorder()
	server.handler.ServeHTTP(health, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/health", nil))
	if health.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status %d", health.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(health.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["ready"] != false || body["transport"] != "valkey.pubsub" {
		t.Fatalf("health body %#v", body)
	}

	index := httptest.NewRecorder()
	server.handler.ServeHTTP(index, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	if index.Code != http.StatusOK {
		t.Fatalf("index status %d: %s", index.Code, index.Body.String())
	}

	asset := httptest.NewRecorder()
	server.handler.ServeHTTP(asset, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/assets/app.js", nil))
	if asset.Code != http.StatusOK {
		t.Fatalf("asset status %d: %s", asset.Code, asset.Body.String())
	}
}

func TestServerRejectsWildcardOrigin(t *testing.T) {
	broker, err := newBroker(newTestDriver(), "chat", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewChatServer(ServerConfig{AllowedOrigins: []string{"*"}}, broker); err == nil {
		t.Fatal("wildcard origin was accepted")
	}
}

func TestLocalOriginsFollowListenPort(t *testing.T) {
	origins := localOriginsForListen(":9090")
	if len(origins) != 2 || origins[0] != "http://localhost:9090" || origins[1] != "http://127.0.0.1:9090" {
		t.Fatalf("origins %#v", origins)
	}
}

func TestDisplayURLUsesReachableLocalHost(t *testing.T) {
	if got := displayURL(":9090"); got != "http://localhost:9090" {
		t.Fatalf("displayURL = %q", got)
	}
	if got := displayURL("127.0.0.1:8081"); got != "http://127.0.0.1:8081" {
		t.Fatalf("displayURL = %q", got)
	}
}

func TestWebSocketReturnsBoundedErrorsForMalformedAndUnsupportedFrames(t *testing.T) {
	broker, err := newBroker(newTestDriver(), "chat", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	const origin = "http://example.test"
	server, err := NewChatServer(ServerConfig{AllowedOrigins: []string{origin}}, broker)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.handler)
	t.Cleanup(httpServer.Close)

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/chat"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Origin": []string{origin}})
	if err != nil {
		if response != nil {
			t.Fatalf("dial WebSocket: %v (status %s)", err, response.Status)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	waitForWebSocketEvents(t, conn, EventReady)

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{`)); err != nil {
		t.Fatal(err)
	}
	assertWebSocketErrorCode(t, conn, "invalid_frame")
	if err := conn.WriteJSON(wsEvent{Type: "chat.delete", Data: map[string]string{"id": "ignored"}}); err != nil {
		t.Fatal(err)
	}
	assertWebSocketErrorCode(t, conn, "unsupported_event")
}

func assertWebSocketErrorCode(t *testing.T, conn *websocket.Conn, expected string) {
	t.Helper()
	frames := waitForWebSocketEvents(t, conn, EventError)
	var event errorEvent
	if err := json.Unmarshal(frames[EventError], &event); err != nil {
		t.Fatal(err)
	}
	if event.Code != expected || event.Message == "" || len(event.Message) > 128 {
		t.Fatalf("error event = %#v, want bounded code %q", event, expected)
	}
}

func TestServeHTTPFailureCancelsAndJoinsMessagingMonitor(t *testing.T) {
	broker, err := newBroker(newTestDriver(), "chat", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewChatServer(DefaultServerConfig(), broker)
	if err != nil {
		t.Fatal(err)
	}
	server.http = &testHTTPServer{serveErr: errors.New("listen failed")}
	started := time.Now()
	err = server.Serve(context.Background())
	if err == nil || !strings.Contains(err.Error(), "serve HTTP") {
		t.Fatalf("serve error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Serve took %s after HTTP failure", elapsed)
	}
}

func TestShutdownBoundsSlowWebSocketHub(t *testing.T) {
	broker, err := newBroker(newTestDriver(), "chat", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewChatServer(DefaultServerConfig(), broker)
	if err != nil {
		t.Fatal(err)
	}
	server.http = &testHTTPServer{}
	release := make(chan struct{})
	server.closeHub = func() error {
		<-release
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = server.shutdown(ctx)
	close(release)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown exceeded its bound: %s", elapsed)
	}
}
