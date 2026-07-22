package valkey

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	admin "github.com/goliatone/go-admin/pkg/admin"
	goadmin "github.com/goliatone/go-messaging/adapters/go-admin"
)

const dockerIntegrationEnvironment = "GOADMIN_VALKEY_DOCKER_TEST"

func TestDockerValkeyWorkerGatewayFanoutAndRecovery(t *testing.T) {
	if os.Getenv(dockerIntegrationEnvironment) == "" {
		t.Skip(dockerIntegrationEnvironment + " is required")
	}
	broker := startDockerValkey(t)

	worker := startIntegrationComponents(t, broker.address, admin.CommandRunRolePublisher)
	firstGateway := startIntegrationComponents(t, broker.address, admin.CommandRunRoleGateway)
	secondGateway := startIntegrationComponents(t, broker.address, admin.CommandRunRoleGateway)
	hybrid := startIntegrationComponents(t, broker.address, admin.CommandRunRoleMonolith)

	store, err := admin.NewMemoryCommandRunStore(admin.CommandRunMemoryStoreConfig{})
	if err != nil {
		t.Fatalf("memory store: %v", err)
	}
	firstReceived := make(chan projectedDelivery, 8)
	secondReceived := make(chan admin.CommandRunUpdate, 8)
	hybridReceived := make(chan admin.CommandRunUpdate, 8)
	firstSub := subscribeIntegration(t, firstGateway, func(ctx context.Context, update admin.CommandRunUpdate) error {
		_, changed, applyErr := store.Apply(ctx, update)
		firstReceived <- projectedDelivery{update: update, changed: changed}
		return applyErr
	})
	secondSub := subscribeIntegration(t, secondGateway, channelUpdateHandler(secondReceived))
	hybridSub := subscribeIntegration(t, hybrid, channelUpdateHandler(hybridReceived))

	initial := validAssemblyUpdate("worker-fanout", 1)
	if err := worker.Transport.PublishCommandRun(context.Background(), initial); err != nil {
		t.Fatalf("worker publish: %v", err)
	}
	if delivery := awaitProjected(t, firstReceived); delivery.update.RunID != initial.RunID || !delivery.changed {
		t.Fatalf("first gateway delivery = %+v", delivery)
	}
	if got := awaitIntegrationUpdate(t, secondReceived); got.RunID != initial.RunID {
		t.Fatalf("second gateway run = %q", got.RunID)
	}
	if got := awaitIntegrationUpdate(t, hybridReceived); got.RunID != initial.RunID {
		t.Fatalf("hybrid gateway run = %q", got.RunID)
	}

	if err := worker.Transport.PublishCommandRun(context.Background(), initial); err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	if delivery := awaitProjected(t, firstReceived); delivery.update.EventID != initial.EventID || delivery.changed {
		t.Fatalf("duplicate projection = %+v", delivery)
	}
	_ = awaitIntegrationUpdate(t, secondReceived)
	_ = awaitIntegrationUpdate(t, hybridReceived)

	hybridUpdate := validAssemblyUpdate("hybrid-publish", 1)
	if err := hybrid.Transport.PublishCommandRun(context.Background(), hybridUpdate); err != nil {
		t.Fatalf("hybrid publish: %v", err)
	}
	_ = awaitProjected(t, firstReceived)
	_ = awaitIntegrationUpdate(t, secondReceived)
	_ = awaitIntegrationUpdate(t, hybridReceived)

	broker.stop(t)
	awaitAnySubscriptionFailure(t, firstSub, secondSub, hybridSub)
	lossStarted := time.Now()
	lossCtx, lossCancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	lossErr := worker.Transport.PublishCommandRun(lossCtx, validAssemblyUpdate("broker-loss", 1))
	lossCancel()
	if !errors.Is(lossErr, goadmin.ErrPublishFailed) {
		t.Fatalf("publish during broker loss error = %v", lossErr)
	}
	if time.Since(lossStarted) > time.Second {
		t.Fatal("publish during broker loss exceeded its bound")
	}

	broker.start(t)
	// Pub/Sub reconnects asynchronously; allow each independent gateway
	// subscription to re-enter its channel before sending the recovery probe.
	time.Sleep(750 * time.Millisecond)
	recovered := validAssemblyUpdate("broker-recovered", 1)
	retryUntil(t, 10*time.Second, func() bool {
		return worker.Transport.PublishCommandRun(context.Background(), recovered) == nil
	}, "publisher did not recover after broker restart")
	if delivery := awaitProjected(t, firstReceived); delivery.update.RunID != recovered.RunID {
		t.Fatalf("first gateway recovery run = %q", delivery.update.RunID)
	}
	if got := awaitIntegrationUpdate(t, secondReceived); got.RunID != recovered.RunID {
		t.Fatalf("second gateway recovery run = %q", got.RunID)
	}
	if got := awaitIntegrationUpdate(t, hybridReceived); got.RunID != recovered.RunID {
		t.Fatalf("hybrid gateway recovery run = %q", got.RunID)
	}

	closeIntegrationSubscription(t, firstSub)
	closeIntegrationSubscription(t, secondSub)
	closeIntegrationSubscription(t, hybridSub)
	closeIntegrationComponents(t, worker, firstGateway, secondGateway, hybrid)
}

type projectedDelivery struct {
	update  admin.CommandRunUpdate
	changed bool
}

type dockerValkey struct {
	name    string
	address string
	mu      sync.Mutex
	running bool
}

func startDockerValkey(t *testing.T) *dockerValkey {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	broker := &dockerValkey{
		name:    fmt.Sprintf("go-admin-valkey-%d-%d", os.Getpid(), time.Now().UnixNano()),
		address: fmt.Sprintf("127.0.0.1:%d", port),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "run", "-d", "--name", broker.name,
		"-p", fmt.Sprintf("127.0.0.1:%d:6379", port), "valkey/valkey:8-alpine").CombinedOutput()
	if err != nil {
		t.Fatalf("start docker Valkey: %v: %s", err, strings.TrimSpace(string(output)))
	}
	broker.running = true
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "docker", "rm", "-f", broker.name).Run()
	})
	waitForTCP(t, broker.address, 15*time.Second)
	return broker
}

func (b *dockerValkey) stop(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.running {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "docker", "stop", "-t", "1", b.name).CombinedOutput(); err != nil {
		t.Fatalf("stop docker Valkey: %v: %s", err, strings.TrimSpace(string(output)))
	}
	b.running = false
}

func (b *dockerValkey) start(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "docker", "start", b.name).CombinedOutput(); err != nil {
		t.Fatalf("restart docker Valkey: %v: %s", err, strings.TrimSpace(string(output)))
	}
	b.running = true
	waitForTCP(t, b.address, 10*time.Second)
}

func startIntegrationComponents(t *testing.T, address string, role admin.CommandRunProcessRole) *Components {
	t.Helper()
	components, err := New(Config{
		Addresses: []string{address}, ApplicationID: "app", EnvironmentID: "test", Role: role,
		ConnectTimeout: 500 * time.Millisecond, OperationTimeout: 750 * time.Millisecond,
		ReconnectMin: 50 * time.Millisecond, ReconnectMax: 250 * time.Millisecond,
		PublishTimeout: 750 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new %s components: %v", role, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var startErr error
	for ctx.Err() == nil {
		startErr = components.StartDriver(ctx)
		if startErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if startErr != nil {
		t.Fatalf("start %s driver: %v", role, startErr)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		_ = components.CloseDriver(closeCtx)
	})
	return components
}

func subscribeIntegration(t *testing.T, components *Components, handler admin.CommandRunHandler) admin.CommandRunSubscription {
	t.Helper()
	subscription, err := components.Transport.SubscribeCommandRuns(context.Background(), admin.CommandRunSelector{Global: true}, handler)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-subscription.Ready():
	case err := <-subscription.Errors():
		t.Fatalf("subscription failed before ready: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not become ready")
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		_ = subscription.Close(closeCtx)
	})
	return subscription
}

func channelUpdateHandler(target chan<- admin.CommandRunUpdate) admin.CommandRunHandler {
	return func(_ context.Context, update admin.CommandRunUpdate) error {
		target <- update
		return nil
	}
}

func awaitProjected(t *testing.T, source <-chan projectedDelivery) projectedDelivery {
	t.Helper()
	select {
	case delivery := <-source:
		return delivery
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for projected delivery")
		return projectedDelivery{}
	}
}

func awaitIntegrationUpdate(t *testing.T, source <-chan admin.CommandRunUpdate) admin.CommandRunUpdate {
	t.Helper()
	select {
	case update := <-source:
		return update
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for update")
		return admin.CommandRunUpdate{}
	}
}

func awaitAnySubscriptionFailure(t *testing.T, subscriptions ...admin.CommandRunSubscription) {
	t.Helper()
	reported := make(chan error, len(subscriptions))
	for _, subscription := range subscriptions {
		go func(sub admin.CommandRunSubscription) {
			select {
			case err, ok := <-sub.Errors():
				if ok {
					reported <- err
				}
			case <-time.After(2 * time.Second):
			}
		}(subscription)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, goadmin.ErrSubscriptionFailed) {
			t.Fatalf("subscription error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("broker loss was not reported by any subscription")
	}
}

func retryUntil(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal(message)
}

func closeIntegrationSubscription(t *testing.T, subscription admin.CommandRunSubscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := subscription.Close(ctx); err != nil {
		t.Fatalf("close subscription: %v", err)
	}
}

func closeIntegrationComponents(t *testing.T, components ...*Components) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, component := range components {
		if err := component.CloseDriver(ctx); err != nil {
			t.Fatalf("close driver: %v", err)
		}
	}
}

func waitForTCP(t *testing.T, address string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Valkey did not listen on %s", address)
}
