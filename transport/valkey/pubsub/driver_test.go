package pubsub

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/internal/contracttest"
	valkey "github.com/valkey-io/valkey-go"
)

func TestPubSubContract(t *testing.T) {
	address := requireValkeyAddress(t)
	contracttest.Run(t, func(t *testing.T) (contracttest.DuplexDriver, messaging.Destination, messaging.Source) {
		config := DefaultConfig(address)
		driver, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		return driver, messaging.Destination{Name: "contract"}, messaging.Source{Name: "contract"}
	}, contracttest.Options{})
}

func requireValkeyAddress(t *testing.T) string {
	t.Helper()
	address := os.Getenv("VALKEY_ADDRESS")
	if address != "" {
		return address
	}
	if os.Getenv("CI") != "" {
		t.Fatal("VALKEY_ADDRESS is required in CI")
	}
	t.Skip("VALKEY_ADDRESS is required for the Pub/Sub contract suite")
	return ""
}

func TestCapabilitiesAreEphemeral(t *testing.T) {
	driver, err := New(DefaultConfig("127.0.0.1:6379"))
	if err != nil {
		t.Fatal(err)
	}
	caps := driver.Capabilities()
	if caps.Durability || caps.Acknowledgement || caps.Replay || caps.CompetingConsumers || !caps.Fanout {
		t.Fatalf("unexpected capabilities %#v", caps)
	}
}

func TestSubscriptionRecoversAfterServerDisconnect(t *testing.T) {
	address := requireValkeyAddress(t)
	driver, err := New(DefaultConfig(address))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
	channel := "reconnect-" + time.Now().Format("150405.000000000")
	delivered := make(chan struct{}, 1)
	subscription, err := driver.Subscribe(ctx, messaging.Source{Name: channel}, func(context.Context, messaging.Delivery) messaging.HandleResult {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, subscription)
	<-subscription.Ready()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{address}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	if err := client.Do(ctx, client.B().ClientKill().TypePubsub().SkipmeYes().Build()).Error(); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-delivered:
			return
		case <-ticker.C:
			envelope := messaging.NewEnvelope("reconnect", "event", messaging.KindEvent, "1", "application/json", []byte(`{}`), nil)
			if _, publishErr := driver.Publish(ctx, messaging.Destination{Name: channel}, envelope); publishErr != nil && ctx.Err() != nil {
				t.Fatal(publishErr)
			}
		case <-ctx.Done():
			t.Fatal("subscription did not recover after server disconnect")
		}
	}
}

func TestOversizedRawPubSubFrameIsRejectedBeforeDecode(t *testing.T) {
	address := requireValkeyAddress(t)
	config := DefaultConfig(address)
	config.Valkey.MaxMessageBytes = 64
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if startErr := driver.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	cleanupDriver(t, driver)
	channel := "oversized-" + time.Now().Format("150405.000000000")
	subscription, err := driver.Subscribe(ctx, messaging.Source{Name: channel}, func(context.Context, messaging.Delivery) messaging.HandleResult {
		t.Error("oversized frame reached handler")
		return messaging.Complete()
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupSubscription(t, subscription)
	<-subscription.Ready()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{address}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	if publishErr := client.Do(ctx, client.B().Publish().Channel(channel).Message(strings.Repeat("x", 65)).Build()).Error(); publishErr != nil {
		t.Fatal(publishErr)
	}
	select {
	case reported := <-subscription.Errors():
		if !errors.Is(reported, messaging.ErrMessageTooLarge) {
			t.Fatalf("reported %v", reported)
		}
	case <-ctx.Done():
		t.Fatal("oversized frame was not reported")
	}
}

func cleanupDriver(t *testing.T, driver *Driver) {
	t.Helper()
	t.Cleanup(func() {
		if err := driver.Close(context.Background()); err != nil {
			t.Errorf("close driver: %v", err)
		}
	})
}

func cleanupSubscription(t *testing.T, subscription messaging.Subscription) {
	t.Helper()
	t.Cleanup(func() {
		if err := subscription.Close(context.Background()); err != nil {
			t.Errorf("close subscription: %v", err)
		}
	})
}
