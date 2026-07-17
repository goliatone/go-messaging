package pubsub

import (
	"os"
	"testing"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/internal/contracttest"
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
