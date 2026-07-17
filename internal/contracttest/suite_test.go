package contracttest

import (
	"testing"

	messaging "github.com/goliatone/go-messaging"
	"github.com/goliatone/go-messaging/internal/testkit"
)

func TestSuiteAgainstMemoryDriver(t *testing.T) {
	Run(t, func(*testing.T) (DuplexDriver, messaging.Destination, messaging.Source) {
		return testkit.NewMemoryDriver(), messaging.Destination{Name: "contract"}, messaging.Source{Name: "contract"}
	}, Options{})
}
