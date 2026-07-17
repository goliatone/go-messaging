package messaging

import (
	"fmt"
	"sort"
	"strings"
)

type Capability string

const (
	CapabilityDurability         Capability = "durability"
	CapabilityAcknowledgement    Capability = "acknowledgement"
	CapabilityCompetingConsumers Capability = "competing_consumers"
	CapabilityOrdering           Capability = "ordering"
	CapabilityReplay             Capability = "replay"
	CapabilityRequestReply       Capability = "request_reply"
	CapabilityDelay              Capability = "delay"
	CapabilityFanout             Capability = "fanout"
)

type Capabilities struct {
	Durability         bool
	Acknowledgement    bool
	CompetingConsumers bool
	Ordering           bool
	Replay             bool
	RequestReply       bool
	Delay              bool
	Fanout             bool
}

func (c Capabilities) Supports(capability Capability) bool {
	switch capability {
	case CapabilityDurability:
		return c.Durability
	case CapabilityAcknowledgement:
		return c.Acknowledgement
	case CapabilityCompetingConsumers:
		return c.CompetingConsumers
	case CapabilityOrdering:
		return c.Ordering
	case CapabilityReplay:
		return c.Replay
	case CapabilityRequestReply:
		return c.RequestReply
	case CapabilityDelay:
		return c.Delay
	case CapabilityFanout:
		return c.Fanout
	default:
		return false
	}
}

func (c Capabilities) Validate(required ...Capability) error {
	missing := make([]string, 0)
	for _, capability := range required {
		if !c.Supports(capability) {
			missing = append(missing, string(capability))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("%w: missing %s", ErrUnsupportedCapability, strings.Join(missing, ", "))
}

func CapabilityForDisposition(disposition Disposition) []Capability {
	switch disposition {
	case DispositionComplete, DispositionReject:
		return nil
	case DispositionRetry, DispositionDeadLetter:
		return []Capability{CapabilityAcknowledgement}
	default:
		return []Capability{"unknown_disposition"}
	}
}
