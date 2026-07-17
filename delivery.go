package messaging

import (
	"context"
	"time"
)

type DeliveryInfo struct {
	Transport   string
	Destination string
	DeliveryID  string
	Attempt     int
	ReceivedAt  time.Time
	Metadata    map[string]string
}

func (i DeliveryInfo) Clone() DeliveryInfo {
	i.Metadata = cloneHeaders(i.Metadata)
	return i
}

// Delivery exposes clones so handlers cannot mutate driver-owned values.
type Delivery interface {
	Envelope() Envelope
	Info() DeliveryInfo
}

type Acknowledger interface {
	Ack(context.Context) error
	Nack(context.Context, NackOptions) error
}

type NackOptions struct {
	Disposition Disposition
	RetryAfter  time.Duration
	Reason      string
}

type BasicDelivery struct {
	envelope Envelope
	info     DeliveryInfo
}

func NewDelivery(envelope Envelope, info DeliveryInfo) BasicDelivery {
	return BasicDelivery{envelope: envelope.Clone(), info: info.Clone()}
}

func (d BasicDelivery) Envelope() Envelope { return d.envelope.Clone() }
func (d BasicDelivery) Info() DeliveryInfo { return d.info.Clone() }
