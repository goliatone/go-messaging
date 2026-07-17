package messaging

import (
	"context"
	"fmt"
)

type Destination struct {
	Name string
}

type PublishOutcome string

const (
	PublishAccepted               PublishOutcome = "accepted"
	PublishRejected               PublishOutcome = "rejected"
	PublishDefinitelyNotPublished PublishOutcome = "definitely_not_published"
	PublishAmbiguous              PublishOutcome = "ambiguous"
)

type PublishResult struct {
	Outcome           PublishOutcome
	Transport         string
	Destination       string
	ProviderMessageID string
	RecipientCount    *int64
	Metadata          map[string]string
}

func (r PublishResult) Clone() PublishResult {
	r.Metadata = cloneHeaders(r.Metadata)
	if r.RecipientCount != nil {
		value := *r.RecipientCount
		r.RecipientCount = &value
	}
	return r
}

// OutcomeError converts a non-accepted publish outcome into its stable error
// classification. Drivers may return both provider-specific errors and an
// outcome; routers use this method when a driver omits the corresponding error.
func (r PublishResult) OutcomeError() error {
	switch r.Outcome {
	case PublishAccepted:
		return nil
	case PublishRejected:
		return ErrPublishRejected
	case PublishDefinitelyNotPublished:
		return ErrNotPublished
	case PublishAmbiguous:
		return ErrPublishAmbiguous
	case "":
		return fmt.Errorf("%w: driver returned no publish outcome", ErrNotPublished)
	default:
		return fmt.Errorf("%w: driver returned invalid publish outcome %q", ErrNotPublished, r.Outcome)
	}
}

type Publisher interface {
	Publish(context.Context, Destination, Envelope) (PublishResult, error)
}

type PublishDriver interface {
	Driver
	Publisher
}
