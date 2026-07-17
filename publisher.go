package messaging

import "context"

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

type Publisher interface {
	Publish(context.Context, Destination, Envelope) (PublishResult, error)
}

type PublishDriver interface {
	Driver
	Publisher
}
