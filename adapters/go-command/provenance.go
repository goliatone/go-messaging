package commandadapter

import (
	"context"
	"strings"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

func contextWithDeliveryProvenance(ctx context.Context, logicalRoute string, envelope messaging.Envelope, info messaging.DeliveryInfo) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return command.ContextWithDispatchProvenance(ctx, command.DispatchProvenance{
		IngressKind: string(envelope.Kind), Route: strings.TrimSpace(logicalRoute),
		DeliveryID: info.DeliveryID, Attempt: info.Attempt,
		CorrelationID: envelope.CorrelationID, CausationID: envelope.CausationID,
	})
}

// ForRoute returns an ingress view that records a bounded logical route in
// handler provenance without mutating a concurrently shared ingress.
func (i *TypedIngress) ForRoute(logicalRoute string) *TypedIngress {
	if i == nil {
		return nil
	}
	clone := *i
	clone.logicalRoute = strings.TrimSpace(logicalRoute)
	return &clone
}

func (i *CatalogIngress) ForRoute(logicalRoute string) *CatalogIngress {
	if i == nil {
		return nil
	}
	clone := *i
	clone.logicalRoute = strings.TrimSpace(logicalRoute)
	return &clone
}
