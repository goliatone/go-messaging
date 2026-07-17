package commandadapter

import (
	"context"
	"strings"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

type outboundLineage struct {
	correlationID string
	causationID   string
}

// outboundLineageFromContext preserves the trace shared by a nested dispatch
// and identifies the ingress delivery as its immediate cause. Contexts created
// outside a delivery may carry only an upstream causation ID, which remains a
// useful fallback instead of dropping the lineage entirely.
func outboundLineageFromContext(ctx context.Context) outboundLineage {
	provenance, ok := command.DispatchProvenanceFromContext(ctx)
	if !ok {
		return outboundLineage{}
	}
	lineage := outboundLineage{
		correlationID: strings.TrimSpace(provenance.CorrelationID),
		causationID:   strings.TrimSpace(provenance.DeliveryID),
	}
	if lineage.causationID == "" {
		lineage.causationID = strings.TrimSpace(provenance.CausationID)
	}
	return lineage
}

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
