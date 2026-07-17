package commandadapter

import (
	"context"
	"fmt"

	messaging "github.com/goliatone/go-messaging"
)

type ExecutingIngress interface {
	Execute(context.Context, messaging.Delivery) (IngressResult, error)
}

// ReplyingIngress joins worker execution and reply publication into one
// transport handler. A successfully published execution failure is complete:
// the caller received the structured failure and redelivery would duplicate
// work. A reply publication failure is mapped through the injected policy.
type ReplyingIngress struct {
	Ingress ExecutingIngress
	Replies ReplyPublisher
	Errors  ErrorMapper
}

func (i ReplyingIngress) Handler(ctx context.Context, delivery messaging.Delivery) messaging.HandleResult {
	mapper := i.Errors
	if mapper == nil || isTypedNil(mapper) {
		mapper = DefaultErrorMapper{}
	}
	if i.Ingress == nil || isTypedNil(i.Ingress) || delivery == nil || isTypedNil(delivery) {
		return mapper.Map(fmt.Errorf("go-command adapter: replying ingress and delivery are required"), 0)
	}
	request := delivery.Envelope()
	if request.Kind == messaging.KindQuery && request.ReplyTo == "" {
		return messaging.Reject(fmt.Errorf("go-command adapter: query ingress requires a reply route"))
	}
	if request.ReplyTo != "" && request.CorrelationID == "" {
		return messaging.Reject(fmt.Errorf("go-command adapter: reply request requires a correlation id"))
	}
	result, executionErr := i.Ingress.Execute(ctx, delivery)
	if request.ReplyTo == "" {
		return mapper.Map(executionErr, delivery.Info().Attempt)
	}
	if result.Registration == nil || isTypedNil(result.Registration) {
		return mapper.Map(executionErr, delivery.Info().Attempt)
	}
	if _, err := i.Replies.Publish(ctx, request, result.Registration, result.Outcome, executionErr); err != nil {
		return mapper.Map(err, delivery.Info().Attempt)
	}
	return messaging.Complete()
}
