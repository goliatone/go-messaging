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
	mapper := i.errorMapper()
	if i.Ingress == nil || isTypedNil(i.Ingress) || delivery == nil || isTypedNil(delivery) {
		return mapper.Map(fmt.Errorf("go-command adapter: replying ingress and delivery are required"), 0)
	}
	request := delivery.Envelope()
	if err := validateReplyRequest(request); err != nil {
		return messaging.Reject(err)
	}
	result, executionErr := i.Ingress.Execute(ctx, delivery)
	if request.ReplyTo == "" {
		return mapper.Map(executionErr, delivery.Info().Attempt)
	}
	return i.publishReply(ctx, request, result, executionErr, delivery.Info().Attempt, mapper)
}

func (i ReplyingIngress) errorMapper() ErrorMapper {
	if i.Errors == nil || isTypedNil(i.Errors) {
		return DefaultErrorMapper{}
	}
	return i.Errors
}

func validateReplyRequest(request messaging.Envelope) error {
	if request.Kind == messaging.KindQuery && request.ReplyTo == "" {
		return fmt.Errorf("go-command adapter: query ingress requires a reply route")
	}
	if request.ReplyTo != "" && request.CorrelationID == "" {
		return fmt.Errorf("go-command adapter: reply request requires a correlation id")
	}
	return nil
}

func (i ReplyingIngress) publishReply(ctx context.Context, request messaging.Envelope, result IngressResult, executionErr error, attempt int, mapper ErrorMapper) messaging.HandleResult {
	if result.Registration == nil || isTypedNil(result.Registration) {
		if executionErr == nil {
			executionErr = fmt.Errorf("go-command adapter: ingress completed without a registration")
		}
		if _, err := i.Replies.PublishFailure(ctx, request, executionErr); err != nil {
			return mapper.Map(err, attempt)
		}
		return messaging.Complete()
	}
	if _, err := i.Replies.Publish(ctx, request, result.Registration, result.Outcome, executionErr); err != nil {
		return mapper.Map(err, attempt)
	}
	return messaging.Complete()
}
