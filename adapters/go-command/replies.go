package commandadapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	command "github.com/goliatone/go-command"
	gerrors "github.com/goliatone/go-errors"
	messaging "github.com/goliatone/go-messaging"
)

const ReplyWireVersion = "1"

type FailureDTO struct {
	Category        string `json:"category"`
	Code            int    `json:"code,omitempty"`
	TextCode        string `json:"text_code,omitempty"`
	Message         string `json:"message"`
	Retryable       bool   `json:"retryable,omitempty"`
	RetryAfterNanos int64  `json:"retry_after_nanos,omitempty"`
}

type ReplyDTO struct {
	Version         string                  `json:"version"`
	Receipt         command.DispatchReceipt `json:"receipt,omitempty"`
	ResultPresent   bool                    `json:"result_present"`
	Result          json.RawMessage         `json:"result,omitempty"`
	ResultType      string                  `json:"result_type,omitempty"`
	StatusReference string                  `json:"status_reference,omitempty"`
	Failure         *FailureDTO             `json:"failure,omitempty"`
}

type ReplyCodec interface {
	Encode(context.Context, command.MessageRegistration, command.DispatchOutcome, error) ([]byte, error)
	Decode(context.Context, command.MessageRegistration, messaging.Envelope) (command.DispatchOutcome, error)
	ContentType() string
}

type FailureReplyCodec interface {
	EncodeFailure(context.Context, error) ([]byte, error)
}

type JSONReplyCodec struct{}

func (JSONReplyCodec) ContentType() string { return "application/vnd.goliatone.go-command-reply+json" }

func (JSONReplyCodec) EncodeFailure(_ context.Context, dispatchErr error) ([]byte, error) {
	if dispatchErr == nil {
		dispatchErr = fmt.Errorf("go-command adapter: remote command execution failed")
	}
	return json.Marshal(ReplyDTO{Version: ReplyWireVersion, Failure: failureFromError(dispatchErr)})
}

func (JSONReplyCodec) Encode(_ context.Context, registration command.MessageRegistration, outcome command.DispatchOutcome, dispatchErr error) ([]byte, error) {
	if err := validateRegistration(registration); err != nil {
		return nil, err
	}
	dto := ReplyDTO{Version: ReplyWireVersion}
	if dispatchErr != nil {
		dto.Failure = failureFromError(dispatchErr)
		return json.Marshal(dto)
	}
	if err := command.ValidateDispatchReceipt(outcome.Receipt); err != nil {
		return nil, err
	}
	dto.Receipt = outcome.Receipt
	dto.ResultPresent = outcome.ResultPresent
	dto.StatusReference = strings.TrimSpace(outcome.StatusReference)
	if outcome.ResultPresent {
		result, err := json.Marshal(outcome.Result)
		if err != nil {
			return nil, fmt.Errorf("go-command adapter: encode reply result: %w", err)
		}
		dto.Result = result
		dto.ResultType = typeName(registration.ResultType())
	}
	return json.Marshal(dto)
}

func (JSONReplyCodec) Decode(_ context.Context, registration command.MessageRegistration, envelope messaging.Envelope) (command.DispatchOutcome, error) {
	if err := validateRegistration(registration); err != nil {
		return command.DispatchOutcome{}, err
	}
	if envelope.Kind != messaging.KindReply {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: expected reply envelope")
	}
	var dto ReplyDTO
	if err := json.Unmarshal(envelope.Payload, &dto); err != nil {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: decode reply: %w", err)
	}
	if dto.Version != ReplyWireVersion {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: unsupported reply wire version %q", dto.Version)
	}
	if dto.Failure != nil {
		return command.DispatchOutcome{}, errorFromFailure(*dto.Failure)
	}
	if err := command.ValidateDispatchReceipt(dto.Receipt); err != nil {
		return command.DispatchOutcome{}, err
	}
	if envelope.CorrelationID != "" && dto.Receipt.CorrelationID != envelope.CorrelationID {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: reply receipt correlation mismatch")
	}
	outcome := command.DispatchOutcome{
		Receipt: dto.Receipt, ResultPresent: dto.ResultPresent,
		StatusReference: strings.TrimSpace(dto.StatusReference),
	}
	if !dto.ResultPresent {
		if registration.Kind() == command.HandlerKindQuery {
			return command.DispatchOutcome{}, command.NewDynamicResultMissingError(registration)
		}
		return outcome, nil
	}
	expected := registration.ResultType()
	if expected != nil && dto.ResultType != typeName(expected) {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: reply result type mismatch: expected %s, got %s", typeName(expected), dto.ResultType)
	}
	result, err := decodeResult(dto.Result, expected)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	outcome.Result = result
	return outcome, nil
}

func failureFromError(err error) *FailureDTO {
	failure := &FailureDTO{Category: string(gerrors.CategoryInternal), Message: "remote command execution failed"}
	projected := projectAdapterError(err)
	var retryable *gerrors.RetryableError
	if gerrors.As(projected, &retryable) && retryable.BaseError != nil {
		failure.Category = string(retryable.Category)
		failure.Code = retryable.Code
		failure.TextCode = retryable.TextCode
		failure.Retryable = retryable.IsRetryable()
		failure.RetryAfterNanos = retryable.RetryDelay(1).Nanoseconds()
		return failure
	}
	var structured *gerrors.Error
	if gerrors.As(projected, &structured) {
		failure.Category = string(structured.Category)
		failure.Code = structured.Code
		failure.TextCode = structured.TextCode
	}
	return failure
}

func errorFromFailure(failure FailureDTO) error {
	category := gerrors.Category(strings.TrimSpace(failure.Category))
	if category == "" {
		category = gerrors.CategoryInternal
	}
	message := strings.TrimSpace(failure.Message)
	if message == "" {
		message = "remote command execution failed"
	}
	if failure.Retryable {
		err := gerrors.NewRetryable(message, category).WithRetryDelay(time.Duration(failure.RetryAfterNanos))
		if failure.Code != 0 {
			err = err.WithCode(failure.Code)
		}
		if failure.TextCode != "" {
			err = err.WithTextCode(failure.TextCode)
		}
		return err
	}
	err := gerrors.New(message, category)
	if failure.Code != 0 {
		err = err.WithCode(failure.Code)
	}
	if failure.TextCode != "" {
		err = err.WithTextCode(failure.TextCode)
	}
	return err
}

func decodeResult(data json.RawMessage, expected reflect.Type) (any, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("go-command adapter: present reply result is missing")
	}
	if string(data) == "null" {
		if expected != nil && !nilable(expected) {
			return nil, fmt.Errorf("go-command adapter: null reply result is incompatible with %s", expected)
		}
		return nil, nil
	}
	if expected == nil || expected.Kind() == reflect.Interface {
		var result any
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("go-command adapter: decode reply result: %w", err)
		}
		return result, nil
	}
	target := reflect.New(expected)
	if err := json.Unmarshal(data, target.Interface()); err != nil {
		return nil, fmt.Errorf("go-command adapter: decode reply result: %w", err)
	}
	return target.Elem().Interface(), nil
}

func nilable(value reflect.Type) bool {
	if value == nil {
		return true
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

func typeName(value reflect.Type) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func newEnvelopeID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("go-command adapter: generate envelope id: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

type ReplyPublisher struct {
	Router *messaging.Router
	Codec  ReplyCodec
}

func (p ReplyPublisher) PublishFailure(ctx context.Context, request messaging.Envelope, dispatchErr error) (messaging.RoutingResult, error) {
	codec := p.Codec
	if codec == nil || isTypedNil(codec) {
		codec = JSONReplyCodec{}
	}
	failureCodec, ok := codec.(FailureReplyCodec)
	if !ok || isTypedNil(failureCodec) {
		return messaging.RoutingResult{}, fmt.Errorf("go-command adapter: reply codec cannot encode registration-independent failures")
	}
	payload, err := failureCodec.EncodeFailure(ctx, dispatchErr)
	if err != nil {
		return messaging.RoutingResult{}, err
	}
	return p.publishPayload(ctx, request, codec.ContentType(), payload)
}

func (p ReplyPublisher) Publish(ctx context.Context, request messaging.Envelope, registration command.MessageRegistration, outcome command.DispatchOutcome, dispatchErr error) (messaging.RoutingResult, error) {
	if p.Router == nil {
		return messaging.RoutingResult{}, fmt.Errorf("go-command adapter: reply router is required")
	}
	if strings.TrimSpace(request.ReplyTo) == "" {
		return messaging.RoutingResult{}, fmt.Errorf("go-command adapter: request does not name a logical reply route")
	}
	codec := p.Codec
	if codec == nil || isTypedNil(codec) {
		codec = JSONReplyCodec{}
	}
	payload, err := codec.Encode(ctx, registration, outcome, dispatchErr)
	if err != nil {
		return messaging.RoutingResult{}, err
	}
	return p.publishPayload(ctx, request, codec.ContentType(), payload)
}

func (p ReplyPublisher) publishPayload(ctx context.Context, request messaging.Envelope, contentType string, payload []byte) (messaging.RoutingResult, error) {
	if p.Router == nil {
		return messaging.RoutingResult{}, fmt.Errorf("go-command adapter: reply router is required")
	}
	if strings.TrimSpace(request.ReplyTo) == "" {
		return messaging.RoutingResult{}, fmt.Errorf("go-command adapter: request does not name a logical reply route")
	}
	id, err := newEnvelopeID()
	if err != nil {
		return messaging.RoutingResult{}, err
	}
	reply := messaging.NewEnvelope(id, request.Type, messaging.KindReply, request.SchemaVersion, contentType, payload, nil)
	reply.CorrelationID = request.CorrelationID
	reply.CausationID = request.ID
	result, err := p.Router.Publish(ctx, request.ReplyTo, reply)
	if err != nil {
		return result, err
	}
	if !routingAccepted(result) {
		return result, messaging.ErrNotPublished
	}
	return result, nil
}
