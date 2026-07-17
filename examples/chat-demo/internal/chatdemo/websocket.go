package chatdemo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/goliatone/go-router"
)

const (
	EventSend     = "chat.send"
	EventReady    = "chat.ready"
	EventAccepted = "chat.accepted"
	EventMessage  = "chat.message"
	EventError    = "chat.error"
)

type wsEvent struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type readyEvent struct {
	Transport string `json:"transport"`
	Replay    bool   `json:"replay"`
}

type acceptedEvent struct {
	ID      string `json:"id"`
	Outcome string `json:"outcome"`
}

type errorEvent struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type inboundEvent struct {
	Type string `json:"type"`
}

type eventEmitter interface {
	EmitWithContext(context.Context, string, any) error
}

func registerChatClient(broker *Broker, client router.WSClient) error {
	if err := client.OnMessage(func(ctx context.Context, data []byte) error {
		frameError := validateInboundEvent(data)
		if frameError == nil {
			return nil
		}
		return client.EmitWithContext(ctx, EventError, *frameError)
	}); err != nil {
		return err
	}
	if err := client.OnJSON(EventSend, func(ctx context.Context, data json.RawMessage) error {
		return handleChatSend(ctx, broker, client, data)
	}); err != nil {
		return err
	}
	return client.Emit(EventReady, readyEvent{Transport: "valkey.pubsub", Replay: false})
}

func validateInboundEvent(data []byte) *errorEvent {
	if len(data) == 0 || len(data) > MaxPayloadSize {
		return &errorEvent{Code: "invalid_frame", Message: "message must be one bounded JSON event"}
	}
	var event inboundEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return &errorEvent{Code: "invalid_frame", Message: "message must be a valid JSON event"}
	}
	event.Type = strings.TrimSpace(event.Type)
	if event.Type == "" {
		return &errorEvent{Code: "invalid_event", Message: "message event type is required"}
	}
	if event.Type != EventSend {
		return &errorEvent{Code: "unsupported_event", Message: "message event type is not supported"}
	}
	return nil
}

func handleChatSend(ctx context.Context, broker *Broker, client eventEmitter, data json.RawMessage) error {
	message, err := decodeChatSend(data)
	if err != nil {
		return client.EmitWithContext(ctx, EventError, errorEvent{Code: "invalid_message", Message: err.Error()})
	}
	envelope, result, publishErr := broker.Publish(ctx, message)
	outcome := routingOutcome(result, publishErr)
	if publishErr != nil || outcome != "accepted" {
		return client.EmitWithContext(ctx, EventError, errorEvent{Code: "publish_failed", Message: "message was not accepted"})
	}
	return client.EmitWithContext(ctx, EventAccepted, acceptedEvent{ID: envelope.ID, Outcome: string(outcome)})
}

func decodeChatSend(data []byte) (ChatMessage, error) {
	if len(data) == 0 || len(data) > MaxPayloadSize {
		return ChatMessage{}, errors.New("invalid message size")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var message ChatMessage
	if err := decoder.Decode(&message); err != nil {
		return ChatMessage{}, errors.New("message must be valid JSON")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return ChatMessage{}, errors.New("message must contain one JSON object")
	}
	message.Client = "browser"
	message = message.normalized()
	if err := message.Validate(); err != nil {
		return ChatMessage{}, err
	}
	return message, nil
}
