package chatdemo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	messaging "github.com/goliatone/go-messaging"
)

const (
	ChatRoute       = "chat-messages"
	ChatEventType   = "demo.chat.message"
	ChatSchema      = "1"
	ChatContentType = "application/json"
	ChatDriverName  = "valkey-pubsub"

	MaxSenderRunes = 40
	MaxTextRunes   = 1000
	MaxClientRunes = 64
	MaxPayloadSize = 8 << 10
)

// ChatMessage is the versioned domain payload carried by a messaging envelope.
type ChatMessage struct {
	Sender string `json:"sender"`
	Text   string `json:"text"`
	Client string `json:"client,omitempty"`
}

// ChatMessageView combines the delivered payload with authoritative envelope metadata.
type ChatMessageView struct {
	ID        string    `json:"id"`
	Sender    string    `json:"sender"`
	Text      string    `json:"text"`
	Client    string    `json:"client,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

func (m ChatMessage) normalized() ChatMessage {
	m.Sender = strings.TrimSpace(m.Sender)
	m.Text = strings.TrimSpace(m.Text)
	m.Client = strings.TrimSpace(m.Client)
	return m
}

// Validate rejects empty, invalid UTF-8, and oversized user-controlled values.
func (m ChatMessage) Validate() error {
	m = m.normalized()
	if m.Sender == "" {
		return errors.New("sender is required")
	}
	if m.Text == "" {
		return errors.New("message is required")
	}
	for label, value := range map[string]string{"sender": m.Sender, "message": m.Text, "client": m.Client} {
		if !utf8.ValidString(value) {
			return fmt.Errorf("%s must be valid UTF-8", label)
		}
	}
	if utf8.RuneCountInString(m.Sender) > MaxSenderRunes {
		return fmt.Errorf("sender must be at most %d characters", MaxSenderRunes)
	}
	if utf8.RuneCountInString(m.Text) > MaxTextRunes {
		return fmt.Errorf("message must be at most %d characters", MaxTextRunes)
	}
	if utf8.RuneCountInString(m.Client) > MaxClientRunes {
		return fmt.Errorf("client must be at most %d characters", MaxClientRunes)
	}
	return nil
}

func encodeChatPayload(message ChatMessage) ([]byte, error) {
	message = message.normalized()
	if err := message.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode chat message: %w", err)
	}
	if len(payload) > MaxPayloadSize {
		return nil, errors.New("message payload is too large")
	}
	return payload, nil
}

func decodeChatPayload(payload []byte) (ChatMessage, error) {
	if len(payload) == 0 || len(payload) > MaxPayloadSize {
		return ChatMessage{}, errors.New("invalid chat payload size")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var message ChatMessage
	if err := decoder.Decode(&message); err != nil {
		return ChatMessage{}, fmt.Errorf("decode chat message: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return ChatMessage{}, err
	}
	message = message.normalized()
	if err := message.Validate(); err != nil {
		return ChatMessage{}, err
	}
	return message, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing chat data: %w", err)
	}
	return errors.New("chat payload must contain exactly one JSON value")
}

// DecodeChatDelivery validates and translates an ingress delivery for display.
func DecodeChatDelivery(delivery messaging.Delivery) (ChatMessageView, error) {
	if delivery == nil {
		return ChatMessageView{}, errors.New("chat delivery is required")
	}
	envelope := delivery.Envelope()
	if envelope.Kind != messaging.KindEvent || envelope.Type != ChatEventType ||
		envelope.SchemaVersion != ChatSchema || envelope.ContentType != ChatContentType {
		return ChatMessageView{}, errors.New("chat envelope does not match the supported contract")
	}
	message, err := decodeChatPayload(envelope.Payload)
	if err != nil {
		return ChatMessageView{}, err
	}
	return ChatMessageView{
		ID: envelope.ID, Sender: message.Sender, Text: message.Text,
		Client: message.Client, Timestamp: envelope.Timestamp,
	}, nil
}
