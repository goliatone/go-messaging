package chatdemo

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

type recordedEmitter struct {
	event  string
	data   any
	events []string
}

func (e *recordedEmitter) EmitWithContext(_ context.Context, event string, data any) error {
	e.event, e.data = event, data
	e.events = append(e.events, event)
	return nil
}

func TestHandleChatSendRejectedPublicationNeverEmitsMessage(t *testing.T) {
	driver := newTestDriver()
	driver.outcome = messaging.PublishRejected
	driver.publishErr = messaging.ErrPublishRejected
	broker, err := newBroker(driver, "chat", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	emitter := &recordedEmitter{}
	if err := handleChatSend(context.Background(), broker, emitter, json.RawMessage(`{"sender":"alice","text":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	if len(emitter.events) != 1 || emitter.events[0] != EventError {
		t.Fatalf("events %#v, want only %q", emitter.events, EventError)
	}
}

func TestDecodeChatSendEnforcesBrowserIdentityAndBounds(t *testing.T) {
	message, err := decodeChatSend(json.RawMessage(`{"sender":" alice ","text":" hello ","client":"forged"}`))
	if err != nil {
		t.Fatal(err)
	}
	if message.Sender != "alice" || message.Text != "hello" || message.Client != "browser" {
		t.Fatalf("unexpected message %#v", message)
	}
	if _, err := decodeChatSend(json.RawMessage(`{"sender":"alice","text":"hello","extra":true}`)); err == nil {
		t.Fatal("unknown field was accepted")
	}
}

func TestHandleChatSendSeparatesAcceptedFromDelivered(t *testing.T) {
	driver := newTestDriver()
	broker, err := newBroker(driver, "chat", time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	emitter := &recordedEmitter{}
	if err := handleChatSend(context.Background(), broker, emitter, json.RawMessage(`{"sender":"alice","text":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	if emitter.event != EventAccepted {
		t.Fatalf("event %q, want %q", emitter.event, EventAccepted)
	}
	if _, ok := emitter.data.(acceptedEvent); !ok {
		t.Fatalf("accepted data type %T", emitter.data)
	}
}

func TestValidateInboundEventRejectsMalformedAndUnsupportedFrames(t *testing.T) {
	tests := []struct {
		name string
		data string
		code string
	}{
		{name: "malformed", data: `{`, code: "invalid_frame"},
		{name: "missing type", data: `{}`, code: "invalid_event"},
		{name: "unsupported", data: `{"type":"chat.delete","data":{}}`, code: "unsupported_event"},
		{name: "valid", data: `{"type":"chat.send","data":{}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateInboundEvent([]byte(test.data))
			if test.code == "" {
				if err != nil {
					t.Fatalf("unexpected protocol error: %#v", err)
				}
				return
			}
			if err == nil || err.Code != test.code {
				t.Fatalf("protocol error = %#v, want code %q", err, test.code)
			}
		})
	}
}
