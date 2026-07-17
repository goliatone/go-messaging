package chatdemo

import (
	"strings"
	"testing"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

func TestChatMessageValidation(t *testing.T) {
	tests := []struct {
		name    string
		message ChatMessage
		wantErr bool
	}{
		{name: "valid", message: ChatMessage{Sender: "alice", Text: "hello", Client: "browser"}},
		{name: "empty sender", message: ChatMessage{Text: "hello"}, wantErr: true},
		{name: "empty text", message: ChatMessage{Sender: "alice", Text: "  "}, wantErr: true},
		{name: "long sender", message: ChatMessage{Sender: strings.Repeat("a", MaxSenderRunes+1), Text: "hello"}, wantErr: true},
		{name: "long text", message: ChatMessage{Sender: "alice", Text: strings.Repeat("x", MaxTextRunes+1)}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.message.Validate(); (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestDecodeChatDeliveryRejectsUnknownAndTrailingPayloadData(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"sender":"alice","text":"hi","admin":true}`),
		[]byte(`{"sender":"alice","text":"hi"} {}`),
	} {
		envelope := messaging.NewEnvelope("id", ChatEventType, messaging.KindEvent, ChatSchema, ChatContentType, payload, nil)
		if _, err := DecodeChatDelivery(messaging.NewDelivery(envelope, messaging.DeliveryInfo{})); err == nil {
			t.Fatalf("DecodeChatDelivery(%s) succeeded", payload)
		}
	}
}

func TestDecodeChatDeliveryUsesEnvelopeMetadata(t *testing.T) {
	envelope := messaging.NewEnvelope("envelope-id", ChatEventType, messaging.KindEvent, ChatSchema, ChatContentType, []byte(`{"sender":" alice ","text":" hello ","client":"browser"}`), nil)
	envelope.Timestamp = time.Date(2026, 7, 17, 1, 2, 3, 0, time.UTC)
	view, err := DecodeChatDelivery(messaging.NewDelivery(envelope, messaging.DeliveryInfo{}))
	if err != nil {
		t.Fatal(err)
	}
	if view.ID != envelope.ID || !view.Timestamp.Equal(envelope.Timestamp) || view.Sender != "alice" || view.Text != "hello" {
		t.Fatalf("unexpected view %#v", view)
	}
}
