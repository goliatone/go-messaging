package chatdemo

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

func runSend(ctx context.Context, args []string, stdout, stderr io.Writer) (runErr error) {
	defaults := DefaultBrokerConfig()
	flags := flag.NewFlagSet("send", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sender := flags.String("sender", "", "sender name (required)")
	valkeyAddress := flags.String("valkey", defaults.ValkeyAddress, "Valkey address")
	channel := flags.String("channel", defaults.Channel, "Valkey Pub/Sub channel")
	if err := flags.Parse(args); err != nil {
		return err
	}
	messageText := strings.Join(flags.Args(), " ")
	message := ChatMessage{Sender: *sender, Text: messageText, Client: "cli"}
	if err := message.Validate(); err != nil {
		return err
	}
	broker, err := NewBroker(BrokerConfig{ValkeyAddress: *valkeyAddress, Channel: *channel}, newObserver(stderr))
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, closeBroker(broker)) }()
	if startErr := broker.Start(ctx); startErr != nil {
		return startErr
	}
	envelope, result, err := broker.Publish(ctx, message)
	outcome := routingOutcome(result, err)
	if _, writeErr := fmt.Fprintf(stdout, "id=%s route=%s outcome=%s\n", envelope.ID, ChatRoute, outcome); writeErr != nil {
		return fmt.Errorf("write send result: %w", writeErr)
	}
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}
	if outcome != messaging.PublishAccepted {
		return fmt.Errorf("message was not accepted: %s", outcome)
	}
	return nil
}

func runRead(ctx context.Context, args []string, stdout, stderr io.Writer) (runErr error) {
	defaults := DefaultBrokerConfig()
	flags := flag.NewFlagSet("read", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "write one JSON object per message")
	valkeyAddress := flags.String("valkey", defaults.ValkeyAddress, "Valkey address")
	channel := flags.String("channel", defaults.Channel, "Valkey Pub/Sub channel")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("read does not accept positional arguments")
	}
	broker, err := NewBroker(BrokerConfig{ValkeyAddress: *valkeyAddress, Channel: *channel}, newObserver(stderr))
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, closeBroker(broker)) }()
	if startErr := broker.Start(ctx); startErr != nil {
		return startErr
	}
	ingress, err := broker.NewIngress(func(_ context.Context, delivery messaging.Delivery) messaging.HandleResult {
		view, decodeErr := DecodeChatDelivery(delivery)
		if decodeErr != nil {
			return messaging.Reject(decodeErr)
		}
		if writeErr := WriteChatMessage(stdout, view, *jsonOutput); writeErr != nil {
			return messaging.Reject(writeErr)
		}
		return messaging.Complete()
	})
	if err != nil {
		return err
	}
	subscriptions, err := ingress.Subscribe(ctx)
	if err != nil {
		return safeError(messagingUnavailableMessage, err)
	}
	defer func() { runErr = errors.Join(runErr, closeSubscriptions(subscriptions)) }()
	if err := waitForSubscriptions(ctx, subscriptions); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	if _, writeErr := fmt.Fprintln(stderr, "reading live messages; press Ctrl-C to stop"); writeErr != nil {
		return writeErr
	}
	return monitorMessaging(ctx, broker.Errors(), subscriptions, diagnosticWriter(stderr))
}

// WriteChatMessage formats one delivered chat view for terminal consumers.
func WriteChatMessage(output io.Writer, view ChatMessageView, asJSON bool) error {
	if asJSON {
		if err := json.NewEncoder(output).Encode(view); err != nil {
			return fmt.Errorf("write JSON message: %w", err)
		}
		return nil
	}
	_, err := fmt.Fprintf(output, "%s %-20s %s\n", view.Timestamp.UTC().Format(time.RFC3339), view.Sender, view.Text)
	if err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

func routingOutcome(result messaging.RoutingResult, publishErr error) messaging.PublishOutcome {
	if len(result.Results) > 0 && result.Results[0].Outcome != "" {
		return result.Results[0].Outcome
	}
	switch {
	case errors.Is(publishErr, messaging.ErrPublishRejected):
		return messaging.PublishRejected
	case errors.Is(publishErr, messaging.ErrPublishAmbiguous):
		return messaging.PublishAmbiguous
	default:
		return messaging.PublishDefinitelyNotPublished
	}
}

func waitForSubscriptions(ctx context.Context, subscriptions []messaging.Subscription) error {
	for _, subscription := range subscriptions {
		select {
		case <-subscription.Ready():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func monitorMessaging(ctx context.Context, driverErrors <-chan error, subscriptions []messaging.Subscription, diagnostics ...func(error)) error {
	monitorCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sources := messagingErrorSources(driverErrors, subscriptions)
	if len(sources) == 0 {
		<-monitorCtx.Done()
		return contextCompletion(monitorCtx)
	}

	errorsOut := forwardMessagingErrors(monitorCtx, sources)
	for {
		select {
		case err, ok := <-errorsOut:
			if !ok {
				if monitorCtx.Err() != nil {
					return contextCompletion(monitorCtx)
				}
				return safeError(subscriptionFailedMessage, errors.New("messaging error channels closed"))
			}
			if messaging.IsMessageError(err) {
				if len(diagnostics) > 0 && diagnostics[0] != nil {
					diagnostics[0](err)
				}
				continue
			}
			return safeError(subscriptionFailedMessage, err)
		case <-monitorCtx.Done():
			return contextCompletion(monitorCtx)
		}
	}
}

func diagnosticWriter(output io.Writer) func(error) {
	if output == nil {
		output = io.Discard
	}
	return func(error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				return
			}
		}()
		if _, err := fmt.Fprintln(output, messageRejectedMessage); err != nil {
			return
		}
	}
}

func messagingErrorSources(driverErrors <-chan error, subscriptions []messaging.Subscription) []<-chan error {
	sources := make([]<-chan error, 0, len(subscriptions)+1)
	if driverErrors != nil {
		sources = append(sources, driverErrors)
	}
	for _, subscription := range subscriptions {
		if source := subscription.Errors(); source != nil {
			sources = append(sources, source)
		}
	}
	return sources
}

func forwardMessagingErrors(ctx context.Context, sources []<-chan error) <-chan error {
	errorsOut := make(chan error, len(sources))
	var forwarders sync.WaitGroup
	forwarders.Add(len(sources))
	for _, source := range sources {
		go func() {
			defer forwarders.Done()
			forwardMessagingError(ctx, source, errorsOut)
		}()
	}
	go func() {
		forwarders.Wait()
		close(errorsOut)
	}()
	return errorsOut
}

func forwardMessagingError(ctx context.Context, source <-chan error, errorsOut chan<- error) {
	for {
		select {
		case err, ok := <-source:
			if !ok {
				return
			}
			if err != nil {
				select {
				case errorsOut <- err:
				case <-ctx.Done():
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func contextCompletion(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return ctx.Err()
}

func closeSubscriptions(subscriptions []messaging.Subscription) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var joined error
	for _, subscription := range subscriptions {
		current := subscription
		joined = errors.Join(joined, runCleanup(ctx, func() error { return current.Close(ctx) }))
	}
	return safeError(shutdownFailedMessage, joined)
}

func closeBroker(broker *Broker) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return runCleanup(ctx, func() error { return broker.Close(ctx) })
}
