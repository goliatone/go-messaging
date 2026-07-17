// Package chatdemo implements the runnable browser and terminal chat example.
package chatdemo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	messaging "github.com/goliatone/go-messaging"
)

const usage = `usage:
  chat-demo serve [options]
  chat-demo send --sender <name> [options] <message>
  chat-demo read [--json] [options]
`

const (
	messagingUnavailableMessage = "messaging service is unavailable"
	publishFailedMessage        = "message could not be published"
	subscriptionFailedMessage   = "chat subscription was interrupted"
	shutdownFailedMessage       = "messaging shutdown did not complete cleanly"
)

// publicError keeps the original cause available to errors.Is/errors.As while
// ensuring command and log boundaries expose only a stable application message.
type publicError struct {
	message string
	cause   error
}

func (e *publicError) Error() string { return e.message }
func (e *publicError) Unwrap() error { return e.cause }

func safeError(message string, cause error) error {
	if cause == nil {
		return nil
	}
	return &publicError{message: message, cause: cause}
}

// Run dispatches a chat-demo subcommand.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		if _, err := fmt.Fprint(stderr, usage); err != nil {
			return err
		}
		return errors.New("a subcommand is required")
	}
	switch args[0] {
	case "send":
		return runSend(ctx, args[1:], stdout, stderr)
	case "read":
		return runRead(ctx, args[1:], stdout, stderr)
	case "serve":
		return runServe(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		_, err := fmt.Fprint(stdout, usage)
		return err
	default:
		_, writeErr := fmt.Fprint(stderr, usage)
		return errors.Join(writeErr, fmt.Errorf("unknown subcommand %q", args[0]))
	}
}

func newObserver(output io.Writer) messaging.Observer {
	if output == nil {
		output = io.Discard
	}
	logger := slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return messaging.ObserverFunc(func(_ context.Context, observation messaging.Observation) {
		attributes := []any{
			"operation", observation.Operation,
			"route", observation.LogicalRoute,
			"transport", observation.Transport,
			"outcome", observation.Outcome,
			"latency", observation.Latency.Round(time.Microsecond),
		}
		if observation.Err != nil {
			attributes = append(attributes, "error", "messaging operation failed")
		}
		logger.Info("messaging operation", attributes...)
	})
}
