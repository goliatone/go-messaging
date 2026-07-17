package messaging

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCorrelationRegisterBeforeDeliverAndCleanup(t *testing.T) {
	r, err := NewCorrelationRegistry(1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w, err := r.Register("corr-1", "orders.reply", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	reply := validEnvelope()
	reply.Kind = KindReply
	reply.Type = "orders.reply"
	reply.CorrelationID = "corr-1"
	if !r.Deliver(reply) || r.Deliver(reply) {
		t.Fatal("reply should resolve exactly once")
	}
	got, err := w.Await(context.Background())
	if err != nil || got.CorrelationID != "corr-1" || r.Pending() != 0 {
		t.Fatalf("got=%#v err=%v pending=%d", got, err, r.Pending())
	}
}

func TestCorrelationMismatchAndCancellation(t *testing.T) {
	r, err := NewCorrelationRegistry(1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w, err := r.Register("corr-1", "right", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	reply := validEnvelope()
	reply.Kind = KindReply
	reply.Type = "wrong"
	reply.CorrelationID = "corr-1"
	if r.Deliver(reply) || r.Pending() != 1 {
		t.Fatal("mismatch consumed waiter")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.Await(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if r.Pending() != 0 {
		t.Fatal("cancellation leaked waiter")
	}
}

func TestCorrelationCapacityAndTimeout(t *testing.T) {
	r, err := NewCorrelationRegistry(1, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	w, err := r.Register("one", "reply", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register("two", "reply", time.Time{}); !errors.Is(err, ErrCorrelation) {
		t.Fatalf("got %v", err)
	}
	if _, err := w.Await(context.Background()); !errors.Is(err, ErrReplyTimeout) {
		t.Fatalf("got %v", err)
	}
}

func TestCorrelationExplicitCancelAndSingleAwait(t *testing.T) {
	r, err := NewCorrelationRegistry(1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w, err := r.Register("one", "reply", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	w.Cancel()
	if _, err := w.Await(context.Background()); !errors.Is(err, ErrCorrelation) {
		t.Fatalf("got %v", err)
	}
	if _, err := w.Await(context.Background()); !errors.Is(err, ErrCorrelation) {
		t.Fatalf("second await got %v", err)
	}
}

func TestCorrelationObservesMatchedAndLateReplies(t *testing.T) {
	var observations []Observation
	r, err := NewCorrelationRegistry(1, time.Second, ObserverFunc(func(_ context.Context, observation Observation) {
		observations = append(observations, observation)
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register("corr", "reply", time.Time{}); err != nil {
		t.Fatal(err)
	}
	reply := validEnvelope()
	reply.Kind = KindReply
	reply.Type = "reply"
	reply.CorrelationID = "corr"
	if !r.Deliver(reply) || r.Deliver(reply) {
		t.Fatal("expected one matched and one duplicate reply")
	}
	if len(observations) != 2 || observations[0].Operation != OperationReply || observations[0].Outcome != "matched" || observations[1].Outcome != "unknown_or_duplicate" || !errors.Is(observations[1].Err, ErrCorrelation) {
		t.Fatalf("observations = %#v", observations)
	}
}
