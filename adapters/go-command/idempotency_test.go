package commandadapter

import (
	"context"
	"errors"
	"reflect"
	"testing"

	command "github.com/goliatone/go-command"
)

type claimStoreStub struct {
	result    ClaimResult
	claim     Claim
	completed *Completion
	released  *ClaimToken
}

func (s *claimStoreStub) Claim(_ context.Context, claim Claim) (ClaimResult, error) {
	s.claim = claim
	return s.result, nil
}
func (s *claimStoreStub) Complete(_ context.Context, completion Completion) error {
	s.completed = &completion
	return nil
}
func (s *claimStoreStub) Release(_ context.Context, token ClaimToken) error {
	s.released = &token
	return nil
}

func TestGuardedIngressExecutorFencesCompletionAndRelease(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	token := ClaimToken{Key: "idem-1", Token: "generation-7"}
	t.Run("completion", func(t *testing.T) {
		store := &claimStoreStub{result: ClaimResult{Status: ClaimAcquired, Token: token}}
		executed := 0
		guard := GuardedIngressExecutor{Store: store, Next: executorFunc(func(context.Context, command.MessageRegistration, any, command.DispatchOptions) (command.DispatchOutcome, error) {
			executed++
			return command.DispatchOutcome{Receipt: command.DispatchReceipt{Accepted: true, Mode: command.ExecutionModeInline, CommandID: registration.ID()}}, nil
		})}
		if _, err := guard.ExecuteInbound(context.Background(), registration, createMessage{Name: "Ada"}, command.DispatchOptions{Mode: command.ExecutionModeInline, IdempotencyKey: "idem-1"}); err != nil {
			t.Fatal(err)
		}
		if executed != 1 || store.completed == nil || store.completed.Token != token || store.released != nil || store.claim.Fingerprint == "" {
			t.Fatalf("executed=%d store=%#v", executed, store)
		}
	})

	t.Run("release on failure", func(t *testing.T) {
		store := &claimStoreStub{result: ClaimResult{Status: ClaimAcquired, Token: token}}
		guard := GuardedIngressExecutor{Store: store, Next: executorFunc(func(context.Context, command.MessageRegistration, any, command.DispatchOptions) (command.DispatchOutcome, error) {
			return command.DispatchOutcome{}, errors.New("failed")
		})}
		if _, err := guard.ExecuteInbound(context.Background(), registration, createMessage{Name: "Ada"}, command.DispatchOptions{Mode: command.ExecutionModeInline, IdempotencyKey: "idem-1"}); err == nil {
			t.Fatal("expected execution failure")
		}
		if store.released == nil || *store.released != token || store.completed != nil {
			t.Fatalf("store=%#v", store)
		}
	})
}

func TestGuardedIngressExecutorUsesCompletedOutcomeWithoutExecution(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	want := command.DispatchOutcome{Receipt: command.DispatchReceipt{Accepted: true, Mode: command.ExecutionModeInline, CommandID: registration.ID()}}
	store := &claimStoreStub{result: ClaimResult{Status: ClaimCompleted, Outcome: &want}}
	guard := GuardedIngressExecutor{Store: store, Next: executorFunc(func(context.Context, command.MessageRegistration, any, command.DispatchOptions) (command.DispatchOutcome, error) {
		t.Fatal("completed claim executed again")
		return command.DispatchOutcome{}, nil
	})}
	got, err := guard.ExecuteInbound(context.Background(), registration, createMessage{Name: "Ada"}, command.DispatchOptions{Mode: command.ExecutionModeInline, IdempotencyKey: "idem-1", CorrelationID: "new-correlation"})
	if err != nil || got.Receipt.CommandID != want.Receipt.CommandID || got.Receipt.CorrelationID != "new-correlation" {
		t.Fatalf("outcome=%#v err=%v", got, err)
	}
}
