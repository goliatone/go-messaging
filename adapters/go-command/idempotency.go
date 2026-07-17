package commandadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	command "github.com/goliatone/go-command"
)

var (
	ErrClaimInProgress = errors.New("go-command adapter: idempotency claim is in progress")
	ErrClaimConflict   = errors.New("go-command adapter: idempotency claim conflicts with another payload")
)

type ClaimStatus string

const (
	ClaimAcquired   ClaimStatus = "acquired"
	ClaimInProgress ClaimStatus = "in_progress"
	ClaimCompleted  ClaimStatus = "completed"
	ClaimConflict   ClaimStatus = "conflict"
)

type Claim struct {
	Key            string
	Fingerprint    string
	RegistrationID string
	CorrelationID  string
}

type ClaimToken struct {
	Key   string
	Token string
}

type ClaimResult struct {
	Status  ClaimStatus
	Token   ClaimToken
	Outcome *command.DispatchOutcome
}

type Completion struct {
	Token   ClaimToken
	Outcome command.DispatchOutcome
}

// ClaimStore is transport-independent. Tokens fence completion/release so an
// expired worker cannot mutate a newer claim generation.
type ClaimStore interface {
	Claim(context.Context, Claim) (ClaimResult, error)
	Complete(context.Context, Completion) error
	Release(context.Context, ClaimToken) error
}

type GuardedIngressExecutor struct {
	Next           IngressExecutor
	Store          ClaimStore
	Codec          TypedCodec
	CleanupTimeout time.Duration
}

func (e GuardedIngressExecutor) ExecuteInbound(ctx context.Context, registration command.MessageRegistration, message any, options command.DispatchOptions) (command.DispatchOutcome, error) {
	if e.Next == nil || isTypedNil(e.Next) {
		return command.DispatchOutcome{}, fmt.Errorf("go-command adapter: guarded ingress executor requires a next executor")
	}
	key := strings.TrimSpace(options.IdempotencyKey)
	if key == "" || e.Store == nil || isTypedNil(e.Store) {
		return e.Next.ExecuteInbound(ctx, registration, message, options)
	}
	result, err := e.claim(ctx, registration, message, options, key)
	if err != nil {
		return command.DispatchOutcome{}, err
	}
	if outcome, resolved, err := resolveClaim(result, key, options.CorrelationID); resolved {
		return outcome, err
	}
	return e.executeClaimed(ctx, registration, message, options, result.Token)
}

func (e GuardedIngressExecutor) claim(ctx context.Context, registration command.MessageRegistration, message any, options command.DispatchOptions, key string) (ClaimResult, error) {
	codec := e.Codec
	if codec == nil || isTypedNil(codec) {
		codec = JSONTypedCodec{}
	}
	payload, err := codec.Encode(ctx, registration, message)
	if err != nil {
		return ClaimResult{}, err
	}
	digest := sha256.Sum256(append([]byte(registration.ID()+"\x00"), payload...))
	claim := Claim{
		Key: key, Fingerprint: hex.EncodeToString(digest[:]),
		RegistrationID: registration.ID(), CorrelationID: options.CorrelationID,
	}
	return e.Store.Claim(ctx, claim)
}

func resolveClaim(result ClaimResult, key, correlationID string) (command.DispatchOutcome, bool, error) {
	switch result.Status {
	case ClaimCompleted:
		if result.Outcome == nil {
			return command.DispatchOutcome{}, true, fmt.Errorf("go-command adapter: completed idempotency claim has no outcome")
		}
		replayed := *result.Outcome
		replayed.Receipt.CorrelationID = correlationID
		return replayed, true, nil
	case ClaimInProgress:
		return command.DispatchOutcome{}, true, ErrClaimInProgress
	case ClaimConflict:
		return command.DispatchOutcome{}, true, ErrClaimConflict
	case ClaimAcquired:
		if strings.TrimSpace(result.Token.Key) != key || strings.TrimSpace(result.Token.Token) == "" {
			return command.DispatchOutcome{}, true, fmt.Errorf("go-command adapter: acquired claim requires a fencing token")
		}
		return command.DispatchOutcome{}, false, nil
	default:
		return command.DispatchOutcome{}, true, fmt.Errorf("go-command adapter: invalid claim status %q", result.Status)
	}
}

func (e GuardedIngressExecutor) executeClaimed(ctx context.Context, registration command.MessageRegistration, message any, options command.DispatchOptions, token ClaimToken) (command.DispatchOutcome, error) {
	outcome, executeErr := e.Next.ExecuteInbound(ctx, registration, message, options)
	cleanupTimeout := e.CleanupTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = 5 * time.Second
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if executeErr != nil {
		if releaseErr := e.Store.Release(cleanupCtx, token); releaseErr != nil {
			return command.DispatchOutcome{}, errors.Join(executeErr, releaseErr)
		}
		return command.DispatchOutcome{}, executeErr
	}
	if err := e.Store.Complete(cleanupCtx, Completion{Token: token, Outcome: outcome}); err != nil {
		return command.DispatchOutcome{}, err
	}
	return outcome, nil
}
