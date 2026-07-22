package goadmin

import (
	"errors"
	"fmt"

	admin "github.com/goliatone/go-admin/admin"
)

var (
	// ErrInvalidConfig identifies incomplete or contradictory adapter wiring.
	ErrInvalidConfig = errors.New("go-admin messaging adapter: invalid configuration")
	// ErrPublisherDisabled identifies a publish attempt on subscriber-only wiring.
	ErrPublisherDisabled = errors.New("go-admin messaging adapter: publisher is disabled")
	// ErrSubscriberDisabled identifies a subscription attempt on publisher-only wiring.
	ErrSubscriberDisabled = errors.New("go-admin messaging adapter: subscriber is disabled")
	// ErrEnvelopeRejected identifies an envelope that failed the adapter trust boundary.
	ErrEnvelopeRejected = fmt.Errorf("go-admin messaging adapter: %w", admin.ErrCommandRunEnvelopeRejected)
	// ErrIdentityMismatch identifies an application or environment mismatch.
	ErrIdentityMismatch = fmt.Errorf("go-admin messaging adapter: identity mismatch: %w", admin.ErrCommandRunScopeRejected)
	// ErrScopeRejected identifies a scope outside the subscriber's selector.
	ErrScopeRejected = fmt.Errorf("go-admin messaging adapter: %w", admin.ErrCommandRunScopeRejected)
	// ErrPublishFailed identifies a provider-neutral publish failure.
	ErrPublishFailed = fmt.Errorf("go-admin messaging adapter: %w", admin.ErrCommandRunPublishFailed)
	// ErrSubscriptionFailed identifies a provider-neutral subscription failure.
	ErrSubscriptionFailed = fmt.Errorf("go-admin messaging adapter: %w", admin.ErrCommandRunSubscriptionFailed)
	// ErrDeliveryFailed identifies a recoverable provider-neutral message delivery failure.
	ErrDeliveryFailed = fmt.Errorf("go-admin messaging adapter: %w", admin.ErrCommandRunDeliveryDropped)
)
