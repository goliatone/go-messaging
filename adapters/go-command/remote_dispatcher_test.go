package commandadapter

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	command "github.com/goliatone/go-command"
	messaging "github.com/goliatone/go-messaging"
)

type remoteTestDriver struct {
	publish func(context.Context, messaging.Destination, messaging.Envelope) (messaging.PublishResult, error)
	ready   chan struct{}
}

func newRemoteTestDriver() *remoteTestDriver {
	ready := make(chan struct{})
	close(ready)
	return &remoteTestDriver{ready: ready}
}

func (*remoteTestDriver) Capabilities() messaging.Capabilities {
	return messaging.Capabilities{RequestReply: true}
}
func (*remoteTestDriver) Start(context.Context) error { return nil }
func (d *remoteTestDriver) Ready() <-chan struct{}    { return d.ready }
func (*remoteTestDriver) Errors() <-chan error        { return nil }
func (*remoteTestDriver) Close(context.Context) error { return nil }
func (d *remoteTestDriver) Publish(ctx context.Context, destination messaging.Destination, envelope messaging.Envelope) (messaging.PublishResult, error) {
	return d.publish(ctx, destination, envelope)
}

func TestRemoteDispatcherWaitsForCorrelatedWorkerQueryResult(t *testing.T) {
	registration := ingressRegistration{
		id: "test.lookup", messageType: "test.lookup", kind: command.HandlerKindQuery,
		request: reflect.TypeFor[lookupMessage](), result: reflect.TypeFor[string](),
		newMessage: func() any { return &lookupMessage{} },
	}
	driver := newRemoteTestDriver()
	correlations := newTestCorrelationRegistry(t, 4)
	var remote *RemoteDispatcher
	driver.publish = func(_ context.Context, destination messaging.Destination, request messaging.Envelope) (messaging.PublishResult, error) {
		if correlations.Pending() != 1 {
			t.Fatalf("request published before waiter registration: pending=%d", correlations.Pending())
		}
		if destination.Name != "queries" || request.ReplyTo != "reply" {
			t.Fatalf("destination=%q reply_to=%q", destination.Name, request.ReplyTo)
		}
		outcome := command.DispatchOutcome{
			Receipt: command.DispatchReceipt{
				Accepted: true, Mode: command.ExecutionModeInline,
				CommandID: registration.ID(), CorrelationID: request.CorrelationID,
			},
			Result: "found:42", ResultPresent: true,
		}
		payload, err := (JSONReplyCodec{}).Encode(context.Background(), registration, outcome, nil)
		if err != nil {
			t.Fatal(err)
		}
		reply := messaging.NewEnvelope("reply-1", request.Type, messaging.KindReply, "1", (JSONReplyCodec{}).ContentType(), payload, nil)
		reply.CorrelationID = request.CorrelationID
		if got := remote.HandleReply(context.Background(), messaging.NewDelivery(reply, messaging.DeliveryInfo{})); got.Disposition != messaging.DispositionComplete {
			t.Fatalf("reply disposition %#v", got)
		}
		return messaging.PublishResult{Outcome: messaging.PublishAccepted}, nil
	}
	router := remoteTestRouter(t, driver, false)
	remote = newTestRemoteDispatcher(t, RemoteDispatcherConfig{Router: router, Correlations: correlations, ReplyRoute: "reply"})

	outcome, err := remote.DispatchRemote(
		context.Background(), command.DispatchRoute{Target: command.DispatchTargetRemote, Name: "request"},
		registration, lookupMessage{ID: "42"}, command.DispatchOptions{Mode: command.ExecutionModeInline},
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Result != "found:42" || !outcome.ResultPresent || correlations.Pending() != 0 {
		t.Fatalf("outcome=%#v pending=%d", outcome, correlations.Pending())
	}
}

func TestRemoteDispatcherPreservesNestedDispatchLineage(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	provider, err := command.NewMessageRegistrationIndex(registration)
	if err != nil {
		t.Fatal(err)
	}
	catalogCodec, err := NewJSONCatalogCodec(provider, JSONTypedCodec{}, CatalogBinding{
		CatalogID: "create", RegistrationID: registration.ID(), Kind: registration.Kind(), SchemaVersion: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name                string
		catalog             bool
		explicitCorrelation string
		wantCorrelation     string
	}{
		{name: "typed inherits correlation", wantCorrelation: "correlation-ingress"},
		{name: "catalog inherits correlation", catalog: true, wantCorrelation: "correlation-ingress"},
		{name: "typed explicit correlation wins", explicitCorrelation: "correlation-explicit", wantCorrelation: "correlation-explicit"},
		{name: "catalog explicit correlation wins", catalog: true, explicitCorrelation: "correlation-explicit", wantCorrelation: "correlation-explicit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := newRemoteTestDriver()
			correlations := newTestCorrelationRegistry(t, 1)
			var remote *RemoteDispatcher
			driver.publish = func(_ context.Context, _ messaging.Destination, request messaging.Envelope) (messaging.PublishResult, error) {
				if request.CorrelationID != test.wantCorrelation || request.CausationID != "delivery-ingress" {
					t.Fatalf("request lineage correlation=%q causation=%q", request.CorrelationID, request.CausationID)
				}
				if test.catalog {
					if request.Type != CatalogEnvelopeType {
						t.Fatalf("request type = %q", request.Type)
					}
					var dto CatalogDispatchDTO
					if unmarshalErr := json.Unmarshal(request.Payload, &dto); unmarshalErr != nil {
						t.Fatal(unmarshalErr)
					}
					if dto.Options.CorrelationID != test.wantCorrelation {
						t.Fatalf("catalog correlation = %q", dto.Options.CorrelationID)
					}
				} else if request.Type != registration.MessageType() {
					t.Fatalf("request type = %q", request.Type)
				}
				outcome := command.DispatchOutcome{Receipt: command.DispatchReceipt{
					Accepted: true, Mode: command.ExecutionModeInline,
					CommandID: registration.ID(), CorrelationID: request.CorrelationID,
				}}
				payload, encodeErr := (JSONReplyCodec{}).Encode(context.Background(), registration, outcome, nil)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				reply := messaging.NewEnvelope("reply-lineage", request.Type, messaging.KindReply, "1", (JSONReplyCodec{}).ContentType(), payload, nil)
				reply.CorrelationID = request.CorrelationID
				if got := remote.HandleReply(context.Background(), messaging.NewDelivery(reply, messaging.DeliveryInfo{})); got.Disposition != messaging.DispositionComplete {
					t.Fatalf("reply disposition %#v", got)
				}
				return messaging.PublishResult{Outcome: messaging.PublishAccepted}, nil
			}
			config := RemoteDispatcherConfig{
				Router: remoteTestRouter(t, driver, false), Correlations: correlations, ReplyRoute: "reply",
			}
			if test.catalog {
				config.CatalogCodec = catalogCodec
				config.UseCatalog = func(command.MessageRegistration) bool { return true }
			}
			remote = newTestRemoteDispatcher(t, config)
			ctx := command.ContextWithDispatchProvenance(context.Background(), command.DispatchProvenance{
				DeliveryID: "delivery-ingress", CorrelationID: "correlation-ingress", CausationID: "cause-before-ingress",
			})
			_, dispatchErr := remote.DispatchRemote(
				ctx, command.DispatchRoute{Target: command.DispatchTargetRemote, Name: "request"},
				registration, createMessage{Name: "Ada"}, command.DispatchOptions{
					Mode: command.ExecutionModeInline, CorrelationID: test.explicitCorrelation,
				},
			)
			if dispatchErr != nil {
				t.Fatal(dispatchErr)
			}
		})
	}
}

func TestOutboundLineageFallsBackToKnownCausation(t *testing.T) {
	ctx := command.ContextWithDispatchProvenance(context.Background(), command.DispatchProvenance{
		CorrelationID: " correlation ", CausationID: " earlier-cause ",
	})
	lineage := outboundLineageFromContext(ctx)
	if lineage.correlationID != "correlation" || lineage.causationID != "earlier-cause" {
		t.Fatalf("lineage = %#v", lineage)
	}
}

func TestRemoteDispatcherCleansWaiterOnCancellationAndAmbiguousPublish(t *testing.T) {
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	t.Run("cancellation", func(t *testing.T) {
		driver := newRemoteTestDriver()
		driver.publish = func(context.Context, messaging.Destination, messaging.Envelope) (messaging.PublishResult, error) {
			return messaging.PublishResult{Outcome: messaging.PublishAccepted}, nil
		}
		correlations := newTestCorrelationRegistry(t, 1)
		remote := newTestRemoteDispatcher(t, RemoteDispatcherConfig{Router: remoteTestRouter(t, driver, false), Correlations: correlations, ReplyRoute: "reply"})
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)
		_, err := remote.DispatchRemote(ctx, command.DispatchRoute{Target: command.DispatchTargetRemote, Name: "request"}, registration, createMessage{Name: "Ada"}, command.DispatchOptions{Mode: command.ExecutionModeInline})
		if !errors.Is(err, context.Canceled) || correlations.Pending() != 0 {
			t.Fatalf("err=%v pending=%d", err, correlations.Pending())
		}
	})

	t.Run("ambiguous publish", func(t *testing.T) {
		driver := newRemoteTestDriver()
		driver.publish = func(context.Context, messaging.Destination, messaging.Envelope) (messaging.PublishResult, error) {
			return messaging.PublishResult{Outcome: messaging.PublishAmbiguous}, messaging.ErrPublishAmbiguous
		}
		correlations := newTestCorrelationRegistry(t, 1)
		remote := newTestRemoteDispatcher(t, RemoteDispatcherConfig{Router: remoteTestRouter(t, driver, false), Correlations: correlations, ReplyRoute: "reply"})
		_, err := remote.DispatchRemote(context.Background(), command.DispatchRoute{Target: command.DispatchTargetRemote, Name: "request"}, registration, createMessage{Name: "Ada"}, command.DispatchOptions{Mode: command.ExecutionModeInline})
		if !errors.Is(err, messaging.ErrPublishAmbiguous) || correlations.Pending() != 0 {
			t.Fatalf("err=%v pending=%d", err, correlations.Pending())
		}
	})
}

func TestRemoteDispatcherRequiresReplyPath(t *testing.T) {
	driver := newRemoteTestDriver()
	driver.publish = func(context.Context, messaging.Destination, messaging.Envelope) (messaging.PublishResult, error) {
		t.Fatal("publish should not run")
		return messaging.PublishResult{}, nil
	}
	correlations := newTestCorrelationRegistry(t, 1)
	remote := newTestRemoteDispatcher(t, RemoteDispatcherConfig{Router: remoteTestRouter(t, driver, false), Correlations: correlations})
	registration := ingressRegistration{
		id: "test.create", messageType: "test.create", kind: command.HandlerKindCommand,
		request: reflect.TypeFor[createMessage](), newMessage: func() any { return &createMessage{} },
	}
	_, err := remote.DispatchRemote(context.Background(), command.DispatchRoute{Target: command.DispatchTargetRemote, Name: "request"}, registration, createMessage{}, command.DispatchOptions{Mode: command.ExecutionModeInline})
	if !errors.Is(err, messaging.ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}

func TestRemoteDispatcherRejectsRouteThatDoesNotAcceptReplies(t *testing.T) {
	driver := newRemoteTestDriver()
	driver.publish = func(context.Context, messaging.Destination, messaging.Envelope) (messaging.PublishResult, error) {
		t.Fatal("publish should not run")
		return messaging.PublishResult{}, nil
	}
	registry, err := messaging.NewDriverRegistry(map[string]messaging.Driver{"remote": driver})
	if err != nil {
		t.Fatal(err)
	}
	router, err := messaging.NewRouter(registry, []messaging.Route{
		{Name: "request", Strategy: messaging.StrategyPrimary, Bindings: []messaging.RouteBinding{{Driver: "remote", Destination: messaging.Destination{Name: "requests"}}}},
		{Name: "reply", Strategy: messaging.StrategyPrimary, Kinds: []messaging.Kind{messaging.KindCommand}, Bindings: []messaging.RouteBinding{{Driver: "remote", Destination: messaging.Destination{Name: "replies"}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	remote := newTestRemoteDispatcher(t, RemoteDispatcherConfig{Router: router, Correlations: newTestCorrelationRegistry(t, 1), ReplyRoute: "reply"})
	err = remote.ValidateRoutes(command.DispatchRoute{Target: command.DispatchTargetRemote, Name: "request"})
	if !errors.Is(err, messaging.ErrUnsupportedCapability) {
		t.Fatalf("got %v", err)
	}
}

func newTestCorrelationRegistry(t *testing.T, capacity int) *messaging.CorrelationRegistry {
	t.Helper()
	registry, err := messaging.NewCorrelationRegistry(capacity, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func newTestRemoteDispatcher(t *testing.T, config RemoteDispatcherConfig) *RemoteDispatcher {
	t.Helper()
	dispatcher, err := NewRemoteDispatcher(config)
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

func remoteTestRouter(t *testing.T, driver *remoteTestDriver, requestReplyRequired bool) *messaging.Router {
	t.Helper()
	registry, err := messaging.NewDriverRegistry(map[string]messaging.Driver{"remote": driver})
	if err != nil {
		t.Fatal(err)
	}
	required := []messaging.Capability(nil)
	if requestReplyRequired {
		required = []messaging.Capability{messaging.CapabilityRequestReply}
	}
	router, err := messaging.NewRouter(registry, []messaging.Route{
		{Name: "request", Strategy: messaging.StrategyPrimary, Required: required, Bindings: []messaging.RouteBinding{{Driver: "remote", Destination: messaging.Destination{Name: "queries"}}}},
		{Name: "reply", Strategy: messaging.StrategyPrimary, Bindings: []messaging.RouteBinding{{Driver: "remote", Destination: messaging.Destination{Name: "replies"}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return router
}
