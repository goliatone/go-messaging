# go-command Adapter Guide

The adapter connects `go-command` registrations to logical `go-messaging` routes without making either root module depend on a provider. It requires `go-command` v0.23.1 or newer.

## Choose a process role

- Local-only: use `go-command` directly; no messaging setup is needed.
- Publisher: construct a messaging driver and router, then configure `RemoteDispatcher` on a `go-command` runtime.
- Worker: consume a command/query source through `TypedIngress` and publish the executing process outcome with `ReplyingIngress`.
- Hybrid: install placement policies only for registrations that should execute remotely; registrations without a policy remain local.

The logical route in a placement policy is not a channel or stream name. The messaging router maps it to provider destinations.

## Local-only

Local dispatch remains unchanged and does not require this adapter:

```go
runtime := dispatcher.NewRuntime()
dispatcher.SubscribeCommandTo(runtime, command.CommandFunc[CreateUser](handleCreateUser))

err := dispatcher.DispatchTo(ctx, runtime, CreateUser{Email: "user@example.com"})
```

## Publisher and hybrid placement

Start the selected transport driver, register it, and define request and reply routes. This example uses one Valkey Pub/Sub driver; Streams can be substituted when durable processing is required.

```go
driver, err := pubsub.New(pubsub.DefaultConfig("127.0.0.1:6379"))
if err != nil {
    return err
}
if err := driver.Start(ctx); err != nil {
    return err
}

drivers, err := messaging.NewDriverRegistry(map[string]messaging.Driver{
    "commands": driver,
})
if err != nil {
    return err
}
router, err := messaging.NewRouter(drivers, []messaging.Route{
    {
        Name: "commands-primary", Strategy: messaging.StrategyPrimary,
        Kinds: []messaging.Kind{messaging.KindCommand, messaging.KindQuery},
        Bindings: []messaging.RouteBinding{{
            Driver: "commands", Destination: messaging.Destination{Name: "commands"},
        }},
    },
    {
        Name: "command-replies", Strategy: messaging.StrategyPrimary,
        Kinds: []messaging.Kind{messaging.KindReply},
        Bindings: []messaging.RouteBinding{{
            Driver: "commands", Destination: messaging.Destination{Name: "command-replies"},
        }},
    },
}, nil)
if err != nil {
    return err
}

correlations, err := messaging.NewCorrelationRegistry(4096, 30*time.Second)
if err != nil {
    return err
}
remote, err := commandadapter.NewRemoteDispatcher(commandadapter.RemoteDispatcherConfig{
    Router: router, Correlations: correlations,
    ReplyRoute: "command-replies",
})
if err != nil {
    return err
}
```

Attach registrations before installing placement. Validate the route before accepting traffic:

```go
registration, ok := runtime.RegistrationProvider().RegistrationByMessageType(
    command.HandlerKindCommand, "user.create",
)
if !ok {
    return command.NewRegistrationNotFoundError(command.HandlerKindCommand, "user.create")
}
route := command.DispatchRoute{
    Target: command.DispatchTargetRemote,
    Name:   "commands-primary",
}
if err := remote.ValidateRoutes(route); err != nil {
    return err
}
if err := runtime.ReplacePlacementPolicies(command.PlacementPolicy{
    Kind: command.HandlerKindCommand, RegistrationID: registration.ID(), Route: route,
}); err != nil {
    return err
}
if err := runtime.ConfigureRemoteDispatcher(remote); err != nil {
    return err
}
if err := runtime.RoutedReady(); err != nil {
    return err
}
```

Subscribe the publisher process to its reply destination before issuing remote work. The handler completes unknown, late, and duplicate replies so they cannot become poison-message loops:

```go
replyIngress, err := messaging.NewIngress(drivers, []messaging.IngressBinding{{
    Name: "command-replies", LogicalRoute: "command-replies",
    Driver: "commands", Source: messaging.Source{Name: "command-replies"},
    AcceptedKinds: []messaging.Kind{messaging.KindReply},
    Handlers: []messaging.Handler{remote.HandleReply},
}})
if err != nil {
    return err
}
replySubscriptions, err := replyIngress.Subscribe(ctx)
```

Wait for every returned subscription's `Ready` channel before exposing the process. Close subscriptions and drivers during shutdown.

## Worker

The worker uses the same initialized registration provider as its `go-command` runtime. `RuntimeExecutor` invokes `Runtime.InvokeLocal`, which prevents a received command from being routed remotely again.

```go
typed, err := commandadapter.NewTypedIngress(
    runtime.RegistrationProvider(),
    commandadapter.RuntimeExecutor{Runtime: runtime},
    commandadapter.JSONTypedCodec{},
)
if err != nil {
    return err
}
worker := commandadapter.ReplyingIngress{
    Ingress: typed.ForRoute("commands-primary"),
    Replies: commandadapter.ReplyPublisher{Router: router},
    Errors:  commandadapter.DefaultErrorMapper{},
}
commandIngress, err := messaging.NewIngress(drivers, []messaging.IngressBinding{{
    Name: "command-worker", LogicalRoute: "commands-primary",
    Driver: "commands",
    Source: messaging.Source{Name: "commands", Group: "workers", Consumer: workerID},
    AcceptedKinds: []messaging.Kind{messaging.KindCommand, messaging.KindQuery},
    Handlers: []messaging.Handler{worker.Handler},
}})
if err != nil {
    return err
}
commandSubscriptions, err := commandIngress.Subscribe(ctx)
```

Queries require a reply route. Commands may be one-way, but remote `go-command` dispatch uses a reply so the publisher receives the executing process receipt rather than treating broker acceptance as execution.

For cross-version or externally exposed command schemas, configure `JSONCatalogCodec` with explicit catalog bindings. The adapter intentionally does not serialize `command.DispatchOptions` directly.

## Explicit event-to-command trigger

Events never become commands implicitly. Authorize each mapping and invoke `ExecuteBound` from an event handler:

```go
bindings, err := commandadapter.NewIngressBindings(commandadapter.IngressBinding{
    EnvelopeKind: messaging.KindEvent,
    MessageType:  "user.signup-requested",
    HandlerKind:  command.HandlerKindCommand,
})
if err != nil {
    return err
}
eventHandler := func(ctx context.Context, delivery messaging.Delivery) messaging.HandleResult {
    _, err := typed.ForRoute("signup-events").ExecuteBound(ctx, delivery, bindings)
    return (commandadapter.DefaultErrorMapper{}).Map(err, delivery.Info().Attempt)
}
```

The command registration must use the authorized event message type and a compatible payload shape. Keep authorization in application assembly, not in payload fields.

## Command and query replies

After hybrid placement is configured, existing `go-command` APIs continue to work:

```go
receipt, err := dispatcher.DispatchWith(ctx, CreateUser{Email: email}, command.DispatchOptions{})
user, err := dispatcher.Query[FindUser, *User](ctx, FindUser{ID: id})
```

The adapter registers correlation before publishing, validates command ID, execution mode, result type, and correlation on the reply, and removes waiters on completion, timeout, or cancellation. Caller cancellation stops waiting; it does not guarantee cancellation of work already accepted by the worker.

Use an application-supplied `ClaimStore` with `GuardedIngressExecutor` when ambiguous delivery or retry can execute a command more than once. Claims are fenced and transport-independent; completed outcomes are reused with the current correlation ID.
