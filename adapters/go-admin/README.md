# go-admin command-run adapter

`github.com/goliatone/go-messaging/adapters/go-admin` implements the optional
go-admin command-run publisher/subscriber contracts over `go-messaging`. The
root adapter stays provider-neutral; the `valkey` subpackage assembles the first
supported remote topology with Valkey Pub/Sub.

## Contract

Each lifecycle update uses this strict envelope:

```text
type             go-admin.debug.command-run.updated
kind             event
schema_version   1
content_type     application/json
id               command-run event_id
correlation_id   command-run correlation_id
causation_id     command-run dispatch_id
payload          CommandRunUpdate JSON
```

Application, environment, tenant, and organization scope is mirrored into
headers. Decode requires exact header/payload agreement, the configured
application/environment identity, bounded payload size, and the subscriber's
trusted selector before calling a handler. Unknown JSON fields, trailing JSON,
wrong metadata/lineage, and unauthorized scopes are rejected. Adapter errors
map to provider-neutral go-admin categories; they do not contain message
payloads, tenant values, credentials, or raw provider causes.

## Valkey Pub/Sub assembly

The optional helper creates one channel:

```text
<environment>.<application>.go-admin.command-runs
```

Application and environment segments accept letters, digits, `_`, and `-`.
Every web gateway creates its own Pub/Sub subscription to this channel, so all
web nodes receive the same worker event. Do not replace this with one Valkey
Streams consumer group: a competing group distributes events across gateways
instead of broadcasting to each gateway.

```go
components, err := commandvalkey.New(commandvalkey.Config{
    Addresses:      []string{"127.0.0.1:6379"},
    ApplicationID:  "console",
    EnvironmentID:  "production",
    Role:           admin.CommandRunRoleGateway,
    PublishTimeout: 500 * time.Millisecond,
})
if err != nil {
    return err
}

// Driver lifecycle is host-owned.
if err := components.StartDriver(ctx); err != nil {
    return err
}

cfg.Debug = admin.DebugConfig{
    Enabled:     true,
    AppID:       "console",
    Environment: "production",
    CommandRuns: admin.CommandRunRuntimeConfig{
        Enabled:       true,
        Role:          admin.CommandRunRoleGateway,
        Subscriber:    components.Transport,
        RequireFanout: true,
    },
}
if err := adm.RegisterModule(admin.NewDebugModule(cfg.Debug)); err != nil {
    _ = components.CloseDriver(context.Background())
    return err
}
if err := adm.Initialize(router); err != nil {
    _ = components.CloseDriver(context.Background())
    return err
}

// Shutdown ordering is mandatory: subscriptions first, driver second.
if err := adm.CloseCommandRunRuntime(closeCtx); err != nil {
    return err
}
return components.CloseDriver(closeCtx)
```

Use `CommandRunRolePublisher` plus `Publisher: components.Transport` in a
worker, or `CommandRunRoleMonolith` plus `Transport: components.Transport` in a
hybrid process. Components are assembled but not started; the host starts the
driver before the runtime and closes it only after runtime subscriptions.
Closing an adapter subscription never closes the injected driver or router.

## Delivery and recovery limits

Valkey Pub/Sub is ephemeral fanout: durability and replay are false. Broker
loss is reported through safe publish/subscription failures, and subscriptions
resubscribe automatically, but events published during the gap are not
replayed. The gateway's `CommandRunStore` supplies reconnect snapshots. The
default memory store is process-local, so process restart history requires a
host-provided shared/durable store or a later durable projection topology.

The channel isolates application/environment traffic, not individual tenants.
Every trusted gateway for that channel receives all of its tenant/org updates,
then the adapter selector and authenticated Debug WebSocket authorizer enforce
scope. Use separate application/environment channels or a custom broker
topology when broker-level tenant isolation is required.

Duplicate Pub/Sub deliveries are possible. All consumers must converge through
go-admin's idempotent projector/store, which uses `event_id`, `run_id`, and
`revision` and prevents terminal regression.

## Diagnostics

`CommandRunRuntime.Diagnostics()` and go-admin Doctor expose role, transport
capabilities, readiness, projection count, last successful projection time,
and cumulative safe counters for publish, subscription, rejection, drop, and
projection failures. A health probe reports broker outages while Valkey's
automatic subscription recovery remains active.

## Development

The adapter is a separate module. The committed repository `go.work` contains
only modules from this repository, so it remains valid in a standalone clone.
For coordinated work with unreleased go-admin or go-command sources, create a
temporary workspace outside all participating repositories and point `GOWORK`
at it; never add sibling paths to a committed workspace.

The independent release gate deliberately disables workspace resolution:

```sh
cd adapters/go-admin
GOWORK=off go mod tidy -diff
GOWORK=off go test -race ./...
GOWORK=off go vet ./...

# Real broker coverage, including two gateways and restart recovery.
GOADMIN_VALKEY_DOCKER_TEST=1 GOWORK=off go test ./valkey -run DockerValkey
```

Release the go-admin contract version before publishing an adapter version that
requires it; do not ship local `replace` directives. The current adapter source
requires the command-run facade and `admin/commandruntest`, which are newer than
go-admin `v0.121.2`; that version therefore cannot be the final compatibility
floor.
