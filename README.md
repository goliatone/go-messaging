# go-messaging

Transport-neutral messaging contracts and independently versioned transport modules for Go. The root module owns immutable envelopes, routing, ingress, delivery dispositions, capabilities, correlation, and observability. Provider SDKs stay in nested modules.

## Modules

| Module | Purpose |
| --- | --- |
| `github.com/goliatone/go-messaging` | Contracts, envelopes, routes, ingress, and reply correlation |
| `github.com/goliatone/go-messaging/transport/valkey` | Valkey Pub/Sub and Streams drivers |
| `github.com/goliatone/go-messaging/adapters/go-command` | Optional typed bridge for `go-command` v0.23.1+ |

Install only the modules used by the application:

```sh
go get github.com/goliatone/go-messaging@latest
go get github.com/goliatone/go-messaging/transport/valkey@latest
go get github.com/goliatone/go-messaging/adapters/go-command@latest
```

## Basic publisher

```go
driver, err := pubsub.New(pubsub.DefaultConfig("127.0.0.1:6379"))
if err != nil {
    return err
}
if err := driver.Start(ctx); err != nil {
    return err
}
defer driver.Close(context.Background())

drivers, err := messaging.NewDriverRegistry(map[string]messaging.Driver{
    "valkey-pubsub": driver,
})
if err != nil {
    return err
}
router, err := messaging.NewRouter(drivers, []messaging.Route{{
    Name:     "domain-events",
    Strategy: messaging.StrategyPrimary,
    Kinds:    []messaging.Kind{messaging.KindEvent},
    Bindings: []messaging.RouteBinding{{
        Driver:      "valkey-pubsub",
        Destination: messaging.Destination{Name: "events"},
    }},
}}, nil)
if err != nil {
    return err
}

event := messaging.NewEnvelope(
    "event-42", "user.created", messaging.KindEvent,
    "1", "application/json", payload, nil,
)
_, err = router.Publish(ctx, "domain-events", event)
```

Use Valkey Streams when durable delivery, acknowledgement, competing consumers, replay, or dead-lettering is required. Use Pub/Sub for ephemeral fanout. Routes are logical application names; provider destinations remain inside route bindings.

## Runnable chat example

[`examples/chat-demo`](examples/chat-demo/README.md) provides a complete browser
and terminal chat over a go-router WebSocket gateway and Valkey Pub/Sub. It is a
nested module, so its UI and transport dependencies do not enter the root module.

## go-command integration

The optional adapter supports local-only, publisher, worker, hybrid placement, explicitly authorized event-to-command triggers, and correlated command/query replies. See [the go-command adapter guide](docs/GUIDE_GO_COMMAND_ADAPTER.md) for assembly examples and safety rules.

## Development and releases

Run every present module independently:

```sh
./taskfile go:race
./taskfile go:vet
```

Preview a synchronized release without changing files, refs, or remotes:

```sh
./taskfile release:dry-run 0.2.0
```

Before an actual release, `./taskfile release:preflight` verifies the branch and tracked tree, rejects stale untracked `.version`/`CHANGELOG.md` outputs, and checks `git-cliff` plus the `origin` remote. The dry-run validates semantic import-version rules, lists only present nested-module tags, preserves the reviewed `go-command` requirement, and prints the atomic push refspec.
