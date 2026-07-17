# Browser and CLI Chat Demo

This runnable example connects browser and terminal clients through one
`go-messaging` logical route backed by Valkey Pub/Sub. The browser edge uses
`go-router` for HTTP, embedded static assets, health, and WebSocket upgrades.

This chat is deliberately **live and ephemeral**. Pub/Sub does not retain
history: clients do not receive messages published while they are offline.

## Architecture

```text
Browser --WebSocket--> go-router --publish--> go-messaging Router
                                                |
CLI send ---------------------------------------+
                                                v
                                         Valkey Pub/Sub
                                                |
                         +----------------------+------------------+
                         v                                         v
                server Ingress                              CLI read Ingress
                         |                                         |
                         v                                         v
                  WebSocket broadcast                         terminal output
```

The WebSocket endpoint is an application gateway, not a `go-messaging`
transport driver. The nested module owns the go-router and Valkey dependencies;
the root messaging module remains transport-neutral.

## Prerequisites

- Go 1.26 or newer (required by go-router v0.59.0)
- Docker with Compose, or an unauthenticated local Valkey listening on the
  example's `127.0.0.1:6399` default

## Start the demo

From this directory:

```sh
docker compose up -d
go run ./cmd/chat-demo serve
```

Open [http://localhost:8989](http://localhost:8989). The health endpoint is
`http://localhost:8989/api/health`.

Stop Valkey when finished:

```sh
docker compose down
```

The server defaults are:

```text
--listen :8989
--valkey 127.0.0.1:6399
--channel go-messaging.demo.chat
--max-message-bytes 8192
--shutdown-timeout 5s
```

Compose publishes Valkey on loopback port `6399`, rather than the conventional
Redis/Valkey port `6379`, so it can run alongside an existing local instance.

WebSocket origins default to `localhost` and `127.0.0.1` on the selected listen
port. Use `--origins http://dev.example:8989` for another explicit local origin;
wildcards are rejected.

## Terminal clients

Publish one message directly through go-messaging and Valkey:

```sh
go run ./cmd/chat-demo send --sender cli "hello from the terminal"
```

The command reports the authoritative envelope ID, logical route, and
publication outcome:

```text
id=… route=chat-messages outcome=accepted
```

Read live messages until Ctrl-C:

```sh
go run ./cmd/chat-demo read
go run ./cmd/chat-demo read --json
```

`send` and `read` connect directly to Valkey; the web server is not required.
Use the same `--valkey` and `--channel` flags on every process when overriding
the defaults.

## WebSocket protocol

Connect to `/ws/chat`. Sending is enabled only after the server emits
`chat.ready`.

```json
{"type":"chat.ready","data":{"transport":"valkey.pubsub","replay":false}}
{"type":"chat.send","data":{"sender":"alice","text":"hello"}}
{"type":"chat.accepted","data":{"id":"…","outcome":"accepted"}}
{"type":"chat.message","data":{"id":"…","sender":"alice","text":"hello","client":"browser","timestamp":"2026-07-17T12:00:00Z"}}
{"type":"chat.error","data":{"code":"publish_failed","message":"message was not accepted"}}
```

`chat.accepted` means Valkey accepted the publication. Only `chat.message`
represents delivery back through ingress. The browser never creates a local
message card from its submitted text or from `chat.accepted`.

## Tests

Unit, route, protocol, and lifecycle tests run without infrastructure; the
real-Valkey test runs when `VALKEY_ADDRESS` is set:

```sh
GOWORK=off go test ./...
VALKEY_ADDRESS=127.0.0.1:6399 GOWORK=off go test -race ./...
```

From the repository root, module-aware formatting, test, race, vet, lint, and
security commands discover this nested module automatically.

## Troubleshooting

- **`NOAUTH` while starting:** the target Valkey requires credentials. The
  included Compose service is unauthenticated and listens on `127.0.0.1:6399`;
  check that it is running and that `--valkey` points to the intended instance.
- **WebSocket returns 403:** open the UI from the server URL or add its exact
  origin with `--origins`; wildcard origins are intentionally disabled.
- **Messages sent while disconnected are missing:** this is expected Pub/Sub
  behavior. The demo has no history or replay.
- **A different process uses port 8989:** choose another listen address, for
  example `--listen :8090`; the default local origin allowlist follows that port.
