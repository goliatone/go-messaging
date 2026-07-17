module github.com/goliatone/go-messaging/transport/valkey

go 1.23.4

require (
	github.com/goliatone/go-messaging v0.0.0
	github.com/valkey-io/valkey-go v1.0.65
)

require golang.org/x/sys v0.31.0 // indirect

replace github.com/goliatone/go-messaging => ../..
