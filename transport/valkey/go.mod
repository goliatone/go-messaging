module github.com/goliatone/go-messaging/transport/valkey

go 1.23.4

require (
	github.com/goliatone/go-messaging v0.0.0
	github.com/valkey-io/valkey-go v1.0.65
)

require (
	github.com/go-ozzo/ozzo-validation/v4 v4.3.0 // indirect
	github.com/goliatone/go-errors v0.11.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

replace github.com/goliatone/go-messaging => ../..
