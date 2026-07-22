package valkey_test

import (
	"fmt"

	admin "github.com/goliatone/go-admin/pkg/admin"
	commandvalkey "github.com/goliatone/go-messaging/adapters/go-admin/valkey"
)

func ExampleNew() {
	components, err := commandvalkey.New(commandvalkey.Config{
		Addresses:     []string{"127.0.0.1:6379"},
		ApplicationID: "console",
		EnvironmentID: "production",
		Role:          admin.CommandRunRolePublisher,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(components.Channel)
	fmt.Println(components.Transport.Capabilities().Fanout)

	// Output:
	// production.console.go-admin.command-runs
	// true
}
