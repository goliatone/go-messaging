package valkey

import (
	"context"
	"errors"
	"testing"
	"time"

	admin "github.com/goliatone/go-admin/pkg/admin"
	goadmin "github.com/goliatone/go-messaging/adapters/go-admin"
)

func TestChannelName(t *testing.T) {
	channel, err := ChannelName("test", "console")
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if channel != "test.console.go-admin.command-runs" {
		t.Fatalf("channel = %q", channel)
	}
	for _, invalid := range [][2]string{{"", "app"}, {"test", ""}, {"test.prod", "app"}, {"test", "app name"}} {
		if _, err := ChannelName(invalid[0], invalid[1]); !errors.Is(err, goadmin.ErrInvalidConfig) {
			t.Fatalf("ChannelName(%q, %q) error = %v", invalid[0], invalid[1], err)
		}
	}
}

func TestRoleSpecificAssemblyAndBoundedUnavailablePublish(t *testing.T) {
	for _, role := range []admin.CommandRunProcessRole{
		admin.CommandRunRolePublisher,
		admin.CommandRunRoleGateway,
		admin.CommandRunRoleMonolith,
	} {
		t.Run(role.String(), func(t *testing.T) {
			components, err := New(Config{
				Addresses: []string{"127.0.0.1:1"}, ApplicationID: "app", EnvironmentID: "test",
				Role: role, ConnectTimeout: 20 * time.Millisecond, PublishTimeout: 20 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			defer components.CloseDriver(context.Background())
			if components.Channel != "test.app.go-admin.command-runs" || components.Transport == nil {
				t.Fatalf("unexpected components: %+v", components)
			}
			if role.Has(admin.CommandRunRolePublisher) != (components.Router != nil) {
				t.Fatalf("router presence does not match role %s", role)
			}
		})
	}

	components, err := New(Config{
		Addresses: []string{"127.0.0.1:1"}, ApplicationID: "app", EnvironmentID: "test",
		Role: admin.CommandRunRolePublisher, PublishTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	started := time.Now()
	err = components.Transport.PublishCommandRun(context.Background(), validAssemblyUpdate("unavailable", 1))
	if !errors.Is(err, goadmin.ErrPublishFailed) {
		t.Fatalf("publish error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("unavailable publish was not bounded: %s", elapsed)
	}
}

func validAssemblyUpdate(runID string, revision uint64) admin.CommandRunUpdate {
	return admin.CommandRunUpdate{
		SchemaVersion: admin.CommandRunSchemaVersion,
		EventID:       "event-" + runID,
		RunID:         runID,
		Revision:      revision,
		CommandID:     "test.command",
		Phase:         admin.CommandRunPhaseSubmitted,
		OccurredAt:    time.Now().UTC(),
		Scope:         admin.CommandRunScope{ApplicationID: "app", EnvironmentID: "test"},
	}
}
