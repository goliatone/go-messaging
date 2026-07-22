// Package valkey assembles the go-admin messaging adapter with Valkey Pub/Sub.
package valkey

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strings"
	"time"

	admin "github.com/goliatone/go-admin/pkg/admin"
	messaging "github.com/goliatone/go-messaging"
	goadmin "github.com/goliatone/go-messaging/adapters/go-admin"
	"github.com/goliatone/go-messaging/transport/valkey/pubsub"
)

const (
	// LogicalRoute is the stable application-level command-run route.
	LogicalRoute          = "go-admin-command-runs"
	defaultPublishTimeout = 2 * time.Second
)

var channelSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Config contains provider settings without leaking provider types through the
// returned go-admin transport contract.
type Config struct {
	Addresses        []string
	Username         string
	Password         string
	ClientName       string
	Database         int
	TLSConfig        *tls.Config
	ConnectTimeout   time.Duration
	OperationTimeout time.Duration
	ReconnectMin     time.Duration
	ReconnectMax     time.Duration
	MaxPayloadBytes  int
	MaxMessageBytes  int
	QueueSize        int
	PublishTimeout   time.Duration

	ApplicationID  string
	EnvironmentID  string
	Role           admin.CommandRunProcessRole
	TransportName  string
	ErrorBuffer    int
	ContractLimits admin.CommandRunContractLimits
}

// Components are assembled but not started. The host must start Driver before
// starting the go-admin runtime, then close runtime subscriptions before Driver.
type Components struct {
	Driver    messaging.Driver
	Router    *messaging.Router
	Transport *goadmin.Transport
	Channel   string
	Route     string
}

// New assembles role-specific publisher/subscriber wiring around one host-owned driver.
func New(config Config) (*Components, error) {
	config.ApplicationID = strings.TrimSpace(config.ApplicationID)
	config.EnvironmentID = strings.TrimSpace(config.EnvironmentID)
	if !config.Role.Valid() {
		return nil, fmt.Errorf("%w: role is invalid", goadmin.ErrInvalidConfig)
	}
	channel, err := ChannelName(config.EnvironmentID, config.ApplicationID)
	if err != nil {
		return nil, err
	}

	driverConfig := pubsub.DefaultConfig(config.Addresses...)
	driverConfig.Valkey.Username = config.Username
	driverConfig.Valkey.Password = config.Password
	driverConfig.Valkey.ClientName = config.ClientName
	driverConfig.Valkey.Database = config.Database
	if config.TLSConfig != nil {
		driverConfig.Valkey.TLSConfig = config.TLSConfig.Clone()
	}
	if config.ConnectTimeout > 0 {
		driverConfig.Valkey.ConnectTimeout = config.ConnectTimeout
	}
	if config.OperationTimeout > 0 {
		driverConfig.Valkey.OperationTimeout = config.OperationTimeout
	}
	if config.ReconnectMin > 0 {
		driverConfig.Valkey.ReconnectMin = config.ReconnectMin
	}
	if config.ReconnectMax > 0 {
		driverConfig.Valkey.ReconnectMax = config.ReconnectMax
	}
	if config.MaxMessageBytes > 0 {
		driverConfig.Valkey.MaxMessageBytes = config.MaxMessageBytes
	}
	if config.QueueSize > 0 {
		driverConfig.QueueSize = config.QueueSize
	}
	driver, err := pubsub.New(driverConfig)
	if err != nil {
		return nil, fmt.Errorf("%w: Valkey driver configuration", goadmin.ErrInvalidConfig)
	}
	drivers, err := messaging.NewDriverRegistry(map[string]messaging.Driver{"valkey-pubsub": driver})
	if err != nil {
		return nil, fmt.Errorf("%w: driver registry", goadmin.ErrInvalidConfig)
	}
	codec, err := goadmin.NewCodec(goadmin.CodecConfig{
		ApplicationID: config.ApplicationID, EnvironmentID: config.EnvironmentID,
		MaxPayloadBytes: config.MaxPayloadBytes, ContractLimits: config.ContractLimits,
	})
	if err != nil {
		return nil, err
	}

	transportConfig := goadmin.TransportConfig{
		Name: config.TransportName, Drivers: drivers, Codec: codec, ErrorBuffer: config.ErrorBuffer,
	}
	var router *messaging.Router
	if config.Role.Has(admin.CommandRunRolePublisher) {
		publishTimeout := config.PublishTimeout
		if publishTimeout <= 0 {
			publishTimeout = defaultPublishTimeout
		}
		router, err = messaging.NewRouter(drivers, []messaging.Route{{
			Name: LogicalRoute, Strategy: messaging.StrategyPrimary,
			Bindings: []messaging.RouteBinding{{
				Driver: "valkey-pubsub", Destination: messaging.Destination{Name: channel},
			}},
			Kinds: []messaging.Kind{messaging.KindEvent}, Types: []string{goadmin.MessageType},
			Timeout: publishTimeout, MaxMessageBytes: config.MaxPayloadBytes,
		}}, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: publisher route", goadmin.ErrInvalidConfig)
		}
		transportConfig.Router = router
		transportConfig.PublishRoute = LogicalRoute
	}
	if config.Role.Has(admin.CommandRunRoleGateway) {
		transportConfig.Sources = []goadmin.SourceBinding{{
			Name: "valkey-command-runs", LogicalRoute: LogicalRoute, Driver: "valkey-pubsub",
			Source: messaging.Source{Name: channel},
		}}
	}
	transport, err := goadmin.NewTransport(transportConfig)
	if err != nil {
		return nil, err
	}
	return &Components{
		Driver: driver, Router: router, Transport: transport, Channel: channel, Route: LogicalRoute,
	}, nil
}

// ChannelName returns the application/environment Pub/Sub fanout channel.
func ChannelName(environmentID, applicationID string) (string, error) {
	environmentID = strings.TrimSpace(environmentID)
	applicationID = strings.TrimSpace(applicationID)
	if !channelSegmentPattern.MatchString(environmentID) || !channelSegmentPattern.MatchString(applicationID) {
		return "", fmt.Errorf("%w: application and environment must be channel-safe identifiers", goadmin.ErrInvalidConfig)
	}
	return environmentID + "." + applicationID + ".go-admin.command-runs", nil
}

// StartDriver is a small convenience that preserves explicit host ownership.
func (c *Components) StartDriver(ctx context.Context) error {
	if c == nil || c.Driver == nil {
		return goadmin.ErrInvalidConfig
	}
	return c.Driver.Start(ctx)
}

// CloseDriver closes only the host-owned driver. Runtime subscriptions must be closed first.
func (c *Components) CloseDriver(ctx context.Context) error {
	if c == nil || c.Driver == nil {
		return nil
	}
	return c.Driver.Close(ctx)
}
