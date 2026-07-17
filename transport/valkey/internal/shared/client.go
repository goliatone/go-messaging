package shared

import (
	"context"
	"fmt"
	"net"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

type ClientFactory func(Config) (valkey.Client, error)

func Open(config Config) (valkey.Client, error) { return OpenWith(config, nil) }

func OpenWith(config Config, factory ClientFactory) (valkey.Client, error) {
	config = config.Clone()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if factory != nil {
		return factory(config)
	}
	option := valkey.ClientOption{
		InitAddress:  config.Addresses,
		Username:     config.Username,
		Password:     config.Password,
		ClientName:   config.ClientName,
		SelectDB:     config.Database,
		TLSConfig:    config.TLSConfig,
		Dialer:       netDialer(config.ConnectTimeout),
		DisableCache: true,
	}
	client, err := valkey.NewClient(option)
	if err != nil {
		return nil, fmt.Errorf("valkey: create client: %w", err)
	}
	return client, nil
}

func Ping(ctx context.Context, client valkey.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		return Classify("ping", err)
	}
	return nil
}

func netDialer(timeout time.Duration) net.Dialer {
	return net.Dialer{Timeout: timeout}
}
