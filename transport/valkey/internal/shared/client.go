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
		client, err := factory(config)
		if err != nil {
			return nil, err
		}
		return withOperationTimeout(client, config.OperationTimeout), nil
	}
	client, err := valkey.NewClient(clientOption(config))
	if err != nil {
		return nil, fmt.Errorf("valkey: create client: %w", err)
	}
	return withOperationTimeout(client, config.OperationTimeout), nil
}

func clientOption(config Config) valkey.ClientOption {
	return valkey.ClientOption{
		InitAddress:      config.Addresses,
		Username:         config.Username,
		Password:         config.Password,
		ClientName:       config.ClientName,
		SelectDB:         config.Database,
		TLSConfig:        config.TLSConfig,
		Dialer:           netDialer(config.ConnectTimeout),
		ConnWriteTimeout: config.OperationTimeout,
		RetryDelay:       boundedRetryDelay(config.ReconnectMin, config.ReconnectMax),
		DisableCache:     true,
	}
}

func boundedRetryDelay(minimum, maximum time.Duration) valkey.RetryDelayFn {
	return func(attempts int, _ valkey.Completed, _ error) time.Duration {
		delay := minimum
		for range max(attempts, 0) {
			if delay >= maximum || delay > maximum/2 {
				return maximum
			}
			delay *= 2
		}
		return min(delay, maximum)
	}
}

type operationTimeoutClient struct {
	valkey.Client
	timeout time.Duration
}

func withOperationTimeout(client valkey.Client, timeout time.Duration) valkey.Client {
	return &operationTimeoutClient{Client: client, timeout: timeout}
}

func (c *operationTimeoutClient) Do(ctx context.Context, command valkey.Completed) valkey.ValkeyResult {
	if ctx == nil {
		ctx = context.Background()
	}
	bounded, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.Client.Do(bounded, command)
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
