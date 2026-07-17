package shared

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Addresses        []string
	Username         string
	Password         string
	ClientName       string
	Database         int
	TLSConfig        *tls.Config
	ConnectTimeout   time.Duration
	OperationTimeout time.Duration
	MaxMessageBytes  int
	ReconnectMin     time.Duration
	ReconnectMax     time.Duration
}

func DefaultConfig(addresses ...string) Config {
	return Config{
		Addresses:        append([]string(nil), addresses...),
		ConnectTimeout:   5 * time.Second,
		OperationTimeout: 10 * time.Second,
		MaxMessageBytes:  4 << 20,
		ReconnectMin:     100 * time.Millisecond,
		ReconnectMax:     5 * time.Second,
	}
}

func (c Config) Clone() Config {
	c.Addresses = append([]string(nil), c.Addresses...)
	if c.TLSConfig != nil {
		c.TLSConfig = c.TLSConfig.Clone()
	}
	return c
}

func (c Config) Validate() error {
	if len(c.Addresses) == 0 {
		return fmt.Errorf("valkey: at least one address is required")
	}
	for _, address := range c.Addresses {
		if strings.TrimSpace(address) == "" {
			return fmt.Errorf("valkey: address must not be empty")
		}
	}
	if c.Database < 0 || c.ConnectTimeout <= 0 || c.OperationTimeout <= 0 || c.MaxMessageBytes <= 0 {
		return fmt.Errorf("valkey: database and limits are invalid")
	}
	if c.ReconnectMin <= 0 || c.ReconnectMax < c.ReconnectMin {
		return fmt.Errorf("valkey: reconnect bounds are invalid")
	}
	return nil
}
