package valkey

import "testing"

func TestConfigValidationAndClone(t *testing.T) {
	config := DefaultConfig("127.0.0.1:6379")
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	clone := config.Clone()
	clone.Addresses[0] = "other"
	if config.Addresses[0] == "other" {
		t.Fatal("clone shares addresses")
	}
	bad := DefaultConfig()
	if err := bad.Validate(); err == nil {
		t.Fatal("expected missing address")
	}
}
