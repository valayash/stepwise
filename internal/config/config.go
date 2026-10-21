// Package config loads Tollgate settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the runtime configuration.
type Config struct {
	Addr            string        // TOLLGATE_ADDR, default ":8080"
	AnthropicAPIKey string        // ANTHROPIC_API_KEY; Anthropic routes are disabled when empty
	RequestTimeout  time.Duration // TOLLGATE_REQUEST_TIMEOUT, default 60s
	MaxBodyBytes    int64         // TOLLGATE_MAX_BODY_BYTES, default 1 MiB
}

// Load reads the environment and applies defaults.
func Load() (Config, error) {
	c := Config{
		Addr:            getenv("TOLLGATE_ADDR", ":8080"),
		AnthropicAPIKey: os.Getenv("ANTHROPIC_API_KEY"),
		RequestTimeout:  60 * time.Second,
		MaxBodyBytes:    1 << 20,
	}

	if v := os.Getenv("TOLLGATE_REQUEST_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("TOLLGATE_REQUEST_TIMEOUT: %w", err)
		}
		c.RequestTimeout = d
	}
	if v := os.Getenv("TOLLGATE_MAX_BODY_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("TOLLGATE_MAX_BODY_BYTES: must be a positive integer")
		}
		c.MaxBodyBytes = n
	}
	return c, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
