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
	StreamTimeout   time.Duration // TOLLGATE_STREAM_TIMEOUT, default 5m
	MaxBodyBytes    int64         // TOLLGATE_MAX_BODY_BYTES, default 1 MiB
	MockDelay       time.Duration // TOLLGATE_MOCK_DELAY, pause between mock stream words, default 0
}

// Load reads the environment and applies defaults.
func Load() (Config, error) {
	c := Config{
		Addr:            getenv("TOLLGATE_ADDR", ":8080"),
		AnthropicAPIKey: os.Getenv("ANTHROPIC_API_KEY"),
		RequestTimeout:  60 * time.Second,
		StreamTimeout:   5 * time.Minute,
		MaxBodyBytes:    1 << 20,
	}

	for _, d := range []struct {
		env string
		dst *time.Duration
	}{
		{"TOLLGATE_REQUEST_TIMEOUT", &c.RequestTimeout},
		{"TOLLGATE_STREAM_TIMEOUT", &c.StreamTimeout},
		{"TOLLGATE_MOCK_DELAY", &c.MockDelay},
	} {
		v := os.Getenv(d.env)
		if v == "" {
			continue
		}
		dur, err := time.ParseDuration(v)
		if err != nil || dur <= 0 {
			return Config{}, fmt.Errorf("%s: must be a positive duration like 30s", d.env)
		}
		*d.dst = dur
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
