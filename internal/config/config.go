// Package config loads Stepwise settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/valayash/stepwise/internal/ratelimit"
)

// Config is the runtime configuration. Every field maps to one variable.
type Config struct {
	Addr         string // STEPWISE_ADDR, default ":8080"
	DatabaseURL  string // STEPWISE_DATABASE_URL; empty = in-memory store (dev only)
	RedisURL     string // STEPWISE_REDIS_URL; shares rate limits across all workers
	MetricsAddr  string // STEPWISE_METRICS_ADDR; metrics + health for worker-only processes
	DemoShopURL  string // STEPWISE_DEMO_SHOP_URL; MCP URL the UI's example run uses
	APIToken     string // STEPWISE_API_TOKEN; empty = no auth
	MaxBodyBytes int64  // STEPWISE_MAX_BODY_BYTES, default 1 MiB

	// Providers.
	AnthropicAPIKey string        // ANTHROPIC_API_KEY; enables claude-* models
	OpenAIAPIKey    string        // OPENAI_API_KEY; enables gpt-*, o* models
	RequestTimeout  time.Duration // STEPWISE_REQUEST_TIMEOUT, gateway non-streaming, default 60s
	StreamTimeout   time.Duration // STEPWISE_STREAM_TIMEOUT, gateway streaming, default 5m
	MockDelay       time.Duration // STEPWISE_MOCK_DELAY, pause between mock stream words
	MockLatency     time.Duration // STEPWISE_MOCK_LATENCY, delay per mock call
	MockFailRate    float64       // STEPWISE_MOCK_FAIL_RATE, 0..1 simulated outages

	// Engine.
	Workers      int             // STEPWISE_WORKERS, concurrent runs per process, default 8
	Lease        time.Duration   // STEPWISE_LEASE, default 30s
	MaxAttempts  int             // STEPWISE_MAX_ATTEMPTS, tries per step, default 6
	PollInterval time.Duration   // STEPWISE_POLL_INTERVAL, idle polling, default 1s
	BackoffBase  time.Duration   // STEPWISE_BACKOFF_BASE, first retry delay, default 1s
	BackoffMax   time.Duration   // STEPWISE_BACKOFF_MAX, retry delay cap, default 2m
	RateLimits   ratelimit.Rules // STEPWISE_RATE_LIMITS, e.g. "claude-=rpm:50,tpm:40000"

	// Webhooks and triage.
	WebhookSecret   string // STEPWISE_WEBHOOK_SECRET; signs deliveries
	TriageEnabled   bool   // STEPWISE_TRIAGE, default true
	TriageModel     string // STEPWISE_TRIAGE_MODEL, default: claude-sonnet-4-5 if a key is set, else mock
	TriageAutoRetry bool   // STEPWISE_TRIAGE_AUTO_RETRY, default false (a human approves retries)

	LogLevel string // STEPWISE_LOG_LEVEL: debug, info, warn, error
}

// Load reads the environment and applies defaults.
func Load() (Config, error) {
	c := Config{
		Addr:            getenv("STEPWISE_ADDR", ":8080"),
		DatabaseURL:     os.Getenv("STEPWISE_DATABASE_URL"),
		RedisURL:        os.Getenv("STEPWISE_REDIS_URL"),
		MetricsAddr:     os.Getenv("STEPWISE_METRICS_ADDR"),
		DemoShopURL:     os.Getenv("STEPWISE_DEMO_SHOP_URL"),
		APIToken:        os.Getenv("STEPWISE_API_TOKEN"),
		MaxBodyBytes:    1 << 20,
		AnthropicAPIKey: os.Getenv("ANTHROPIC_API_KEY"),
		OpenAIAPIKey:    os.Getenv("OPENAI_API_KEY"),
		RequestTimeout:  60 * time.Second,
		StreamTimeout:   5 * time.Minute,
		Workers:         8,
		Lease:           30 * time.Second,
		MaxAttempts:     6,
		WebhookSecret:   os.Getenv("STEPWISE_WEBHOOK_SECRET"),
		TriageEnabled:   true,
		TriageModel:     os.Getenv("STEPWISE_TRIAGE_MODEL"),
		LogLevel:        getenv("STEPWISE_LOG_LEVEL", "info"),
	}

	durations := []struct {
		env string
		dst *time.Duration
	}{
		{"STEPWISE_REQUEST_TIMEOUT", &c.RequestTimeout},
		{"STEPWISE_STREAM_TIMEOUT", &c.StreamTimeout},
		{"STEPWISE_MOCK_DELAY", &c.MockDelay},
		{"STEPWISE_MOCK_LATENCY", &c.MockLatency},
		{"STEPWISE_LEASE", &c.Lease},
		{"STEPWISE_POLL_INTERVAL", &c.PollInterval},
		{"STEPWISE_BACKOFF_BASE", &c.BackoffBase},
		{"STEPWISE_BACKOFF_MAX", &c.BackoffMax},
	}
	for _, d := range durations {
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

	ints := []struct {
		env string
		dst *int
	}{
		{"STEPWISE_WORKERS", &c.Workers},
		{"STEPWISE_MAX_ATTEMPTS", &c.MaxAttempts},
	}
	for _, i := range ints {
		if v := os.Getenv(i.env); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return Config{}, fmt.Errorf("%s: must be a positive integer", i.env)
			}
			*i.dst = n
		}
	}

	if v := os.Getenv("STEPWISE_MAX_BODY_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("STEPWISE_MAX_BODY_BYTES: must be a positive integer")
		}
		c.MaxBodyBytes = n
	}
	if v := os.Getenv("STEPWISE_MOCK_FAIL_RATE"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || f > 1 {
			return Config{}, fmt.Errorf("STEPWISE_MOCK_FAIL_RATE: must be between 0 and 1")
		}
		c.MockFailRate = f
	}
	for env, dst := range map[string]*bool{"STEPWISE_TRIAGE": &c.TriageEnabled, "STEPWISE_TRIAGE_AUTO_RETRY": &c.TriageAutoRetry} {
		if v := os.Getenv(env); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return Config{}, fmt.Errorf("%s: must be true or false", env)
			}
			*dst = b
		}
	}
	if v := os.Getenv("STEPWISE_RATE_LIMITS"); v != "" {
		rules, err := ratelimit.ParseRules(v)
		if err != nil {
			return Config{}, fmt.Errorf("STEPWISE_RATE_LIMITS: %w", err)
		}
		c.RateLimits = rules
	}
	if c.TriageModel == "" {
		c.TriageModel = "mock"
		if c.AnthropicAPIKey != "" {
			c.TriageModel = "claude-sonnet-4-5"
		}
	}
	return c, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
