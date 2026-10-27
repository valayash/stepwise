package config

import (
	"testing"
	"time"
)

var allVars = []string{
	"STEPWISE_ADDR", "STEPWISE_DATABASE_URL", "STEPWISE_API_TOKEN", "STEPWISE_MAX_BODY_BYTES",
	"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "STEPWISE_REQUEST_TIMEOUT", "STEPWISE_STREAM_TIMEOUT",
	"STEPWISE_MOCK_DELAY", "STEPWISE_MOCK_LATENCY", "STEPWISE_MOCK_FAIL_RATE", "STEPWISE_WORKERS",
	"STEPWISE_LEASE", "STEPWISE_MAX_ATTEMPTS", "STEPWISE_RATE_LIMITS", "STEPWISE_WEBHOOK_SECRET",
	"STEPWISE_TRIAGE", "STEPWISE_TRIAGE_MODEL", "STEPWISE_TRIAGE_AUTO_RETRY", "STEPWISE_LOG_LEVEL",
	"STEPWISE_POLL_INTERVAL", "STEPWISE_BACKOFF_BASE", "STEPWISE_BACKOFF_MAX", "STEPWISE_REDIS_URL",
	"STEPWISE_METRICS_ADDR", "STEPWISE_DEMO_SHOP_URL",
}

func clear(t *testing.T) {
	for _, k := range allVars {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clear(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" || c.Workers != 8 || c.Lease != 30*time.Second || c.MaxAttempts != 6 ||
		!c.TriageEnabled || c.TriageAutoRetry || c.TriageModel != "mock" || c.MaxBodyBytes != 1<<20 ||
		c.RequestTimeout != time.Minute || c.StreamTimeout != 5*time.Minute || c.DatabaseURL != "" {
		t.Errorf("defaults = %+v", c)
	}
}

func TestLoadOverrides(t *testing.T) {
	clear(t)
	t.Setenv("STEPWISE_WORKERS", "32")
	t.Setenv("STEPWISE_LEASE", "10s")
	t.Setenv("STEPWISE_MOCK_FAIL_RATE", "0.25")
	t.Setenv("STEPWISE_RATE_LIMITS", "claude-=rpm:50,tpm:40000")
	t.Setenv("STEPWISE_TRIAGE", "false")
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-x")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Workers != 32 || c.Lease != 10*time.Second || c.MockFailRate != 0.25 || c.RateLimits != "claude-=rpm:50,tpm:40000" || c.TriageEnabled {
		t.Errorf("overrides not applied: %+v", c)
	}
	if c.TriageModel != "claude-sonnet-5-5" {
		t.Errorf("triage model should default to Claude when a key is set, got %q", c.TriageModel)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	cases := map[string]string{
		"STEPWISE_REQUEST_TIMEOUT": "soon",
		"STEPWISE_LEASE":           "-1s",
		"STEPWISE_MAX_BODY_BYTES":  "0",
		"STEPWISE_WORKERS":         "zero",
		"STEPWISE_MOCK_FAIL_RATE":  "2",
		"STEPWISE_TRIAGE":          "maybe",
	}
	for k, v := range cases {
		t.Run(k, func(t *testing.T) {
			clear(t)
			t.Setenv(k, v)
			if _, err := Load(); err == nil {
				t.Errorf("%s=%q accepted", k, v)
			}
		})
	}
}
