package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{"TOLLGATE_ADDR", "TOLLGATE_REQUEST_TIMEOUT", "TOLLGATE_STREAM_TIMEOUT",
		"TOLLGATE_MAX_BODY_BYTES", "TOLLGATE_MOCK_DELAY", "ANTHROPIC_API_KEY"} {
		t.Setenv(k, "")
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" || c.RequestTimeout != time.Minute || c.StreamTimeout != 5*time.Minute ||
		c.MaxBodyBytes != 1<<20 || c.MockDelay != 0 {
		t.Errorf("defaults = %+v", c)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("TOLLGATE_ADDR", ":9000")
	t.Setenv("TOLLGATE_STREAM_TIMEOUT", "90s")
	t.Setenv("TOLLGATE_MOCK_DELAY", "50ms")
	t.Setenv("TOLLGATE_MAX_BODY_BYTES", "2048")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":9000" || c.StreamTimeout != 90*time.Second || c.MockDelay != 50*time.Millisecond || c.MaxBodyBytes != 2048 {
		t.Errorf("overrides not applied: %+v", c)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	cases := map[string]string{
		"TOLLGATE_REQUEST_TIMEOUT": "soon",
		"TOLLGATE_STREAM_TIMEOUT":  "-1s",
		"TOLLGATE_MAX_BODY_BYTES":  "0",
	}
	for k, v := range cases {
		t.Run(k, func(t *testing.T) {
			t.Setenv(k, v)
			if _, err := Load(); err == nil {
				t.Errorf("%s=%q accepted", k, v)
			}
		})
	}
}
