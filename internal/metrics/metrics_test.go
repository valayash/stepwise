package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("steps_total", "Steps executed.", "kind")
	c.Inc("llm")
	c.Add(2, "tool")
	c.Inc("bad", "too-many-labels") // ignored

	h := r.NewHistogramVec("call_seconds", "Call latency.", []float64{0.1, 1}, "provider")
	h.Observe(0.05, "mock")
	h.Observe(0.5, "mock")

	r.NewGaugeFunc("breaker_open", "Open breakers.", func() []Sample {
		return []Sample{{LabelVals: []string{`a"b`}, Value: 1}}
	}, "provider")

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	out := string(body)

	for _, want := range []string{
		"# TYPE steps_total counter",
		`steps_total{kind="llm"} 1`,
		`steps_total{kind="tool"} 2`,
		`call_seconds_bucket{provider="mock",le="0.1"} 1`,
		`call_seconds_bucket{provider="mock",le="1"} 2`,
		`call_seconds_bucket{provider="mock",le="+Inf"} 2`,
		`call_seconds_sum{provider="mock"} 0.55`,
		`call_seconds_count{provider="mock"} 2`,
		`breaker_open{provider="a\"b"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if c.Value("tool") != 2 {
		t.Errorf("Value = %v", c.Value("tool"))
	}
}
