package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/telemetry"
)

// A run that is picked up by two different claims (here: a deferred rate
// limit forces a second claim) is still exactly one trace whose ID is the
// run ID, with model and tool spans inside it, and tools receive traceparent.
func TestTracesFollowTheRunAcrossClaims(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	var mu sync.Mutex
	var traceparent string
	tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		traceparent = r.Header.Get("traceparent")
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer tool.Close()

	h := newHarness(t)
	h.deps.Limiter = &stubLimiter{} // defers once → two claims
	h.start()
	run := h.create(core.AgentSpec{Model: "mock", Tools: []core.ToolSpec{{
		Type: core.ToolHTTP, Name: "lookup", URL: tool.URL, InputSchema: []byte(`{"type":"object","properties":{}}`),
	}}}, "hello", 0)
	h.waitStatus(run.ID, core.RunCompleted)
	h.shutdown()
	_ = tp.ForceFlush(context.Background())

	var claims, chats, toolSpans int
	for _, s := range rec.Ended() {
		if got := s.SpanContext().TraceID().String(); got != run.ID {
			t.Errorf("span %q has trace %s, want the run ID %s", s.Name(), got, run.ID)
		}
		switch {
		case s.Name() == "run.claim":
			claims++
		case strings.HasPrefix(s.Name(), "chat "):
			chats++
			attrs := map[string]any{}
			for _, kv := range s.Attributes() {
				attrs[string(kv.Key)] = kv.Value.AsInterface()
			}
			if attrs["gen_ai.request.model"] != "mock" || attrs["gen_ai.system"] != "mock" {
				t.Errorf("chat span attributes = %v", attrs)
			}
			if _, ok := attrs["gen_ai.usage.input_tokens"]; !ok {
				t.Error("chat span has no token usage")
			}
		case s.Name() == "execute_tool lookup":
			toolSpans++
		}
	}
	if claims != 2 || chats != 2 || toolSpans != 1 {
		t.Errorf("spans: %d claims, %d chat, %d tool; want 2, 2, 1", claims, chats, toolSpans)
	}

	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(traceparent, run.ID) {
		t.Errorf("tool got traceparent %q, want it to carry trace %s", traceparent, run.ID)
	}
}

func TestRunContextIsStable(t *testing.T) {
	id := core.NewID()
	a := telemetry.TraceID(telemetry.RunContext(context.Background(), id))
	b := telemetry.TraceID(telemetry.RunContext(context.Background(), id))
	if a != id || b != id {
		t.Errorf("trace IDs %s / %s, want %s", a, b, id)
	}
	if telemetry.TraceID(telemetry.RunContext(context.Background(), "not-hex")) != "" {
		t.Error("a non-hex run ID must not produce a trace context")
	}
	_ = provider.NewMock()
	_ = time.Second
}
