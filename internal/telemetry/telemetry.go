// Package telemetry sets up OpenTelemetry tracing.
//
// Every run is one trace: its trace ID is the run ID (both are 128-bit), so
// all of a run's claims — across workers, retries and crashes — land in the
// same trace, and anyone holding a run ID can open its trace directly.
package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Name is the instrumentation scope.
const Name = "github.com/valayash/stepwise"

// Tracer returns the Stepwise tracer (a no-op until Setup installs a provider).
func Tracer() trace.Tracer { return otel.Tracer(Name) }

// Setup installs an OTLP/HTTP exporter when OTEL_EXPORTER_OTLP_ENDPOINT or
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is set (standard OpenTelemetry
// variables; e.g. http://jaeger:4318). Otherwise tracing stays a no-op.
// The returned function flushes and stops the exporter.
func Setup(ctx context.Context, service, version, role string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("service.version", version),
		attribute.String("stepwise.role", role),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// RunContext returns ctx carrying a parent span context whose trace ID is
// the run ID. Spans started from it join the run's trace.
func RunContext(ctx context.Context, runID string) context.Context {
	tid, err := trace.TraceIDFromHex(runID)
	if err != nil {
		return ctx // not a 128-bit hex ID; spans get a random trace
	}
	// A stable synthetic root span ID, so every claim hangs off the same parent.
	sum := sha256.Sum256([]byte("stepwise-run:" + runID))
	var sid trace.SpanID
	copy(sid[:], sum[:8])
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

// Inject writes the W3C traceparent header for the span in ctx, so a tool
// can attach its own spans and logs to the agent run that called it.
func Inject(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// TraceID returns the trace ID in ctx, or "".
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	id := sc.TraceID()
	return hex.EncodeToString(id[:])
}
