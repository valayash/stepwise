// Package provider defines the provider-neutral interface Tollgate routes to,
// plus the concrete LLM provider adapters.
package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Message is a provider-neutral chat message.
type Message struct {
	Role    string // "system", "user" or "assistant"
	Content string
}

// Request is a provider-neutral completion request.
type Request struct {
	Model       string
	Messages    []Message
	MaxTokens   int
	Temperature *float64
}

// Response is a provider-neutral completion result.
type Response struct {
	ID           string
	Content      string
	FinishReason string // OpenAI vocabulary: "stop", "length", "tool_calls"
	InputTokens  int
	OutputTokens int
}

// DeltaFunc receives each piece of generated text during a stream.
// Returning an error aborts the stream.
type DeltaFunc func(text string) error

// Provider is anything that can complete a chat request.
type Provider interface {
	Name() string
	// Complete returns the whole answer at once.
	Complete(ctx context.Context, req Request) (Response, error)
	// Stream calls onDelta for each text piece as it is generated, then returns
	// the final metadata (ID, finish reason, token usage). Content is left empty:
	// the caller has already seen every piece.
	Stream(ctx context.Context, req Request, onDelta DeltaFunc) (Response, error)
}

// ErrUnknownModel is returned when no provider serves the requested model.
var ErrUnknownModel = errors.New("unknown model")

// ErrIncompleteStream means the provider closed the stream without a final event.
var ErrIncompleteStream = errors.New("stream ended before completion")

// StreamError is an error event sent by a provider after streaming started.
type StreamError struct {
	Provider string
	Type     string
	Message  string
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("%s stream error (%s): %s", e.Provider, e.Type, e.Message)
}

// UpstreamError is a non-2xx answer from a provider. The gateway uses Status
// to decide whether the error is retryable (5xx, 429) or the caller's fault.
type UpstreamError struct {
	Provider string
	Status   int
	Body     string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("%s returned %d: %s", e.Provider, e.Status, e.Body)
}

// Route maps a model-name prefix to a provider.
type Route struct {
	Prefix   string
	Provider Provider
}

// Router picks a provider by model prefix. The first matching route wins.
type Router struct {
	routes []Route
}

// NewRouter builds a router from routes in priority order.
func NewRouter(routes ...Route) *Router {
	return &Router{routes: routes}
}

// For returns the provider that serves model.
func (r *Router) For(model string) (Provider, error) {
	for _, rt := range r.routes {
		if strings.HasPrefix(model, rt.Prefix) {
			return rt.Provider, nil
		}
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownModel, model)
}
