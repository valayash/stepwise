// Package provider defines the provider-neutral interface Stepwise calls,
// plus the concrete LLM provider adapters (Anthropic, OpenAI, mock).
package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Message is a provider-neutral chat message.
//
// Roles:
//   - "system", "user": plain text in Content.
//   - "assistant": optional text in Content plus any ToolCalls the model made.
//   - "tool": the result of one tool call; ToolCallID links it to the call.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	IsError    bool       `json:"is_error,omitempty"` // tool result reports a failure
}

// ToolDef describes a tool the model may call. InputSchema is a JSON Schema object.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// ToolCall is the model asking to run a tool. Input is a JSON object.
type ToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// Request is a provider-neutral completion request.
type Request struct {
	Model       string
	Messages    []Message
	Tools       []ToolDef
	MaxTokens   int
	Temperature *float64
	// ResponseSchema, if set, constrains the final answer to JSON matching it.
	ResponseSchema json.RawMessage
	// Cache asks the provider to cache the prompt prefix. Worth it when the same
	// conversation comes back (agent turns); a one-off call would only pay the
	// cache-write premium.
	Cache bool
}

// Response is a provider-neutral completion result.
type Response struct {
	ID           string     `json:"id"`
	Content      string     `json:"content,omitempty"`
	ToolCalls    []ToolCall `json:"tool_calls,omitempty"`
	FinishReason string     `json:"finish_reason"`           // OpenAI vocabulary: "stop", "length", "tool_calls"
	InputTokens  int        `json:"input_tokens"`            // the whole prompt, cached or not
	CachedTokens int        `json:"cached_tokens,omitempty"` // part of InputTokens read from the provider's cache
	OutputTokens int        `json:"output_tokens"`
}

// DeltaFunc receives each piece of generated text during a stream.
// Returning an error aborts the stream.
type DeltaFunc func(text string) error

// Provider is anything that can complete a chat request.
type Provider interface {
	Name() string
	// Complete returns the whole answer at once, including any tool calls.
	Complete(ctx context.Context, req Request) (Response, error)
	// Stream calls onDelta for each text piece as it is generated, then returns
	// the final metadata (ID, finish reason, token usage). Content is left empty:
	// the caller has already seen every piece. Streaming is text-only.
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

// UpstreamError is a non-2xx answer from a provider. Callers use Status to
// decide whether the error is retryable (5xx, 429) or the caller's fault.
type UpstreamError struct {
	Provider   string
	Status     int
	Body       string
	RetryAfter string // raw Retry-After header, when the provider sent one
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("%s returned %d: %s", e.Provider, e.Status, e.Body)
}

// Retryable reports whether an error is worth retrying later: rate limits,
// provider-side failures, timeouts and broken connections. Bad requests
// (4xx other than 429) are not: sending them again gives the same answer.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var ue *UpstreamError
	if errors.As(err, &ue) {
		return ue.Status == 429 || ue.Status == 408 || ue.Status == 409 || ue.Status >= 500
	}
	var se *StreamError
	if errors.As(err, &se) {
		return se.Type == "overloaded_error" || se.Type == "api_error" || se.Type == "rate_limit_error"
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Timeouts, connection resets, truncated streams.
	return true
}

// RateLimited reports whether the provider refused the call for quota (429):
// the provider is up, we are just going too fast.
func RateLimited(err error) bool {
	var ue *UpstreamError
	var se *StreamError
	return errors.As(err, &ue) && ue.Status == 429 || errors.As(err, &se) && se.Type == "rate_limit_error"
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

// Models lists the configured prefixes, for error messages and docs.
func (r *Router) Models() []string {
	out := make([]string, len(r.routes))
	for i, rt := range r.routes {
		out[i] = rt.Prefix + "*"
	}
	return out
}
