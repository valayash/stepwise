package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	anthropicDefaultBaseURL = "https://api.anthropic.com"
	anthropicVersion        = "2023-06-01"
	anthropicDefaultMax     = 1024
)

// Anthropic adapts the Anthropic Messages API to the Provider interface.
type Anthropic struct {
	apiKey  string
	baseURL string
	client  *http.Client
}

// AnthropicOption customises the adapter (used by tests to point at a fake server).
type AnthropicOption func(*Anthropic)

// WithAnthropicBaseURL overrides the API base URL.
func WithAnthropicBaseURL(u string) AnthropicOption {
	return func(a *Anthropic) { a.baseURL = strings.TrimRight(u, "/") }
}

// WithAnthropicHTTPClient overrides the HTTP client.
func WithAnthropicHTTPClient(c *http.Client) AnthropicOption {
	return func(a *Anthropic) { a.client = c }
}

// NewAnthropic builds the adapter.
func NewAnthropic(apiKey string, opts ...AnthropicOption) *Anthropic {
	a := &Anthropic{
		apiKey:  apiKey,
		baseURL: anthropicDefaultBaseURL,
		client:  &http.Client{Timeout: 120 * time.Second},
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Name implements Provider.
func (a *Anthropic) Name() string { return "anthropic" }

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicRequest struct {
	Model       string             `json:"model"`
	MaxTokens   int                `json:"max_tokens"`
	System      string             `json:"system,omitempty"`
	Messages    []anthropicMessage `json:"messages"`
	Temperature *float64           `json:"temperature,omitempty"`
}

type anthropicResponse struct {
	ID         string `json:"id"`
	StopReason string `json:"stop_reason"`
	Content    []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// Complete implements Provider.
func (a *Anthropic) Complete(ctx context.Context, req Request) (Response, error) {
	body := anthropicRequest{
		Model:       req.Model,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	}
	if body.MaxTokens <= 0 {
		body.MaxTokens = anthropicDefaultMax
	}

	// Anthropic takes system prompts as a top-level field, not a message role.
	var system []string
	for _, m := range req.Messages {
		if m.Role == "system" {
			system = append(system, m.Content)
			continue
		}
		body.Messages = append(body.Messages, anthropicMessage{Role: m.Role, Content: m.Content})
	}
	body.System = strings.Join(system, "\n\n")

	buf, err := json.Marshal(body)
	if err != nil {
		return Response{}, fmt.Errorf("encode anthropic request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(buf))
	if err != nil {
		return Response{}, fmt.Errorf("build anthropic request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("x-api-key", a.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return Response{}, fmt.Errorf("call anthropic: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return Response{}, fmt.Errorf("read anthropic response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return Response{}, &UpstreamError{Provider: a.Name(), Status: resp.StatusCode, Body: truncate(string(raw), 512)}
	}

	var ar anthropicResponse
	if err := json.Unmarshal(raw, &ar); err != nil {
		return Response{}, fmt.Errorf("decode anthropic response: %w", err)
	}

	var text strings.Builder
	for _, c := range ar.Content {
		if c.Type == "text" {
			text.WriteString(c.Text)
		}
	}

	return Response{
		ID:           ar.ID,
		Content:      text.String(),
		FinishReason: mapAnthropicStop(ar.StopReason),
		InputTokens:  ar.Usage.InputTokens,
		OutputTokens: ar.Usage.OutputTokens,
	}, nil
}

func mapAnthropicStop(s string) string {
	switch s {
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	default: // end_turn, stop_sequence
		return "stop"
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
