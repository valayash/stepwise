package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	anthropicDefaultBaseURL = "https://api.anthropic.com"
	anthropicVersion        = "2023-06-01"
	anthropicDefaultMax     = 1024
	maxSSELine              = 1 << 20 // 1 MiB: largest single SSE line we accept
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

// NewAnthropic builds the adapter. The HTTP client has no overall timeout on
// purpose: http.Client.Timeout also covers reading the body, which would cut
// long streams. Deadlines come from the request context instead.
func NewAnthropic(apiKey string, opts ...AnthropicOption) *Anthropic {
	a := &Anthropic{
		apiKey:  apiKey,
		baseURL: anthropicDefaultBaseURL,
		client:  &http.Client{},
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
	Stream      bool               `json:"stream,omitempty"`
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

// anthropicStreamEvent covers every streaming event type we read; each event
// fills only the fields relevant to it.
type anthropicStreamEvent struct {
	Type    string `json:"type"`
	Message struct {
		ID    string `json:"id"`
		Usage struct {
			InputTokens int `json:"input_tokens"`
		} `json:"usage"`
	} `json:"message"` // message_start
	Delta struct {
		Type       string `json:"type"`
		Text       string `json:"text"`
		StopReason string `json:"stop_reason"`
	} `json:"delta"` // content_block_delta, message_delta
	Usage struct {
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"` // message_delta
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"` // error
}

// Complete implements Provider.
func (a *Anthropic) Complete(ctx context.Context, req Request) (Response, error) {
	resp, err := a.send(ctx, req, false)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return Response{}, fmt.Errorf("read anthropic response: %w", err)
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

// Stream implements Provider by reading Anthropic's server-sent events.
func (a *Anthropic) Stream(ctx context.Context, req Request, onDelta DeltaFunc) (Response, error) {
	resp, err := a.send(ctx, req, true)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	var out Response
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), maxSSELine)

	for sc.Scan() {
		// SSE frames look like "event: <name>\ndata: <json>\n\n". The JSON
		// repeats the event name in "type", so the data lines are enough.
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue
		}
		var ev anthropicStreamEvent
		if err := json.Unmarshal([]byte(strings.TrimSpace(data)), &ev); err != nil {
			return out, fmt.Errorf("decode anthropic stream event: %w", err)
		}

		switch ev.Type {
		case "message_start":
			out.ID = ev.Message.ID
			out.InputTokens = ev.Message.Usage.InputTokens
		case "content_block_delta":
			if ev.Delta.Type == "text_delta" && ev.Delta.Text != "" {
				if err := onDelta(ev.Delta.Text); err != nil {
					return out, err
				}
			}
		case "message_delta":
			out.FinishReason = mapAnthropicStop(ev.Delta.StopReason)
			out.OutputTokens = ev.Usage.OutputTokens // cumulative count
		case "message_stop":
			return out, nil
		case "error":
			return out, &StreamError{Provider: a.Name(), Type: ev.Error.Type, Message: ev.Error.Message}
		}
		// ping, content_block_start, content_block_stop: nothing to do.
	}

	if err := ctx.Err(); err != nil {
		return out, err // client left or deadline hit; the body read failed because of it
	}
	if err := sc.Err(); err != nil {
		return out, fmt.Errorf("read anthropic stream: %w", err)
	}
	return out, ErrIncompleteStream
}

// send builds and sends the request, turning non-2xx answers into UpstreamError.
// On success the caller owns resp.Body.
func (a *Anthropic) send(ctx context.Context, req Request, stream bool) (*http.Response, error) {
	body := anthropicRequest{
		Model:       req.Model,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		Stream:      stream,
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
		return nil, fmt.Errorf("encode anthropic request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("build anthropic request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("x-api-key", a.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	if stream {
		httpReq.Header.Set("accept", "text/event-stream")
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call anthropic: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, &UpstreamError{Provider: a.Name(), Status: resp.StatusCode, Body: truncate(string(raw), 512)}
	}
	return resp, nil
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
