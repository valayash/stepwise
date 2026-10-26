package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

// anthropicMessage.Content is either a plain string or a list of content
// blocks (text, tool_use, tool_result).
type anthropicMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type anthropicBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`          // tool_use
	Name      string          `json:"name,omitempty"`        // tool_use
	Input     json.RawMessage `json:"input,omitempty"`       // tool_use
	ToolUseID string          `json:"tool_use_id,omitempty"` // tool_result
	Content   string          `json:"content,omitempty"`     // tool_result
	IsError   bool            `json:"is_error,omitempty"`    // tool_result
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type anthropicRequest struct {
	Model       string             `json:"model"`
	MaxTokens   int                `json:"max_tokens"`
	System      string             `json:"system,omitempty"`
	Messages    []anthropicMessage `json:"messages"`
	Tools       []anthropicTool    `json:"tools,omitempty"`
	Temperature *float64           `json:"temperature,omitempty"`
	Stream      bool               `json:"stream,omitempty"`
}

type anthropicResponse struct {
	ID         string           `json:"id"`
	StopReason string           `json:"stop_reason"`
	Content    []anthropicBlock `json:"content"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// toAnthropicMessages converts neutral messages into Anthropic's shape:
//   - system messages become the top-level system field;
//   - assistant tool calls become tool_use blocks;
//   - consecutive tool results are merged into ONE user message of
//     tool_result blocks, because Anthropic requires every result for a turn
//     to arrive together right after the assistant's tool_use turn.
func toAnthropicMessages(msgs []Message) (system string, out []anthropicMessage) {
	var sys []string
	for _, m := range msgs {
		switch m.Role {
		case "system":
			sys = append(sys, m.Content)
		case "assistant":
			if len(m.ToolCalls) == 0 {
				out = append(out, anthropicMessage{Role: "assistant", Content: m.Content})
				continue
			}
			var blocks []anthropicBlock
			if m.Content != "" {
				blocks = append(blocks, anthropicBlock{Type: "text", Text: m.Content})
			}
			for _, tc := range m.ToolCalls {
				blocks = append(blocks, anthropicBlock{Type: "tool_use", ID: tc.ID, Name: tc.Name, Input: jsonObject(tc.Input)})
			}
			out = append(out, anthropicMessage{Role: "assistant", Content: blocks})
		case "tool":
			res := anthropicBlock{Type: "tool_result", ToolUseID: m.ToolCallID, Content: m.Content, IsError: m.IsError}
			if n := len(out); n > 0 && out[n-1].Role == "user" {
				if blocks, ok := out[n-1].Content.([]anthropicBlock); ok && len(blocks) > 0 && blocks[0].Type == "tool_result" {
					out[n-1].Content = append(blocks, res)
					continue
				}
			}
			out = append(out, anthropicMessage{Role: "user", Content: []anthropicBlock{res}})
		default: // user
			out = append(out, anthropicMessage{Role: "user", Content: m.Content})
		}
	}
	return strings.Join(sys, "\n\n"), out
}

// jsonObject guarantees a JSON object: providers reject a missing or null tool input.
func jsonObject(raw json.RawMessage) json.RawMessage {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || bytes.Equal(t, []byte("null")) {
		return json.RawMessage(`{}`)
	}
	return raw
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
	var ar anthropicResponse
	if err := decodeBody(resp, a.Name(), &ar); err != nil {
		return Response{}, err
	}

	var text strings.Builder
	var calls []ToolCall
	for _, c := range ar.Content {
		switch c.Type {
		case "text":
			text.WriteString(c.Text)
		case "tool_use":
			calls = append(calls, ToolCall{ID: c.ID, Name: c.Name, Input: jsonObject(c.Input)})
		}
	}

	return Response{
		ID:           ar.ID,
		Content:      text.String(),
		ToolCalls:    calls,
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
	var out Response
	// SSE frames look like "event: <name>\ndata: <json>\n\n". The JSON
	// repeats the event name in "type", so the data lines are enough.
	err = readSSE(ctx, resp, a.Name(), func(data string) (bool, error) {
		var ev anthropicStreamEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return false, fmt.Errorf("decode anthropic stream event: %w", err)
		}
		switch ev.Type {
		case "message_start":
			out.ID = ev.Message.ID
			out.InputTokens = ev.Message.Usage.InputTokens
		case "content_block_delta":
			if ev.Delta.Type == "text_delta" && ev.Delta.Text != "" {
				return false, onDelta(ev.Delta.Text)
			}
		case "message_delta":
			out.FinishReason = mapAnthropicStop(ev.Delta.StopReason)
			out.OutputTokens = ev.Usage.OutputTokens // cumulative count
		case "message_stop":
			return true, nil
		case "error":
			return false, &StreamError{Provider: a.Name(), Type: ev.Error.Type, Message: ev.Error.Message}
		}
		// ping, content_block_start, content_block_stop: nothing to do.
		return false, nil
	})
	return out, err
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

	body.System, body.Messages = toAnthropicMessages(req.Messages)
	for _, t := range req.Tools {
		body.Tools = append(body.Tools, anthropicTool{Name: t.Name, Description: t.Description, InputSchema: jsonObject(t.InputSchema)})
	}

	header := http.Header{}
	header.Set("x-api-key", a.apiKey)
	header.Set("anthropic-version", anthropicVersion)
	if stream {
		header.Set("accept", "text/event-stream")
	}
	return postJSON(ctx, a.client, a.Name(), a.baseURL+"/v1/messages", header, body)
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
