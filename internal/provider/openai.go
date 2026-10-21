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

const openAIDefaultBaseURL = "https://api.openai.com"

// OpenAI adapts the OpenAI Chat Completions API to the Provider interface.
type OpenAI struct {
	apiKey  string
	baseURL string
	client  *http.Client
}

// OpenAIOption customises the adapter.
type OpenAIOption func(*OpenAI)

// WithOpenAIBaseURL overrides the API base URL (tests, Azure-style proxies, local servers).
func WithOpenAIBaseURL(u string) OpenAIOption {
	return func(o *OpenAI) { o.baseURL = strings.TrimRight(u, "/") }
}

// NewOpenAI builds the adapter. Like the Anthropic adapter, deadlines come
// from the request context rather than a client-wide timeout.
func NewOpenAI(apiKey string, opts ...OpenAIOption) *OpenAI {
	o := &OpenAI{apiKey: apiKey, baseURL: openAIDefaultBaseURL, client: &http.Client{}}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Name implements Provider.
func (o *OpenAI) Name() string { return "openai" }

type openAIToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"` // a JSON object encoded as a string
	} `json:"function"`
}

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    *string          `json:"content"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
}

type openAITool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

type openAIRequest struct {
	Model         string          `json:"model"`
	Messages      []openAIMessage `json:"messages"`
	Tools         []openAITool    `json:"tools,omitempty"`
	MaxTokens     int             `json:"max_completion_tokens,omitempty"`
	Temperature   *float64        `json:"temperature,omitempty"`
	Stream        bool            `json:"stream,omitempty"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options,omitempty"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type openAIResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Message      openAIMessage `json:"message"`
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage openAIUsage `json:"usage"`
}

func strPtr(s string) *string { return &s }

func toOpenAIMessages(msgs []Message) []openAIMessage {
	out := make([]openAIMessage, 0, len(msgs))
	for _, m := range msgs {
		om := openAIMessage{Role: m.Role, ToolCallID: m.ToolCallID}
		switch {
		case m.Role == "assistant" && len(m.ToolCalls) > 0:
			if m.Content != "" {
				om.Content = strPtr(m.Content)
			}
			for _, tc := range m.ToolCalls {
				otc := openAIToolCall{ID: tc.ID, Type: "function"}
				otc.Function.Name = tc.Name
				otc.Function.Arguments = string(jsonObject(tc.Input))
				om.ToolCalls = append(om.ToolCalls, otc)
			}
		case m.Role == "tool" && m.IsError:
			om.Content = strPtr("ERROR: " + m.Content)
		default:
			om.Content = strPtr(m.Content)
		}
		out = append(out, om)
	}
	return out
}

func (o *OpenAI) buildBody(req Request, stream bool) openAIRequest {
	body := openAIRequest{
		Model:       req.Model,
		Messages:    toOpenAIMessages(req.Messages),
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		Stream:      stream,
	}
	for _, t := range req.Tools {
		ot := openAITool{Type: "function"}
		ot.Function.Name = t.Name
		ot.Function.Description = t.Description
		ot.Function.Parameters = jsonObject(t.InputSchema)
		body.Tools = append(body.Tools, ot)
	}
	if stream {
		body.StreamOptions = &struct {
			IncludeUsage bool `json:"include_usage"`
		}{IncludeUsage: true}
	}
	return body
}

// Complete implements Provider.
func (o *OpenAI) Complete(ctx context.Context, req Request) (Response, error) {
	resp, err := o.send(ctx, o.buildBody(req, false))
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return Response{}, fmt.Errorf("read openai response: %w", err)
	}
	var or openAIResponse
	if err := json.Unmarshal(raw, &or); err != nil {
		return Response{}, fmt.Errorf("decode openai response: %w", err)
	}
	if len(or.Choices) == 0 {
		return Response{}, fmt.Errorf("openai response has no choices")
	}

	ch := or.Choices[0]
	out := Response{
		ID:           or.ID,
		FinishReason: mapOpenAIFinish(ch.FinishReason),
		InputTokens:  or.Usage.PromptTokens,
		OutputTokens: or.Usage.CompletionTokens,
	}
	if ch.Message.Content != nil {
		out.Content = *ch.Message.Content
	}
	for _, tc := range ch.Message.ToolCalls {
		args := json.RawMessage(tc.Function.Arguments)
		if !json.Valid(args) {
			// Models occasionally emit broken JSON; hand the tool an empty object
			// rather than failing the whole step.
			args = json.RawMessage(`{}`)
		}
		out.ToolCalls = append(out.ToolCalls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Input: jsonObject(args)})
	}
	return out, nil
}

// Stream implements Provider (text only).
func (o *OpenAI) Stream(ctx context.Context, req Request, onDelta DeltaFunc) (Response, error) {
	resp, err := o.send(ctx, o.buildBody(req, true))
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	var out Response
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), maxSSELine)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			if out.FinishReason == "" {
				out.FinishReason = "stop"
			}
			return out, nil
		}
		var ev struct {
			ID      string `json:"id"`
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *openAIUsage `json:"usage"`
			Error *struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return out, fmt.Errorf("decode openai stream event: %w", err)
		}
		if ev.Error != nil {
			return out, &StreamError{Provider: o.Name(), Type: ev.Error.Type, Message: ev.Error.Message}
		}
		if out.ID == "" {
			out.ID = ev.ID
		}
		for _, c := range ev.Choices {
			if c.Delta.Content != "" {
				if err := onDelta(c.Delta.Content); err != nil {
					return out, err
				}
			}
			if c.FinishReason != nil {
				out.FinishReason = mapOpenAIFinish(*c.FinishReason)
			}
		}
		if ev.Usage != nil {
			out.InputTokens = ev.Usage.PromptTokens
			out.OutputTokens = ev.Usage.CompletionTokens
		}
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := sc.Err(); err != nil {
		return out, fmt.Errorf("read openai stream: %w", err)
	}
	return out, ErrIncompleteStream
}

func (o *OpenAI) send(ctx context.Context, body openAIRequest) (*http.Response, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode openai request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/v1/chat/completions", bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("build openai request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+o.apiKey)

	resp, err := o.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call openai: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, &UpstreamError{
			Provider: o.Name(), Status: resp.StatusCode, Body: truncate(string(raw), 512),
			RetryAfter: resp.Header.Get("Retry-After"),
		}
	}
	return resp, nil
}

func mapOpenAIFinish(s string) string {
	switch s {
	case "length", "tool_calls":
		return s
	case "function_call":
		return "tool_calls"
	default: // stop, content_filter, ""
		return "stop"
	}
}
