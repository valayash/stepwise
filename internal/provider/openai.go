package provider

import (
	"context"
	"encoding/json"
	"fmt"
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
	ResponseFormat *openAIResponseFormat `json:"response_format,omitempty"`
}

// openAIResponseFormat carries structured output: {"type": "json_schema", "json_schema": {...}}.
type openAIResponseFormat struct {
	Type       string           `json:"type"`
	JSONSchema openAIJSONSchema `json:"json_schema"`
}

type openAIJSONSchema struct {
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
	Strict bool            `json:"strict"`
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
	if len(req.ResponseSchema) > 0 {
		body.ResponseFormat = &openAIResponseFormat{Type: "json_schema",
			JSONSchema: openAIJSONSchema{Name: "response", Schema: req.ResponseSchema, Strict: true}}
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
	var or openAIResponse
	if err := decodeBody(resp, o.Name(), &or); err != nil {
		return Response{}, err
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
	var out Response
	err = readSSE(ctx, resp, o.Name(), func(data string) (bool, error) {
		if data == "[DONE]" {
			if out.FinishReason == "" {
				out.FinishReason = "stop"
			}
			return true, nil
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
			return false, fmt.Errorf("decode openai stream event: %w", err)
		}
		if ev.Error != nil {
			return false, &StreamError{Provider: o.Name(), Type: ev.Error.Type, Message: ev.Error.Message}
		}
		if out.ID == "" {
			out.ID = ev.ID
		}
		for _, c := range ev.Choices {
			if c.Delta.Content != "" {
				if err := onDelta(c.Delta.Content); err != nil {
					return false, err
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
		return false, nil
	})
	return out, err
}

func (o *OpenAI) send(ctx context.Context, body openAIRequest) (*http.Response, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+o.apiKey)
	return postJSON(ctx, o.client, o.Name(), o.baseURL+"/v1/chat/completions", header, body)
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
