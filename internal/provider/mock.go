package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Mock is a deterministic, free provider for tests, demos and chaos runs.
//
// Without tools it echoes the last user message. With tools it behaves like a
// tiny agent: it calls each offered tool once, in the order given, then
// answers with a summary of the results. Tool arguments are filled from
// "key: value" pairs in the first user message, so a prompt like
// "order_id: ORD-1001, amount: 49.99" drives the demo agents end to end.
type Mock struct {
	// Delay is the pause between streamed words, to simulate generation speed.
	Delay time.Duration
	// Latency is added to every Complete call, to simulate a slow model.
	Latency time.Duration
	// FailRate is the probability (0..1) that a call fails with a retryable 503.
	FailRate float64

	seq   atomic.Int64
	calls atomic.Int64
}

// NewMock returns a ready Mock provider.
func NewMock() *Mock { return &Mock{} }

// Name implements Provider.
func (m *Mock) Name() string { return "mock" }

// Calls reports how many Complete calls were made (used by tests).
func (m *Mock) Calls() int64 { return m.calls.Load() }

// Complete implements Provider.
func (m *Mock) Complete(ctx context.Context, req Request) (Response, error) {
	m.calls.Add(1)
	if m.Latency > 0 {
		select {
		case <-time.After(m.Latency):
		case <-ctx.Done():
			return Response{}, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	if m.FailRate > 0 && rand.Float64() < m.FailRate {
		return Response{}, &UpstreamError{Provider: m.Name(), Status: 503, Body: "mock: simulated outage"}
	}

	in := 0
	var firstUser, lastUser string
	for _, msg := range req.Messages {
		in += countWords(msg.Content)
		if msg.Role == "user" {
			if firstUser == "" {
				firstUser = msg.Content
			}
			lastUser = msg.Content
		}
	}
	id := fmt.Sprintf("mock-%d", m.seq.Add(1))

	if len(req.Tools) > 0 {
		if res, ok := m.nextToolCall(id, req, firstUser, in); ok {
			return res, nil
		}
		return m.answer(id, "mock: done. "+summarizeToolResults(req.Messages), in, req.MaxTokens), nil
	}
	return m.answer(id, "mock: "+lastUser, in, req.MaxTokens), nil
}

// nextToolCall returns a call for the first offered tool not yet called.
func (m *Mock) nextToolCall(id string, req Request, prompt string, in int) (Response, bool) {
	called := map[string]bool{}
	for _, msg := range req.Messages {
		for _, tc := range msg.ToolCalls {
			called[tc.Name] = true
		}
	}
	for _, t := range req.Tools {
		if called[t.Name] {
			continue
		}
		return Response{
			ID:           id,
			Content:      "Calling " + t.Name + ".",
			ToolCalls:    []ToolCall{{ID: "call_" + id, Name: t.Name, Input: fillArgs(t.InputSchema, prompt)}},
			FinishReason: "tool_calls",
			InputTokens:  in,
			OutputTokens: 8,
		}, true
	}
	return Response{}, false
}

func (m *Mock) answer(id, text string, in, maxTokens int) Response {
	words := strings.Fields(text)
	finish := "stop"
	if maxTokens > 0 && len(words) > maxTokens {
		words = words[:maxTokens]
		finish = "length"
	}
	return Response{
		ID:           id,
		Content:      strings.Join(words, " "),
		FinishReason: finish,
		InputTokens:  in,
		OutputTokens: len(words),
	}
}

// Stream implements Provider. It emits the Complete answer one word at a time,
// pausing Delay between words (zero by default; tests and demos set it).
func (m *Mock) Stream(ctx context.Context, req Request, onDelta DeltaFunc) (Response, error) {
	req.Tools = nil // streaming is text-only
	res, err := m.Complete(ctx, req)
	if err != nil {
		return Response{}, err
	}
	words := strings.Fields(res.Content)
	for i, w := range words {
		if m.Delay > 0 {
			select {
			case <-time.After(m.Delay):
			case <-ctx.Done():
				return Response{}, ctx.Err()
			}
		}
		if err := ctx.Err(); err != nil {
			return Response{}, err
		}
		if i < len(words)-1 {
			w += " "
		}
		if err := onDelta(w); err != nil {
			return Response{}, err
		}
	}
	res.Content = ""
	return res, nil
}

// fillArgs builds a tool input from "key: value" pairs found in the prompt,
// for every property in the tool's JSON schema.
func fillArgs(schema json.RawMessage, prompt string) json.RawMessage {
	var s struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	_ = json.Unmarshal(schema, &s)
	args := map[string]any{}
	for name, p := range s.Properties {
		// A value runs to the end of its phrase: up to a comma, semicolon or newline.
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(name) + `\s*[:=]\s*"?([^,;"\n]+)`)
		mt := re.FindStringSubmatch(prompt)
		if mt == nil {
			continue
		}
		v := strings.TrimSpace(mt[1])
		if p.Type != "" && p.Type != "string" {
			if f := strings.Fields(v); len(f) > 0 {
				v = f[0]
			}
		}
		switch p.Type {
		case "number", "integer":
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				args[name] = f
			}
		case "boolean":
			args[name] = strings.EqualFold(v, "true") || v == "yes"
		default:
			args[name] = v
		}
	}
	b, _ := json.Marshal(args)
	return b
}

func summarizeToolResults(msgs []Message) string {
	var parts []string
	for _, msg := range msgs {
		if msg.Role == "tool" {
			c := msg.Content
			if len(c) > 120 {
				c = c[:120] + "..."
			}
			parts = append(parts, c)
		}
	}
	if len(parts) == 0 {
		return "No tools were needed."
	}
	return "Results: " + strings.Join(parts, " | ")
}

func countWords(s string) int { return len(strings.Fields(s)) }
