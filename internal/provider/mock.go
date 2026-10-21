package provider

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Mock is a deterministic provider for tests, local dev and load testing.
// It echoes the last user message and counts tokens as whitespace-separated words.
type Mock struct {
	// Delay is the pause between streamed words, to simulate generation speed.
	Delay time.Duration

	seq atomic.Int64
}

// NewMock returns a ready Mock provider.
func NewMock() *Mock { return &Mock{} }

// Name implements Provider.
func (m *Mock) Name() string { return "mock" }

// Complete implements Provider.
func (m *Mock) Complete(ctx context.Context, req Request) (Response, error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}

	var lastUser string
	in := 0
	for _, msg := range req.Messages {
		in += countWords(msg.Content)
		if msg.Role == "user" {
			lastUser = msg.Content
		}
	}

	out := "mock: " + lastUser
	words := strings.Fields(out)
	finish := "stop"
	if req.MaxTokens > 0 && len(words) > req.MaxTokens {
		words = words[:req.MaxTokens]
		out = strings.Join(words, " ")
		finish = "length"
	}

	return Response{
		ID:           fmt.Sprintf("mock-%d", m.seq.Add(1)),
		Content:      out,
		FinishReason: finish,
		InputTokens:  in,
		OutputTokens: len(words),
	}, nil
}

// Stream implements Provider. It emits the Complete answer one word at a time,
// pausing Delay between words (zero by default; tests and load runs set it).
func (m *Mock) Stream(ctx context.Context, req Request, onDelta DeltaFunc) (Response, error) {
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

func countWords(s string) int { return len(strings.Fields(s)) }
