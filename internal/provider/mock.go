package provider

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
)

// Mock is a deterministic provider for tests, local dev and load testing.
// It echoes the last user message and counts tokens as whitespace-separated words.
type Mock struct {
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

func countWords(s string) int { return len(strings.Fields(s)) }
