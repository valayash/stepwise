package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnthropicComplete(t *testing.T) {
	var got anthropicRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path = %q, want /v1/messages", r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "test-key" {
			t.Errorf("missing api key header")
		}
		if r.Header.Get("anthropic-version") != anthropicVersion {
			t.Errorf("missing anthropic-version header")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		_, _ = w.Write([]byte(`{"id":"msg_1","stop_reason":"max_tokens",
			"content":[{"type":"text","text":"Hello "},{"type":"text","text":"there"}],
			"usage":{"input_tokens":12,"output_tokens":3}}`))
	}))
	defer srv.Close()

	a := NewAnthropic("test-key", WithAnthropicBaseURL(srv.URL))
	res, err := a.Complete(context.Background(), Request{
		Model: "claude-test",
		Messages: []Message{
			{Role: "system", Content: "be brief"},
			{Role: "user", Content: "hi"},
		},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if got.System != "be brief" {
		t.Errorf("system = %q, want lifted to top-level field", got.System)
	}
	if len(got.Messages) != 1 || got.Messages[0].Role != "user" {
		t.Errorf("messages = %+v, want only the user message", got.Messages)
	}
	if got.MaxTokens != anthropicDefaultMax {
		t.Errorf("max_tokens = %d, want default %d", got.MaxTokens, anthropicDefaultMax)
	}

	want := Response{ID: "msg_1", Content: "Hello there", FinishReason: "length", InputTokens: 12, OutputTokens: 3}
	if res != want {
		t.Errorf("response = %+v, want %+v", res, want)
	}
}

func TestAnthropicUpstreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_error"}}`))
	}))
	defer srv.Close()

	a := NewAnthropic("k", WithAnthropicBaseURL(srv.URL))
	_, err := a.Complete(context.Background(), Request{Model: "claude-test", Messages: []Message{{Role: "user", Content: "x"}}})

	var ue *UpstreamError
	if !errors.As(err, &ue) || ue.Status != http.StatusTooManyRequests {
		t.Fatalf("err = %v, want UpstreamError 429", err)
	}
}

func TestMapAnthropicStop(t *testing.T) {
	cases := map[string]string{
		"end_turn":      "stop",
		"stop_sequence": "stop",
		"max_tokens":    "length",
		"tool_use":      "tool_calls",
		"":              "stop",
	}
	for in, want := range cases {
		if got := mapAnthropicStop(in); got != want {
			t.Errorf("mapAnthropicStop(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRouter(t *testing.T) {
	m := NewMock()
	r := NewRouter(Route{Prefix: "mock", Provider: m})

	if p, err := r.For("mock-small"); err != nil || p != m {
		t.Errorf("For(mock-small) = %v, %v", p, err)
	}
	if _, err := r.For("gpt-x"); !errors.Is(err, ErrUnknownModel) {
		t.Errorf("For(gpt-x) err = %v, want ErrUnknownModel", err)
	}
}

func TestMockTruncatesAtMaxTokens(t *testing.T) {
	res, err := NewMock().Complete(context.Background(), Request{
		MaxTokens: 2,
		Messages:  []Message{{Role: "user", Content: "one two three"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.FinishReason != "length" || res.OutputTokens != 2 {
		t.Errorf("got %+v, want 2 tokens with finish=length", res)
	}
}
