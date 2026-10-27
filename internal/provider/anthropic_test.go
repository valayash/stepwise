package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
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
			"usage":{"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":100,"cache_read_input_tokens":900}}`))
	}))
	defer srv.Close()

	a := NewAnthropic("test-key", WithAnthropicBaseURL(srv.URL))
	res, err := a.Complete(context.Background(), Request{
		Model: "claude-test",
		Messages: []Message{
			{Role: "system", Content: "be brief"},
			{Role: "user", Content: "hi"},
		},
		ResponseSchema: json.RawMessage(`{"type":"object"}`),
		Cache:          true,
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got.OutputConfig == nil || got.OutputConfig.Format.Type != "json_schema" || string(got.OutputConfig.Format.Schema) != `{"type":"object"}` {
		t.Errorf("output_config = %+v", got.OutputConfig)
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

	if got.CacheControl == nil || got.CacheControl.Type != "ephemeral" {
		t.Errorf("cache_control = %+v, want automatic ephemeral caching", got.CacheControl)
	}
	// input_tokens is only the uncached part; budgets must see the whole prompt.
	want := Response{ID: "msg_1", Content: "Hello there", FinishReason: "length", InputTokens: 1012, CachedTokens: 900, OutputTokens: 3}
	if !reflect.DeepEqual(res, want) {
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

// A realistic Anthropic event stream, including events we ignore (ping, block start/stop).
const anthropicSSE = `event: message_start
data: {"type":"message_start","message":{"id":"msg_9","usage":{"input_tokens":21,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: ping
data: {"type":"ping"}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}

event: message_stop
data: {"type":"message_stop"}

`

func sseServer(t *testing.T, body string, check func(anthropicRequest)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got anthropicRequest
		_ = json.NewDecoder(r.Body).Decode(&got)
		if check != nil {
			check(got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(body))
	}))
}

func TestAnthropicStream(t *testing.T) {
	srv := sseServer(t, anthropicSSE, func(got anthropicRequest) {
		if !got.Stream {
			t.Error("request did not set stream=true")
		}
		if got.CacheControl != nil {
			t.Error("a request without Cache (e.g. the gateway) must not pay the cache-write premium")
		}
	})
	defer srv.Close()

	var deltas []string
	res, err := NewAnthropic("k", WithAnthropicBaseURL(srv.URL)).Stream(context.Background(),
		Request{Model: "claude-x", Messages: []Message{{Role: "user", Content: "hi"}}},
		func(s string) error { deltas = append(deltas, s); return nil })
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	if strings.Join(deltas, "|") != "Hello| world" {
		t.Errorf("deltas = %q", deltas)
	}
	want := Response{ID: "msg_9", FinishReason: "stop", InputTokens: 21, OutputTokens: 2}
	if !reflect.DeepEqual(res, want) {
		t.Errorf("response = %+v, want %+v", res, want)
	}
}

func TestAnthropicStreamFailures(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		check func(error) bool
	}{
		{
			"error event",
			"data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n",
			func(err error) bool { var se *StreamError; return errors.As(err, &se) && se.Type == "overloaded_error" },
		},
		{
			"cut off before message_stop",
			"data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"a\"}}\n\n",
			func(err error) bool { return errors.Is(err, ErrIncompleteStream) },
		},
		{
			"garbage data",
			"data: {not json\n\n",
			func(err error) bool { return err != nil && strings.Contains(err.Error(), "decode") },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := sseServer(t, tc.body, nil)
			defer srv.Close()
			_, err := NewAnthropic("k", WithAnthropicBaseURL(srv.URL)).Stream(context.Background(),
				Request{Model: "claude-x", Messages: []Message{{Role: "user", Content: "hi"}}},
				func(string) error { return nil })
			if !tc.check(err) {
				t.Errorf("unexpected err: %v", err)
			}
		})
	}
}

func TestAnthropicStreamStopsWhenCallbackFails(t *testing.T) {
	srv := sseServer(t, anthropicSSE, nil)
	defer srv.Close()

	stop := errors.New("client gone")
	calls := 0
	_, err := NewAnthropic("k", WithAnthropicBaseURL(srv.URL)).Stream(context.Background(),
		Request{Model: "claude-x", Messages: []Message{{Role: "user", Content: "hi"}}},
		func(string) error { calls++; return stop })
	if !errors.Is(err, stop) || calls != 1 {
		t.Errorf("err = %v, calls = %d; want the callback error after 1 call", err, calls)
	}
}

func TestMockStream(t *testing.T) {
	var got []string
	res, err := NewMock().Stream(context.Background(),
		Request{Messages: []Message{{Role: "user", Content: "a b"}}},
		func(s string) error { got = append(got, s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, "") != "mock: a b" || len(got) != 3 {
		t.Errorf("deltas = %q", got)
	}
	if res.Content != "" || res.OutputTokens != 3 {
		t.Errorf("res = %+v", res)
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
