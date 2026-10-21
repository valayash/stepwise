package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/valayash/tollgate/internal/api"
	"github.com/valayash/tollgate/internal/provider"
)

// readEvents splits an SSE body into its data payloads.
func readEvents(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			out = append(out, data)
		}
	}
	return out
}

func decodeChunk(t *testing.T, data string) api.ChatCompletionChunk {
	t.Helper()
	var c api.ChatCompletionChunk
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		t.Fatalf("chunk %q: %v", data, err)
	}
	return c
}

func TestStreamHappyPath(t *testing.T) {
	rec := post(t, newTestServer(), `{"model":"mock-1","stream":true,
		"stream_options":{"include_usage":true},
		"messages":[{"role":"user","content":"hello streaming world"}]}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}

	events := readEvents(t, rec.Body.String())
	if events[len(events)-1] != "[DONE]" {
		t.Fatalf("last event = %q, want [DONE]", events[len(events)-1])
	}

	first := decodeChunk(t, events[0])
	if first.Object != "chat.completion.chunk" || first.Choices[0].Delta.Role != "assistant" {
		t.Errorf("first chunk should announce the assistant role: %+v", first)
	}

	var text strings.Builder
	var finish string
	var usage *api.Usage
	for _, e := range events[1 : len(events)-1] {
		c := decodeChunk(t, e)
		if c.ID != first.ID {
			t.Errorf("chunk id changed mid-stream: %q vs %q", c.ID, first.ID)
		}
		if c.Usage != nil {
			usage = c.Usage
			if len(c.Choices) != 0 {
				t.Errorf("usage chunk must have empty choices")
			}
			continue
		}
		text.WriteString(c.Choices[0].Delta.Content)
		if f := c.Choices[0].FinishReason; f != nil {
			finish = *f
		}
	}

	if got := text.String(); got != "mock: hello streaming world" {
		t.Errorf("streamed text = %q", got)
	}
	if finish != "stop" {
		t.Errorf("finish_reason = %q, want stop", finish)
	}
	if usage == nil || usage.CompletionTokens != 4 || usage.TotalTokens != usage.PromptTokens+usage.CompletionTokens {
		t.Errorf("usage = %+v", usage)
	}
}

func TestStreamWithoutUsageOption(t *testing.T) {
	rec := post(t, newTestServer(), `{"model":"mock","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	for _, e := range readEvents(t, rec.Body.String()) {
		if e != "[DONE]" && decodeChunk(t, e).Usage != nil {
			t.Fatal("usage chunk sent without stream_options.include_usage")
		}
	}
}

func TestStreamErrorBeforeFirstToken(t *testing.T) {
	h := newTestServer(provider.Route{Prefix: "s", Provider: stubProvider{err: &provider.UpstreamError{Status: 429}}})
	rec := post(t, h, `{"model":"s","stream":true,"messages":[{"role":"user","content":"x"}]}`)

	// Nothing was streamed yet, so the client gets a plain JSON error with a real status.
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func TestStreamErrorMidStream(t *testing.T) {
	h := newTestServer(provider.Route{Prefix: "s", Provider: stubProvider{
		deltas: []string{"partial "},
		err:    &provider.StreamError{Provider: "stub", Type: "overloaded_error"},
	}})
	rec := post(t, h, `{"model":"s","stream":true,"messages":[{"role":"user","content":"x"}]}`)

	events := readEvents(t, rec.Body.String())
	last := events[len(events)-1]
	if last == "[DONE]" {
		t.Fatal("an interrupted stream must not end with [DONE]")
	}
	var er api.ErrorResponse
	if err := json.Unmarshal([]byte(last), &er); err != nil || er.Error.Code != "stream_interrupted" {
		t.Fatalf("last event = %q, want stream_interrupted error", last)
	}
}

// TestStreamClientDisconnectCancelsUpstream runs a real HTTP stack: client →
// gateway → fake Anthropic. When the client hangs up after the first token, the
// gateway must cancel its upstream request so no more tokens are paid for.
func TestStreamClientDisconnectCancelsUpstream(t *testing.T) {
	upstreamGone := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"usage\":{\"input_tokens\":3}}}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"first\"}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done() // hold the stream open until the gateway hangs up
		close(upstreamGone)
	}))
	defer upstream.Close()

	gw := httptest.NewServer(New(Options{
		Router: provider.NewRouter(provider.Route{
			Prefix: "claude-", Provider: provider.NewAnthropic("k", provider.WithAnthropicBaseURL(upstream.URL)),
		}),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}).Handler())
	defer gw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, gw.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"claude-x","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read until the first content token arrives, then hang up.
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.Contains(sc.Text(), `"content":"first"`) {
			break
		}
	}
	cancel()

	select {
	case <-upstreamGone:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request was not cancelled after the client disconnected")
	}
}
