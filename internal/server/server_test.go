package server

import (
	"context"
	"encoding/json"
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

type stubProvider struct {
	err   error
	delay time.Duration
	// deltas are emitted by Stream before it returns err (if any).
	deltas []string
}

func (s stubProvider) Name() string { return "stub" }

func (s stubProvider) Stream(ctx context.Context, _ provider.Request, onDelta provider.DeltaFunc) (provider.Response, error) {
	for _, d := range s.deltas {
		if err := onDelta(d); err != nil {
			return provider.Response{}, err
		}
	}
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return provider.Response{}, ctx.Err()
		}
	}
	if s.err != nil {
		return provider.Response{}, s.err
	}
	return provider.Response{ID: "x", FinishReason: "stop", InputTokens: 1, OutputTokens: len(s.deltas)}, nil
}

func (s stubProvider) Complete(ctx context.Context, _ provider.Request) (provider.Response, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return provider.Response{}, ctx.Err()
		}
	}
	if s.err != nil {
		return provider.Response{}, s.err
	}
	return provider.Response{ID: "x", Content: "ok", FinishReason: "stop"}, nil
}

func newTestServer(routes ...provider.Route) http.Handler {
	if len(routes) == 0 {
		routes = []provider.Route{{Prefix: "mock", Provider: provider.NewMock()}}
	}
	return New(Options{
		Router:         provider.NewRouter(routes...),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		RequestTimeout: 200 * time.Millisecond,
		MaxBodyBytes:   1 << 10,
	}).Handler()
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestChatCompletionsHappyPath(t *testing.T) {
	rec := post(t, newTestServer(), `{"model":"mock-1","messages":[{"role":"user","content":"hello world"}]}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("missing X-Request-ID header")
	}

	var res api.ChatCompletionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Object != "chat.completion" || res.Model != "mock-1" {
		t.Errorf("unexpected envelope: %+v", res)
	}
	if got := res.Choices[0].Message.Content; got != "mock: hello world" {
		t.Errorf("content = %q", got)
	}
	if res.Usage.TotalTokens != res.Usage.PromptTokens+res.Usage.CompletionTokens {
		t.Errorf("usage totals don't add up: %+v", res.Usage)
	}
}

func TestChatCompletionsErrors(t *testing.T) {
	cases := []struct {
		name     string
		routes   []provider.Route
		body     string
		wantCode int
		wantErr  string
	}{
		{"bad json", nil, `{`, http.StatusBadRequest, "invalid_json"},
		{"missing model", nil, `{"messages":[{"role":"user","content":"x"}]}`, http.StatusBadRequest, "invalid_request"},
		{"empty messages", nil, `{"model":"mock","messages":[]}`, http.StatusBadRequest, "invalid_request"},
		{"bad role", nil, `{"model":"mock","messages":[{"role":"tool","content":"x"}]}`, http.StatusBadRequest, "invalid_request"},
		{"zero max_tokens", nil, `{"model":"mock","max_tokens":0,"messages":[{"role":"user","content":"x"}]}`, http.StatusBadRequest, "invalid_request"},
		{"unknown model", nil, `{"model":"gpt-9","messages":[{"role":"user","content":"x"}]}`, http.StatusNotFound, "model_not_found"},
		{"body too large", nil, `{"model":"mock","messages":[{"role":"user","content":"` + strings.Repeat("a", 2048) + `"}]}`, http.StatusRequestEntityTooLarge, "request_too_large"},
		{
			"upstream 429", []provider.Route{{Prefix: "s", Provider: stubProvider{err: &provider.UpstreamError{Status: 429}}}},
			`{"model":"s","messages":[{"role":"user","content":"x"}]}`, http.StatusTooManyRequests, "upstream_rate_limited",
		},
		{
			"upstream 500", []provider.Route{{Prefix: "s", Provider: stubProvider{err: &provider.UpstreamError{Status: 500}}}},
			`{"model":"s","messages":[{"role":"user","content":"x"}]}`, http.StatusBadGateway, "upstream_failed",
		},
		{
			"upstream timeout", []provider.Route{{Prefix: "s", Provider: stubProvider{delay: time.Second}}},
			`{"model":"s","messages":[{"role":"user","content":"x"}]}`, http.StatusGatewayTimeout, "upstream_timeout",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := post(t, newTestServer(tc.routes...), tc.body)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantCode, rec.Body)
			}
			var er api.ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &er); err != nil {
				t.Fatalf("error body not JSON: %s", rec.Body)
			}
			if er.Error.Code != tc.wantErr {
				t.Errorf("code = %q, want %q", er.Error.Code, tc.wantErr)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestServer().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestRequestIDPassthrough(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "abc-123")
	rec := httptest.NewRecorder()
	newTestServer().ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
		t.Errorf("X-Request-ID = %q, want abc-123", got)
	}
}
