package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

var ctx = context.Background()

func echoServer() *Server {
	return NewServer("test", "1",
		ServerTool{
			Tool: Tool{Name: "echo", Description: "Echo the message", InputSchema: json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}}}`)},
			Handler: func(_ context.Context, args json.RawMessage, meta Meta) (string, error) {
				var a struct{ Msg string }
				_ = json.Unmarshal(args, &a)
				return a.Msg + "|" + meta.IdempotencyKey, nil
			},
		},
		ServerTool{
			Tool: Tool{Name: "fail"},
			Handler: func(context.Context, json.RawMessage, Meta) (string, error) {
				return "", errors.New("order not found")
			},
		},
	)
}

func TestClientServerRoundTrip(t *testing.T) {
	srv := httptest.NewServer(echoServer())
	defer srv.Close()
	c := NewClient(nil)

	tools, err := c.ListTools(ctx, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 || tools[0].Name != "echo" || !strings.Contains(string(tools[0].InputSchema), "msg") {
		t.Fatalf("tools = %+v", tools)
	}

	res, err := c.CallTool(ctx, srv.URL, "echo", json.RawMessage(`{"msg":"hi"}`), "run1:call1")
	if err != nil || res.Text != "hi|run1:call1" || res.IsError {
		t.Fatalf("echo = %+v, %v", res, err)
	}

	res, err = c.CallTool(ctx, srv.URL, "fail", nil, "")
	if err != nil || !res.IsError || res.Text != "order not found" {
		t.Fatalf("tool error should come back as isError, got %+v, %v", res, err)
	}

	_, err = c.CallTool(ctx, srv.URL, "missing", nil, "")
	if err == nil || Retryable(err) {
		t.Fatalf("unknown tool should be a non-retryable protocol error: %v", err)
	}
}

func TestClientReinitializesExpiredSession(t *testing.T) {
	var inits, expired atomic.Int32
	inner := echoServer()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var peek struct{ Method string }
		raw, _ := readAll(r)
		_ = json.Unmarshal(raw, &peek)
		if peek.Method == "initialize" {
			inits.Add(1)
		}
		// The first tools/call after the first session hits an "expired" session.
		if peek.Method == "tools/call" && expired.Add(1) == 1 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		r.Body = nopCloser{strings.NewReader(string(raw))}
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()

	c := NewClient(nil)
	res, err := c.CallTool(ctx, srv.URL, "echo", json.RawMessage(`{"msg":"again"}`), "")
	if err != nil || res.Text != "again|" {
		t.Fatalf("call = %+v, %v", res, err)
	}
	if inits.Load() != 2 {
		t.Errorf("initialize called %d times, want 2 (once more after 404)", inits.Load())
	}
}

func TestClientReadsSSEResponses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := readAll(r)
		var req struct {
			ID     *int64 `json:"id"`
			Method string `json:"method"`
		}
		_ = json.Unmarshal(raw, &req)
		if req.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		result := `{"tools":[]}`
		if req.Method == "tools/call" {
			result = `{"content":[{"type":"text","text":"streamed"}]}`
		}
		// A progress notification first, then the real response.
		fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n")
		fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":%s}\n\n", *req.ID, result)
	}))
	defer srv.Close()

	res, err := NewClient(nil).CallTool(ctx, srv.URL, "x", nil, "")
	if err != nil || res.Text != "streamed" {
		t.Fatalf("res = %+v, %v", res, err)
	}
}

func TestTransportErrorsAreRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	_, err := NewClient(nil).CallTool(ctx, srv.URL, "x", nil, "")
	if !Retryable(err) {
		t.Fatalf("503 should be retryable: %v", err)
	}
}

func TestServerRejectsGet(t *testing.T) {
	rec := httptest.NewRecorder()
	echoServer().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET = %d", rec.Code)
	}
}

type nopCloser struct{ *strings.Reader }

func (nopCloser) Close() error { return nil }

func readAll(r *http.Request) ([]byte, error) {
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			return []byte(sb.String()), nil
		}
	}
}
