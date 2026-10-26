package tools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/mcp"
)

var ctx = context.Background()

func TestHTTPToolOutcomes(t *testing.T) {
	var gotKey, gotRun string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotRun = r.Header.Get("Idempotency-Key"), r.Header.Get("X-Stepwise-Run-Id")
		switch r.URL.Path {
		case "/ok":
			var in map[string]any
			_ = json.NewDecoder(r.Body).Decode(&in)
			_ = json.NewEncoder(w).Encode(map[string]any{"echo": in["order_id"]})
		case "/notfound":
			http.Error(w, "no such order", http.StatusNotFound)
		case "/down":
			http.Error(w, "maintenance", http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	r := NewRunner()
	call := func(path string) (Result, error) {
		return r.Run(ctx, Call{
			RunID: "run1", IdempotencyKey: "run1:call1", Input: json.RawMessage(`{"order_id":"ORD-1"}`),
			Spec: core.ToolSpec{Type: core.ToolHTTP, Name: "t", URL: srv.URL + path},
		})
	}

	res, err := call("/ok")
	if err != nil || res.IsError || !strings.Contains(res.Text, "ORD-1") {
		t.Fatalf("ok: %+v %v", res, err)
	}
	if gotKey != "run1:call1" || gotRun != "run1" {
		t.Errorf("headers: key=%q run=%q", gotKey, gotRun)
	}

	res, err = call("/notfound")
	if err != nil || !res.IsError || !strings.Contains(res.Text, "404") {
		t.Errorf("4xx should be a tool error for the model: %+v %v", res, err)
	}

	_, err = call("/down")
	if !IsTransient(err) {
		t.Errorf("5xx should be transient: %v", err)
	}
}

func TestHTTPToolUnreachableIsTransient(t *testing.T) {
	_, err := NewRunner().Run(ctx, Call{Spec: core.ToolSpec{Type: core.ToolHTTP, Name: "t", URL: "http://127.0.0.1:1"}})
	if !IsTransient(err) {
		t.Errorf("connection refused should be transient: %v", err)
	}
}

func TestLongResultsAreTruncated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("a", MaxResultBytes*2)))
	}))
	defer srv.Close()
	res, _ := NewRunner().Run(ctx, Call{Spec: core.ToolSpec{Type: core.ToolHTTP, Name: "t", URL: srv.URL}})
	if len(res.Text) > MaxResultBytes+20 || !strings.HasSuffix(res.Text, "[truncated]") {
		t.Errorf("result length %d not truncated", len(res.Text))
	}
}

func TestBuiltinAndUnknownTypes(t *testing.T) {
	r := NewRunner()
	r.Builtins["add"] = func(context.Context, Call) (Result, error) { return Result{Text: "3"}, nil }
	r.Builtins["flaky"] = func(context.Context, Call) (Result, error) { return Result{}, errors.New("db timeout") }

	if res, err := r.Run(ctx, Call{Spec: core.ToolSpec{Type: core.ToolBuiltin, Name: "add"}}); err != nil || res.Text != "3" {
		t.Errorf("builtin = %+v %v", res, err)
	}
	if _, err := r.Run(ctx, Call{Spec: core.ToolSpec{Type: core.ToolBuiltin, Name: "flaky"}}); !IsTransient(err) {
		t.Errorf("builtin errors are transient: %v", err)
	}
	if res, _ := r.Run(ctx, Call{Spec: core.ToolSpec{Type: core.ToolAgent, Name: "x"}}); !res.IsError {
		t.Error("agent tools are executed by the engine, not the runner")
	}
}

func TestMCPToolAndPrepare(t *testing.T) {
	srv := httptest.NewServer(mcp.NewServer("shop", "1",
		mcp.ServerTool{Tool: mcp.Tool{Name: "get_order", Description: "Look up", InputSchema: json.RawMessage(`{"type":"object"}`)},
			Handler: func(_ context.Context, _ json.RawMessage, m mcp.Meta) (string, error) {
				return "order ok " + m.IdempotencyKey, nil
			}},
		mcp.ServerTool{Tool: mcp.Tool{Name: "issue_refund"},
			Handler: func(context.Context, json.RawMessage, mcp.Meta) (string, error) { return "", errors.New("too late") }},
		mcp.ServerTool{Tool: mcp.Tool{Name: "internal_admin"},
			Handler: func(context.Context, json.RawMessage, mcp.Meta) (string, error) { return "", nil }},
	))
	defer srv.Close()
	r := NewRunner()

	spec, err := r.Prepare(ctx, core.AgentSpec{
		Model:           "mock",
		Tools:           []core.ToolSpec{{Type: core.ToolMCP, URL: srv.URL, Include: []string{"get_order", "issue_refund"}}},
		RequireApproval: []string{"issue_refund"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Tools) != 2 || spec.Tools[0].Name != "get_order" || spec.Tools[0].URL != srv.URL {
		t.Fatalf("expanded tools = %+v", spec.Tools)
	}
	if spec.MaxTurns != core.DefaultMaxTurns {
		t.Error("defaults not applied")
	}

	res, err := r.Run(ctx, Call{Spec: spec.Tools[0], IdempotencyKey: "k1"})
	if err != nil || res.Text != "order ok k1" {
		t.Errorf("mcp call = %+v %v", res, err)
	}
	res, err = r.Run(ctx, Call{Spec: spec.Tools[1]})
	if err != nil || !res.IsError || res.Text != "too late" {
		t.Errorf("mcp tool error = %+v %v", res, err)
	}
}

func TestPrepareValidation(t *testing.T) {
	r := NewRunner()
	bad := []core.AgentSpec{
		{},
		{Model: "m", Tools: []core.ToolSpec{{Type: core.ToolHTTP, Name: "x"}}},
		{Model: "m", Tools: []core.ToolSpec{{Type: core.ToolHTTP, Name: "bad name", URL: "http://x"}}},
		{Model: "m", Tools: []core.ToolSpec{{Type: core.ToolHTTP, Name: "a", URL: "http://x"}, {Type: core.ToolHTTP, Name: "a", URL: "http://y"}}},
		{Model: "m", Tools: []core.ToolSpec{{Type: core.ToolAgent, Name: "sub"}}},
		{Model: "m", Tools: []core.ToolSpec{{Type: "shell", Name: "s"}}},
		{Model: "m", RequireApproval: []string{"ghost"}},
		{Model: "m", Limits: map[string]int{"ghost": 1}},
		{Model: "m", ApprovalTimeout: "soon"},
		{Model: "m", Tools: []core.ToolSpec{{Type: core.ToolHTTP, Name: "a", URL: "http://x"}}, Limits: map[string]int{"a": 0}},
		{Model: "m", Tools: []core.ToolSpec{{Type: core.ToolMCP, URL: "http://127.0.0.1:1"}}},
	}
	for i, s := range bad {
		if _, err := r.Prepare(ctx, s); !errors.Is(err, core.ErrInvalid) {
			t.Errorf("spec %d accepted: %v", i, err)
		}
	}

	ok, err := r.Prepare(ctx, core.AgentSpec{Model: "m", Tools: []core.ToolSpec{{
		Type: core.ToolAgent, Name: "researcher", Agent: &core.AgentSpec{Model: "m"},
	}}})
	if err != nil || ok.Tools[0].Description == "" || ok.Tools[0].Agent.MaxTurns == 0 {
		t.Errorf("sub-agent spec = %+v, %v", ok.Tools, err)
	}
}

func TestToolHeadersResolveSecretsForOwnHostOnly(t *testing.T) {
	t.Setenv("STEPWISE_SECRET_SHOP", "s3cret")
	t.Setenv("STEPWISE_NOT_SECRET", "leak")
	var leaked string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked = r.Header.Get("Authorization") }))
	defer other.Close()
	var got []string // Authorization seen by the tool: once for MCP list, once per call
	mcpSrv := mcp.NewServer("shop", "1", mcp.ServerTool{Tool: mcp.Tool{Name: "ping"},
		Handler: func(context.Context, json.RawMessage, mcp.Meta) (string, error) { return "pong", nil }})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("Authorization"))
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, other.URL, http.StatusFound)
			return
		}
		mcpSrv.ServeHTTP(w, r)
	}))
	defer srv.Close()
	r := NewRunner()
	auth := map[string]string{"Authorization": "Bearer ${STEPWISE_SECRET_SHOP}"}

	spec, err := r.Prepare(ctx, core.AgentSpec{Model: "mock", Tools: []core.ToolSpec{{Type: core.ToolMCP, URL: srv.URL, Headers: auth}}})
	if err != nil {
		t.Fatal(err)
	}
	if res, err := r.Run(ctx, Call{Spec: spec.Tools[0]}); err != nil || res.Text != "pong" {
		t.Fatalf("mcp call = %+v %v", res, err)
	}
	if spec.Tools[0].Headers["Authorization"] != auth["Authorization"] {
		t.Error("the stored spec must keep the ${...} reference, not the secret")
	}
	_, _ = r.Run(ctx, Call{Spec: core.ToolSpec{Type: core.ToolHTTP, Name: "r", URL: srv.URL + "/redirect", Headers: auth}})
	for _, h := range got {
		if h != "Bearer s3cret" {
			t.Fatalf("tool saw Authorization %q; all: %q", h, got)
		}
	}
	if leaked != "" {
		t.Errorf("secret leaked to redirect target: %q", leaked)
	}

	for _, v := range []string{"${STEPWISE_NOT_SECRET}", "${STEPWISE_SECRET_MISSING}"} {
		_, err := r.Prepare(ctx, core.AgentSpec{Model: "mock", Tools: []core.ToolSpec{{Type: core.ToolHTTP, Name: "h", URL: srv.URL, Headers: map[string]string{"X": v}}}})
		if !errors.Is(err, core.ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", v, err)
		}
	}
}

func TestAllowedToolHosts(t *testing.T) {
	hits := 0
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path == "/redirect" { // same server under a host name that isn't allowed
			http.Redirect(w, r, strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)+"/ok", http.StatusFound)
		}
	}))
	defer srv.Close()
	r := NewRunner()
	r.AllowedHosts = []string{"127.0.0.1", "*.shop.com"}

	blocked := core.ToolSpec{Type: core.ToolHTTP, Name: "x", URL: "http://169.254.169.254/latest"}
	if _, err := r.Prepare(ctx, core.AgentSpec{Model: "m", Tools: []core.ToolSpec{blocked}}); !errors.Is(err, core.ErrInvalid) {
		t.Errorf("prepare with a blocked host: %v", err)
	}
	if res, err := r.Run(ctx, Call{Spec: blocked}); err != nil || !res.IsError {
		t.Errorf("run with a blocked host = %+v %v", res, err)
	}
	if _, err := r.Run(ctx, Call{Spec: core.ToolSpec{Type: core.ToolHTTP, Name: "r", URL: srv.URL + "/redirect"}}); err == nil || hits != 1 {
		t.Errorf("redirect to a blocked host: err=%v hits=%d", err, hits)
	}
	for host, want := range map[string]bool{"api.shop.com": true, "shop.com": false, "evilshop.com": false, "127.0.0.1": true} {
		if r.hostAllowed(host) != want {
			t.Errorf("hostAllowed(%q) = %v", host, !want)
		}
	}
}
