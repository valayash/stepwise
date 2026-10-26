// Package tools executes agent tool calls (HTTP, MCP, built-in) and prepares
// agent specs before a run starts.
package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/mcp"
	"github.com/valayash/stepwise/internal/telemetry"
)

// MaxResultBytes caps what a tool may hand back to the model.
const MaxResultBytes = 16 << 10

// Call is one tool invocation.
type Call struct {
	RunID          string
	Spec           core.ToolSpec
	Input          json.RawMessage
	IdempotencyKey string
}

// Result is what the model sees. IsError results are normal outcomes (the
// tool refused or failed in a way the model should know about).
type Result struct {
	Text    string
	IsError bool
}

// TransientError marks a failure worth retrying later (timeouts, 5xx, 429).
type TransientError struct{ Err error }

func (e *TransientError) Error() string { return "transient tool failure: " + e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

// IsTransient reports whether err should be retried.
func IsTransient(err error) bool {
	var te *TransientError
	return errors.As(err, &te)
}

// Builtin is an in-process tool.
type Builtin func(ctx context.Context, c Call) (Result, error)

// Runner executes tool calls.
type Runner struct {
	HTTP     *http.Client
	MCP      *mcp.Client
	Builtins map[string]Builtin
	// AllowedHosts, if set, are the only hosts tools may call ("api.shop.com",
	// "*.shop.com"), so an agent spec can't reach internal addresses.
	AllowedHosts []string
}

// NewRunner returns a runner with default clients.
func NewRunner() *Runner {
	r := &Runner{Builtins: map[string]Builtin{}}
	r.HTTP = &http.Client{Transport: headerTransport{http.DefaultTransport, r.hostAllowed}}
	r.MCP = mcp.NewClient(r.HTTP)
	return r
}

// Run executes one call. The returned error is always a *TransientError (or
// context cancellation); everything else becomes an IsError result.
func (r *Runner) Run(ctx context.Context, c Call) (Result, error) {
	timeout := 30 * time.Second
	if c.Spec.TimeoutSecs > 0 {
		timeout = time.Duration(c.Spec.TimeoutSecs) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ctx, err := withHeaders(ctx, c.Spec)
	if err == nil {
		err = r.checkHost(c.Spec)
	}
	if err != nil {
		return Result{Text: err.Error(), IsError: true}, nil
	}

	var res Result
	switch c.Spec.Type {
	case core.ToolHTTP:
		res, err = r.runHTTP(ctx, c)
	case core.ToolMCP:
		res, err = r.runMCP(ctx, c)
	case core.ToolBuiltin:
		fn, ok := r.Builtins[c.Spec.Name]
		if !ok {
			return Result{Text: "unknown built-in tool " + c.Spec.Name, IsError: true}, nil
		}
		res, err = fn(ctx, c)
	default:
		return Result{Text: fmt.Sprintf("tool type %q cannot be executed directly", c.Spec.Type), IsError: true}, nil
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return Result{}, err
		}
		if !IsTransient(err) {
			err = &TransientError{Err: err}
		}
		return Result{}, err
	}
	if len(res.Text) > MaxResultBytes {
		res.Text = res.Text[:MaxResultBytes] + "\n[truncated]"
	}
	return res, nil
}

func (r *Runner) runHTTP(ctx context.Context, c Call) (Result, error) {
	body := c.Input
	if len(bytes.TrimSpace(body)) == 0 {
		body = json.RawMessage(`{}`)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Spec.URL, bytes.NewReader(body))
	if err != nil {
		return Result{Text: "bad tool URL: " + err.Error(), IsError: true}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", c.IdempotencyKey)
	req.Header.Set("X-Stepwise-Run-Id", c.RunID)
	req.Header.Set("X-Stepwise-Tool", c.Spec.Name)
	telemetry.Inject(ctx, req.Header) // traceparent: the tool can join the run's trace

	resp, err := r.HTTP.Do(req)
	if err != nil {
		return Result{}, &TransientError{Err: err}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxResultBytes+1))
	if err != nil {
		return Result{}, &TransientError{Err: err}
	}
	text := strings.TrimSpace(string(raw))
	switch {
	case resp.StatusCode/100 == 2:
		return Result{Text: text}, nil
	case resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500:
		return Result{}, &TransientError{Err: fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(text, 200))}
	default:
		return Result{Text: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, text), IsError: true}, nil
	}
}

func (r *Runner) runMCP(ctx context.Context, c Call) (Result, error) {
	res, err := r.MCP.CallTool(ctx, c.Spec.URL, c.Spec.Name, c.Input, c.IdempotencyKey)
	if err != nil {
		if mcp.Retryable(err) {
			return Result{}, &TransientError{Err: err}
		}
		return Result{Text: err.Error(), IsError: true}, nil
	}
	return Result{Text: res.Text, IsError: res.IsError}, nil
}

var toolName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// Prepare validates a spec and expands MCP server references into concrete
// tools, recursively for sub-agents. The returned spec is what the run
// stores, so a run's tools never change after it starts.
func (r *Runner) Prepare(ctx context.Context, spec core.AgentSpec) (core.AgentSpec, error) {
	return r.prepare(ctx, spec, 0)
}

func (r *Runner) prepare(ctx context.Context, spec core.AgentSpec, depth int) (core.AgentSpec, error) {
	if depth > 3 {
		return spec, fmt.Errorf("%w: sub-agents nest deeper than 3 levels", core.ErrInvalid)
	}
	if strings.TrimSpace(spec.Model) == "" {
		return spec, fmt.Errorf("%w: agent.model is required", core.ErrInvalid)
	}
	var out []core.ToolSpec
	for i, t := range spec.Tools {
		tctx, err := withHeaders(ctx, t)
		if err == nil {
			err = r.checkHost(t)
		}
		if err != nil {
			return spec, fmt.Errorf("tools[%d]: %w", i, err)
		}
		switch t.Type {
		case core.ToolMCP:
			if t.URL == "" {
				return spec, fmt.Errorf("%w: tools[%d]: mcp tools need a url", core.ErrInvalid, i)
			}
			if t.Name != "" && len(t.InputSchema) > 0 {
				out = append(out, t) // already concrete
				continue
			}
			listed, err := r.MCP.ListTools(tctx, t.URL)
			if err != nil {
				return spec, fmt.Errorf("%w: tools[%d]: listing tools from %s: %v", core.ErrInvalid, i, t.URL, err)
			}
			keep := map[string]bool{}
			for _, n := range t.Include {
				keep[n] = true
			}
			if t.Name != "" {
				keep[t.Name] = true
			}
			for _, lt := range listed {
				if len(keep) > 0 && !keep[lt.Name] {
					continue
				}
				nt := t
				nt.Name, nt.Description, nt.InputSchema, nt.Include = lt.Name, lt.Description, lt.InputSchema, nil
				out = append(out, nt)
			}
		case core.ToolHTTP:
			if t.URL == "" {
				return spec, fmt.Errorf("%w: tools[%d] %q: http tools need a url", core.ErrInvalid, i, t.Name)
			}
			out = append(out, t)
		case core.ToolAgent:
			if t.Agent == nil {
				return spec, fmt.Errorf("%w: tools[%d] %q: agent tools need an agent spec", core.ErrInvalid, i, t.Name)
			}
			child, err := r.prepare(ctx, *t.Agent, depth+1)
			if err != nil {
				return spec, err
			}
			t.Agent = &child
			if t.Description == "" {
				t.Description = "Delegate a task to the " + t.Name + " sub-agent and get its answer."
			}
			out = append(out, t)
		case core.ToolBuiltin:
			if _, ok := r.Builtins[t.Name]; !ok {
				return spec, fmt.Errorf("%w: tools[%d]: unknown built-in %q", core.ErrInvalid, i, t.Name)
			}
			out = append(out, t)
		default:
			return spec, fmt.Errorf("%w: tools[%d]: unknown type %q (use http, mcp, agent)", core.ErrInvalid, i, t.Type)
		}
	}

	seen := map[string]bool{}
	for _, t := range out {
		if !toolName.MatchString(t.Name) {
			return spec, fmt.Errorf("%w: tool name %q must be 1-64 letters, digits, _ or -", core.ErrInvalid, t.Name)
		}
		if seen[t.Name] {
			return spec, fmt.Errorf("%w: duplicate tool name %q", core.ErrInvalid, t.Name)
		}
		seen[t.Name] = true
	}
	for _, n := range spec.RequireApproval {
		if n != "*" && !seen[n] {
			return spec, fmt.Errorf("%w: require_approval names unknown tool %q", core.ErrInvalid, n)
		}
	}
	for n, max := range spec.Limits {
		if !seen[n] || max < 1 {
			return spec, fmt.Errorf("%w: limits[%q] must name a tool and be at least 1", core.ErrInvalid, n)
		}
	}
	spec.Tools = out
	return spec.WithDefaults(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// SecretPrefix limits which environment variables tool headers may read, so
// an agent spec can never send the server's own keys to a tool.
const SecretPrefix = "STEPWISE_SECRET_"

type toolHeaders struct {
	host string
	h    map[string]string
}

type headersKey struct{}

// withHeaders resolves a tool's headers and attaches them to ctx, where
// headerTransport picks them up for both HTTP and MCP requests.
func withHeaders(ctx context.Context, t core.ToolSpec) (context.Context, error) {
	if len(t.Headers) == 0 {
		return ctx, nil
	}
	u, err := url.Parse(t.URL)
	if err != nil {
		return ctx, fmt.Errorf("%w: bad url: %v", core.ErrInvalid, err)
	}
	h := make(map[string]string, len(t.Headers))
	for k, v := range t.Headers {
		var bad string
		h[k] = os.Expand(v, func(name string) string {
			val, ok := os.LookupEnv(name)
			if !ok || !strings.HasPrefix(name, SecretPrefix) {
				bad = name
				return ""
			}
			return val
		})
		if bad != "" {
			return ctx, fmt.Errorf("%w: header %s references ${%s}; only set %s* variables are allowed", core.ErrInvalid, k, bad, SecretPrefix)
		}
	}
	return context.WithValue(ctx, headersKey{}, toolHeaders{host: u.Host, h: h}), nil
}

func (r *Runner) hostAllowed(host string) bool {
	if len(r.AllowedHosts) == 0 {
		return true
	}
	host = strings.ToLower(host)
	for _, a := range r.AllowedHosts {
		if a == host || strings.HasPrefix(a, "*.") && strings.HasSuffix(host, a[1:]) {
			return true
		}
	}
	return false
}

// checkHost rejects HTTP and MCP tools whose URL host is not allowed.
func (r *Runner) checkHost(t core.ToolSpec) error {
	if t.Type != core.ToolHTTP && t.Type != core.ToolMCP {
		return nil
	}
	if u, err := url.Parse(t.URL); err != nil || !r.hostAllowed(u.Hostname()) {
		return fmt.Errorf("%w: tool url %q is not in the allowed tool hosts", core.ErrInvalid, t.URL)
	}
	return nil
}

// headerTransport refuses hosts that aren't allowed (including redirect
// targets) and adds a tool's headers to requests for the tool's own host
// only, without overriding Stepwise's headers.
type headerTransport struct {
	next  http.RoundTripper
	allow func(host string) bool
}

func (t headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.allow(req.URL.Hostname()) {
		return nil, fmt.Errorf("tool host %s is not allowed", req.URL.Hostname())
	}
	th, ok := req.Context().Value(headersKey{}).(toolHeaders)
	if !ok || req.URL.Host != th.host {
		return t.next.RoundTrip(req)
	}
	req = req.Clone(req.Context())
	for k, v := range th.h {
		if req.Header.Get(k) == "" {
			req.Header.Set(k, v)
		}
	}
	return t.next.RoundTrip(req)
}
