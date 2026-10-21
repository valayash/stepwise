// Package mcp is a small Model Context Protocol implementation over the
// Streamable HTTP transport: a client (Stepwise calling tools on MCP servers)
// and a server (used by the demo shop). It covers what agents need —
// initialize, tools/list, tools/call — and nothing more.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// ProtocolVersion is the MCP revision this package speaks.
const ProtocolVersion = "2025-06-18"

// IdempotencyMetaKey is the _meta key Stepwise uses to pass a stable
// idempotency key with every tool call.
const IdempotencyMetaKey = "stepwise/idempotency_key"

// Tool is a tool advertised by a server.
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// CallResult is the outcome of tools/call.
type CallResult struct {
	Text    string // all text content joined
	IsError bool   // the tool reported a failure (shown to the model)
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      *int64 `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("mcp error %d: %s", e.Code, e.Message) }

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// HTTPError is a non-2xx transport answer. 5xx and 429 are worth retrying.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("mcp server returned %d: %s", e.Status, e.Body)
}

// Retryable reports whether err is a transient transport failure.
func Retryable(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == 429 || he.Status >= 500
	}
	var re *rpcError
	if errors.As(err, &re) {
		return false // the server understood and refused
	}
	return err != nil && !errors.Is(err, context.Canceled)
}

// Client talks to MCP servers. One Client serves many server URLs and keeps
// one session per URL. Safe for concurrent use.
type Client struct {
	http *http.Client
	ids  atomic.Int64

	mu       sync.Mutex
	sessions map[string]*session
}

type session struct {
	once sync.Once
	id   string
	err  error
}

// NewClient returns a client. hc may be nil.
func NewClient(hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{}
	}
	return &Client{http: hc, sessions: map[string]*session{}}
}

// ListTools returns every tool the server offers (following pagination).
func (c *Client) ListTools(ctx context.Context, url string) ([]Tool, error) {
	var all []Tool
	cursor := ""
	for page := 0; page < 50; page++ {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.call(ctx, url, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var res struct {
			Tools      []Tool `json:"tools"`
			NextCursor string `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, fmt.Errorf("decode tools/list: %w", err)
		}
		all = append(all, res.Tools...)
		if res.NextCursor == "" {
			return all, nil
		}
		cursor = res.NextCursor
	}
	return all, nil
}

// CallTool runs a tool. idemKey travels in _meta so the server can dedupe.
func (c *Client) CallTool(ctx context.Context, url, name string, args json.RawMessage, idemKey string) (CallResult, error) {
	if len(bytes.TrimSpace(args)) == 0 {
		args = json.RawMessage(`{}`)
	}
	params := map[string]any{"name": name, "arguments": args}
	if idemKey != "" {
		params["_meta"] = map[string]string{IdempotencyMetaKey: idemKey}
	}
	raw, err := c.call(ctx, url, "tools/call", params)
	if err != nil {
		return CallResult{}, err
	}
	var res struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StructuredContent json.RawMessage `json:"structuredContent"`
		IsError           bool            `json:"isError"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return CallResult{}, fmt.Errorf("decode tools/call: %w", err)
	}
	var parts []string
	for _, c := range res.Content {
		if c.Type == "text" {
			parts = append(parts, c.Text)
		}
	}
	text := strings.Join(parts, "\n")
	if text == "" && len(res.StructuredContent) > 0 {
		text = string(res.StructuredContent)
	}
	return CallResult{Text: text, IsError: res.IsError}, nil
}

// call sends a request on an initialized session, re-initializing once if
// the server says the session is gone (404).
func (c *Client) call(ctx context.Context, url, method string, params any) (json.RawMessage, error) {
	for attempt := 0; attempt < 2; attempt++ {
		sid, err := c.session(ctx, url)
		if err != nil {
			return nil, err
		}
		res, _, err := c.post(ctx, url, sid, method, params, true)
		var he *HTTPError
		if errors.As(err, &he) && he.Status == http.StatusNotFound && sid != "" {
			c.dropSession(url)
			continue
		}
		return res, err
	}
	return nil, errors.New("mcp: session could not be re-established")
}

func (c *Client) session(ctx context.Context, url string) (string, error) {
	c.mu.Lock()
	s, ok := c.sessions[url]
	if !ok {
		s = &session{}
		c.sessions[url] = s
	}
	c.mu.Unlock()

	s.once.Do(func() {
		params := map[string]any{
			"protocolVersion": ProtocolVersion,
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]string{"name": "stepwise", "version": "0.1.0"},
		}
		_, hdr, err := c.post(ctx, url, "", "initialize", params, true)
		if err != nil {
			s.err = fmt.Errorf("mcp initialize %s: %w", url, err)
			return
		}
		s.id = hdr.Get("Mcp-Session-Id")
		_, _, s.err = c.post(ctx, url, s.id, "notifications/initialized", nil, false)
	})
	if s.err != nil {
		c.dropSession(url) // let the next call try again
	}
	return s.id, s.err
}

func (c *Client) dropSession(url string) {
	c.mu.Lock()
	delete(c.sessions, url)
	c.mu.Unlock()
}

// post sends one JSON-RPC message. Requests (wantResult) carry an id and
// return the result; notifications return nothing.
func (c *Client) post(ctx context.Context, url, sid, method string, params any, wantResult bool) (json.RawMessage, http.Header, error) {
	req := rpcRequest{JSONRPC: "2.0", Method: method, Params: params}
	var id int64
	if wantResult {
		id = c.ids.Add(1)
		req.ID = &id
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, nil, err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("Accept", "application/json, text/event-stream")
	if method != "initialize" {
		hr.Header.Set("MCP-Protocol-Version", ProtocolVersion)
	}
	if sid != "" {
		hr.Header.Set("Mcp-Session-Id", sid)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(hr.Header)) // traceparent
	resp, err := c.http.Do(hr)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, resp.Header, &HTTPError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if !wantResult {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil, resp.Header, nil
	}

	var msg rpcResponse
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		msg, err = readSSEResponse(resp.Body, id)
	} else {
		err = json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&msg)
	}
	if err != nil {
		return nil, resp.Header, fmt.Errorf("mcp %s: %w", method, err)
	}
	if msg.Error != nil {
		return nil, resp.Header, msg.Error
	}
	return msg.Result, resp.Header, nil
}

// readSSEResponse reads server-sent events until the response with our id
// arrives (servers may send progress notifications first).
func readSSEResponse(r io.Reader, id int64) (rpcResponse, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	var data strings.Builder
	flush := func() (rpcResponse, bool) {
		defer data.Reset()
		if data.Len() == 0 {
			return rpcResponse{}, false
		}
		var msg rpcResponse
		if json.Unmarshal([]byte(data.String()), &msg) == nil && msg.ID != nil && *msg.ID == id {
			return msg, true
		}
		return rpcResponse{}, false
	}
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if msg, ok := flush(); ok {
				return msg, nil
			}
			continue
		}
		if d, ok := strings.CutPrefix(line, "data:"); ok {
			data.WriteString(strings.TrimPrefix(d, " "))
		}
	}
	if msg, ok := flush(); ok {
		return msg, nil
	}
	if err := sc.Err(); err != nil {
		return rpcResponse{}, err
	}
	return rpcResponse{}, errors.New("stream ended without a response")
}

// ---- server ----

// Meta carries per-call context from the client.
type Meta struct {
	IdempotencyKey string
}

// Handler runs a tool. A returned error is reported to the model as a tool
// error (isError), not as a protocol failure.
type Handler func(ctx context.Context, args json.RawMessage, meta Meta) (string, error)

// ServerTool is a tool plus its handler.
type ServerTool struct {
	Tool
	Handler Handler
}

// Server serves tools over Streamable HTTP (POST, JSON responses).
type Server struct {
	Name    string
	Version string
	tools   []ServerTool
	byName  map[string]ServerTool
}

// NewServer builds a server with tools.
func NewServer(name, version string, tools ...ServerTool) *Server {
	s := &Server{Name: name, Version: version, byName: map[string]ServerTool{}}
	for _, t := range tools {
		if len(t.InputSchema) == 0 {
			t.InputSchema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		s.tools = append(s.tools, t)
		s.byName[t.Name] = t
	}
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "this MCP server only accepts POST", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID     *json.RawMessage `json:"id"`
		Method string           `json:"method"`
		Params json.RawMessage  `json:"params"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		writeRPC(w, nil, nil, &rpcError{Code: -32700, Message: "parse error"})
		return
	}
	if req.ID == nil { // notification
		w.WriteHeader(http.StatusAccepted)
		return
	}

	switch req.Method {
	case "initialize":
		w.Header().Set("Mcp-Session-Id", strings.ReplaceAll(fmt.Sprintf("%p-%d", s, sessionSeq.Add(1)), "0x", "s"))
		writeRPC(w, req.ID, map[string]any{
			"protocolVersion": ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]string{"name": s.Name, "version": s.Version},
		}, nil)
	case "ping":
		writeRPC(w, req.ID, map[string]any{}, nil)
	case "tools/list":
		tools := make([]Tool, len(s.tools))
		for i, t := range s.tools {
			tools[i] = t.Tool
		}
		writeRPC(w, req.ID, map[string]any{"tools": tools}, nil)
	case "tools/call":
		var p struct {
			Name      string            `json:"name"`
			Arguments json.RawMessage   `json:"arguments"`
			Meta      map[string]string `json:"_meta"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			writeRPC(w, req.ID, nil, &rpcError{Code: -32602, Message: "invalid params"})
			return
		}
		t, ok := s.byName[p.Name]
		if !ok {
			writeRPC(w, req.ID, nil, &rpcError{Code: -32602, Message: "unknown tool: " + p.Name})
			return
		}
		text, err := t.Handler(r.Context(), p.Arguments, Meta{IdempotencyKey: p.Meta[IdempotencyMetaKey]})
		isErr := err != nil
		if isErr {
			text = err.Error()
		}
		writeRPC(w, req.ID, map[string]any{
			"content": []map[string]string{{"type": "text", "text": text}},
			"isError": isErr,
		}, nil)
	default:
		writeRPC(w, req.ID, nil, &rpcError{Code: -32601, Message: "method not found: " + req.Method})
	}
}

var sessionSeq atomic.Int64

func writeRPC(w http.ResponseWriter, id *json.RawMessage, result any, e *rpcError) {
	w.Header().Set("Content-Type", "application/json")
	msg := map[string]any{"jsonrpc": "2.0", "id": id}
	if e != nil {
		msg["error"] = e
	} else {
		msg["result"] = result
	}
	_ = json.NewEncoder(w).Encode(msg)
}
