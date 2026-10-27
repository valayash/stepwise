// Package api is Stepwise's HTTP interface: create and inspect runs, decide
// approvals, stream live updates, plus health, metrics and the web UI.
package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/tools"
)

//go:embed ui.html
var uiFS embed.FS

// Server serves the API.
type Server struct {
	Store   store.Store
	Tools   *tools.Runner
	Router  *provider.Router
	Token   string       // if set, /v1 requires "Authorization: Bearer <Token>"
	Metrics http.Handler // optional /metrics
	Gateway http.Handler // optional OpenAI-compatible /v1/chat/completions
	Log     *slog.Logger
	// MaxBodyBytes caps request bodies (default 1 MiB).
	MaxBodyBytes int64
	// Info is served at GET /v1/info.
	Info Info
	// MonthlyTokenLimit, if set, refuses new runs once this month's usage reaches it.
	MonthlyTokenLimit int
}

// Info describes this deployment to clients and the UI.
type Info struct {
	Version     string   `json:"version"`
	Mode        string   `json:"mode"`
	Models      []string `json:"models"`
	DemoShopURL string   `json:"demo_shop_url,omitempty"` // MCP URL reachable by workers
}

// Handler returns the root handler.
func (s *Server) Handler() http.Handler {
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.MaxBodyBytes <= 0 {
		s.MaxBodyBytes = 1 << 20
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.ui)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics)
	}

	v1 := http.NewServeMux()
	v1.HandleFunc("POST /v1/runs", s.createRun)
	v1.HandleFunc("GET /v1/runs", s.serveJSON(s.listRuns))
	v1.HandleFunc("GET /v1/runs/{id}", s.serveJSON(s.getRun))
	v1.HandleFunc("GET /v1/runs/{id}/events", s.events)
	v1.HandleFunc("GET /v1/runs/{id}/deliveries", s.serveJSON(s.deliveries))
	v1.HandleFunc("POST /v1/runs/{id}/cancel", s.serveJSON(s.mutate(s.Store.Cancel)))
	v1.HandleFunc("POST /v1/runs/{id}/retry", s.serveJSON(s.mutate(s.Store.Retry)))
	v1.HandleFunc("POST /v1/runs/{id}/messages", s.serveJSON(s.addMessage))
	v1.HandleFunc("POST /v1/runs/{id}/steps/{step}/approve", s.serveJSON(s.decide(true)))
	v1.HandleFunc("POST /v1/runs/{id}/steps/{step}/reject", s.serveJSON(s.decide(false)))
	v1.HandleFunc("GET /v1/approvals", s.serveJSON(s.approvals))
	v1.HandleFunc("GET /v1/usage", s.serveJSON(s.usage))
	v1.HandleFunc("GET /v1/info", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, s.Info) })
	if s.Gateway != nil {
		v1.Handle("POST /v1/chat/completions", s.Gateway)
	}
	mux.Handle("/v1/", s.auth(v1))
	return s.logging(mux)
}

// ---- runs ----

type createRequest struct {
	core.NewRun
	Agent *core.AgentSpec `json:"agent"`
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if !s.decode(w, r, &req) {
		return
	}
	if req.Agent == nil {
		writeErr(w, fmt.Errorf("%w: agent is required", core.ErrInvalid))
		return
	}
	if strings.TrimSpace(req.Input) == "" {
		writeErr(w, fmt.Errorf("%w: input is required", core.ErrInvalid))
		return
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}
	if len(req.Metadata) > 0 && !json.Valid(req.Metadata) {
		writeErr(w, fmt.Errorf("%w: metadata must be JSON", core.ErrInvalid))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if err := s.checkSpend(ctx); err != nil {
		writeErr(w, err)
		return
	}
	spec, err := s.Tools.Prepare(ctx, *req.Agent)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := s.checkModels(spec); err != nil {
		writeErr(w, err)
		return
	}
	req.Spec = spec
	run, created, err := s.Store.CreateRun(ctx, req.NewRun)
	if err != nil {
		writeErr(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/v1/runs/"+run.ID)
	writeJSON(w, status, run)
}

// checkModels rejects unknown models up front, including sub-agents'.
func (s *Server) checkModels(spec core.AgentSpec) error {
	if s.Router == nil {
		return nil
	}
	if _, err := s.Router.For(spec.Model); err != nil {
		return fmt.Errorf("%w: %v (configured: %s)", core.ErrInvalid, err, strings.Join(s.Router.Models(), ", "))
	}
	for _, t := range spec.Tools {
		if t.Agent != nil {
			if err := s.checkModels(*t.Agent); err != nil {
				return err
			}
		}
	}
	return nil
}

// handlerFunc is a handler whose result is sent as JSON with status 200.
// It returns errWritten when it already wrote the response itself.
type handlerFunc func(w http.ResponseWriter, r *http.Request) (any, error)

var errWritten = errors.New("response already written")

// serveJSON adapts a handlerFunc: the result becomes the body, an error becomes
// the matching status code.
func (s *Server) serveJSON(fn handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v, err := fn(w, r)
		switch {
		case errors.Is(err, errWritten):
		case err != nil:
			writeErr(w, err)
		default:
			writeJSON(w, 200, v)
		}
	}
}

type usageView struct {
	store.Usage
	Since time.Time `json:"since"`
	Limit int       `json:"monthly_token_limit,omitempty"`
}

// monthUsage is this calendar month's spend (UTC).
func (s *Server) monthUsage(ctx context.Context) (usageView, error) {
	now := time.Now().UTC()
	v := usageView{Since: time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC), Limit: s.MonthlyTokenLimit}
	var err error
	v.Usage, err = s.Store.Usage(ctx, v.Since)
	return v, err
}

func (s *Server) usage(_ http.ResponseWriter, r *http.Request) (any, error) {
	return s.monthUsage(r.Context())
}

func (s *Server) checkSpend(ctx context.Context) error {
	if s.MonthlyTokenLimit <= 0 {
		return nil
	}
	u, err := s.monthUsage(ctx)
	if err == nil && u.Tokens >= u.Limit {
		err = fmt.Errorf("%w: monthly token limit of %d reached (%d used)", core.ErrLimit, u.Limit, u.Tokens)
	}
	return err
}

func (s *Server) listRuns(_ http.ResponseWriter, r *http.Request) (any, error) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	runs, err := s.Store.ListRuns(r.Context(), store.ListFilter{
		Status: core.RunStatus(q.Get("status")), ParentRunID: q.Get("parent_run_id"), Limit: limit,
	})
	if runs == nil {
		runs = []core.Run{}
	}
	return map[string]any{"runs": runs}, err
}

type runView struct {
	core.Run
	Steps    []core.Step `json:"steps"`
	Children []core.Run  `json:"children,omitempty"`
}

func (s *Server) view(ctx context.Context, id string) (runView, error) {
	run, err := s.Store.GetRun(ctx, id)
	if err != nil {
		return runView{}, err
	}
	steps, err := s.Store.ListSteps(ctx, id)
	if err != nil {
		return runView{}, err
	}
	kids, err := s.Store.ListRuns(ctx, store.ListFilter{ParentRunID: id, Limit: 100})
	if err != nil {
		return runView{}, err
	}
	if steps == nil {
		steps = []core.Step{}
	}
	return runView{Run: run, Steps: steps, Children: kids}, nil
}

func (s *Server) getRun(_ http.ResponseWriter, r *http.Request) (any, error) {
	return s.view(r.Context(), r.PathValue("id"))
}

// events streams the run as server-sent events: a full snapshot now, then a
// fresh snapshot after every change, until the run ends or the client leaves.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	v, err := s.view(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	ch, stop := s.Store.Subscribe()
	defer stop()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(w)
	var last []byte
	send := func(v runView) bool {
		b, _ := json.Marshal(v)
		if bytes.Equal(b, last) {
			return true // nothing changed since the last snapshot
		}
		last = b
		if _, err := fmt.Fprintf(w, "event: run\ndata: %s\n\n", b); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send(v) || v.Status.Terminal() {
		return
	}

	// Stores notify on run state changes, not on every step, so the view also
	// polls; the run's own events just make state changes show up sooner.
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil || rc.Flush() != nil {
				return
			}
			continue
		case ev := <-ch:
			if ev.RunID != id {
				continue
			}
		case <-poll.C:
		}
		v, err := s.view(r.Context(), id)
		if err != nil || !send(v) || v.Status.Terminal() {
			return
		}
	}
}

func (s *Server) deliveries(_ http.ResponseWriter, r *http.Request) (any, error) {
	id := r.PathValue("id")
	if _, err := s.Store.GetRun(r.Context(), id); err != nil {
		return nil, err
	}
	ds, err := s.Store.ListDeliveries(r.Context(), id)
	if ds == nil {
		ds = []store.Delivery{}
	}
	return map[string]any{"deliveries": ds}, err
}

// mutate applies fn (cancel, retry) to the run and returns the run.
func (s *Server) mutate(fn func(context.Context, string) error) handlerFunc {
	return func(_ http.ResponseWriter, r *http.Request) (any, error) {
		id := r.PathValue("id")
		if err := fn(r.Context(), id); err != nil {
			return nil, err
		}
		return s.Store.GetRun(r.Context(), id)
	}
}

func (s *Server) decide(approve bool) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) (any, error) {
		stepID, err := strconv.ParseInt(r.PathValue("step"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: step id must be a number", core.ErrInvalid)
		}
		var body struct {
			Note      string `json:"note"`
			DecidedBy string `json:"decided_by"`
		}
		if r.ContentLength != 0 && !s.decode(w, r, &body) {
			return nil, errWritten
		}
		id := r.PathValue("id")
		if err := s.Store.DecideApproval(r.Context(), id, stepID, core.Approval{Approved: approve, Note: body.Note, DecidedBy: body.DecidedBy}); err != nil {
			return nil, err
		}
		return s.view(r.Context(), id)
	}
}

// addMessage continues a completed run as a conversation.
func (s *Server) addMessage(w http.ResponseWriter, r *http.Request) (any, error) {
	var body struct {
		Content string `json:"content"`
	}
	if !s.decode(w, r, &body) {
		return nil, errWritten
	}
	if strings.TrimSpace(body.Content) == "" {
		return nil, fmt.Errorf("%w: content is required", core.ErrInvalid)
	}
	if err := s.checkSpend(r.Context()); err != nil {
		return nil, err
	}
	id := r.PathValue("id")
	if _, err := s.Store.AddMessage(r.Context(), id, body.Content); err != nil {
		return nil, err
	}
	return s.view(r.Context(), id)
}

type pendingApproval struct {
	RunID     string          `json:"run_id"`
	StepID    int64           `json:"step_id"`
	ToolName  string          `json:"tool_name"`
	ToolInput json.RawMessage `json:"tool_input"`
	Input     string          `json:"run_input"`
	Waiting   time.Duration   `json:"waiting_ns"`
	CreatedAt time.Time       `json:"requested_at"`
}

// approvals lists every tool call waiting for a human: an inbox.
func (s *Server) approvals(_ http.ResponseWriter, r *http.Request) (any, error) {
	steps, err := s.Store.PendingApprovals(r.Context())
	if err != nil {
		return nil, err
	}
	out := []pendingApproval{}
	runs := map[string]core.Run{}
	for _, st := range steps {
		run, ok := runs[st.RunID]
		if !ok {
			if run, err = s.Store.GetRun(r.Context(), st.RunID); err != nil {
				continue
			}
			runs[st.RunID] = run
		}
		out = append(out, pendingApproval{
			RunID: run.ID, StepID: st.ID, ToolName: st.ToolName, ToolInput: st.ToolInput,
			Input: run.Input, Waiting: time.Since(st.CreatedAt), CreatedAt: st.CreatedAt,
		})
	}
	return map[string]any{"approvals": out}, nil
}

// ---- infrastructure ----

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.Store.ListRuns(ctx, store.ListFilter{Limit: 1}); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "store unavailable", "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

func (s *Server) ui(w http.ResponseWriter, _ *http.Request) {
	b, _ := uiFS.ReadFile("ui.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

func (s *Server) auth(next http.Handler) http.Handler {
	if s.Token == "" {
		return next
	}
	want := []byte("Bearer " + s.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			writeJSON(w, http.StatusUnauthorized, errorBody("unauthorized", "missing or wrong bearer token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int)        { sw.status = code; sw.ResponseWriter.WriteHeader(code) }
func (sw *statusWriter) Unwrap() http.ResponseWriter { return sw.ResponseWriter }

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			return
		}
		s.Log.Info("http", "method", r.Method, "path", r.URL.Path, "status", sw.status, "ms", time.Since(start).Milliseconds())
	})
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorBody("too_large", "request body too large"))
		} else if errors.Is(err, io.EOF) {
			writeJSON(w, 400, errorBody("invalid_request", "request body is empty"))
		} else {
			writeJSON(w, 400, errorBody("invalid_json", err.Error()))
		}
		return false
	}
	return true
}

func errorBody(code, msg string) map[string]any {
	return map[string]any{"error": map[string]string{"code": code, "message": msg}}
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, core.ErrNotFound):
		writeJSON(w, 404, errorBody("not_found", err.Error()))
	case errors.Is(err, core.ErrInvalid):
		writeJSON(w, 400, errorBody("invalid_request", err.Error()))
	case errors.Is(err, core.ErrNotRunnable), errors.Is(err, core.ErrConflict):
		writeJSON(w, 409, errorBody("conflict", err.Error()))
	case errors.Is(err, core.ErrLimit):
		writeJSON(w, http.StatusTooManyRequests, errorBody("limit_reached", err.Error()))
	default:
		writeJSON(w, 500, errorBody("internal", "internal error"))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
