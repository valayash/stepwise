// Package gateway exposes an OpenAI-compatible passthrough endpoint, so plain
// (non-durable) LLM calls can share Stepwise providers and credentials.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/valayash/stepwise/internal/provider"
)

// Server wires HTTP handlers to the provider router.
type Server struct {
	router         *provider.Router
	log            *slog.Logger
	requestTimeout time.Duration
	streamTimeout  time.Duration
	maxBodyBytes   int64
	now            func() time.Time
}

// Options configures a Server.
type Options struct {
	Router         *provider.Router
	Logger         *slog.Logger
	RequestTimeout time.Duration // whole non-streaming request
	StreamTimeout  time.Duration // whole streaming request; longer, since answers arrive gradually
	MaxBodyBytes   int64
}

// New builds a Server.
func New(o Options) *Server {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 60 * time.Second
	}
	if o.StreamTimeout <= 0 {
		o.StreamTimeout = 5 * time.Minute
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 1 << 20
	}
	return &Server{
		router:         o.Router,
		log:            o.Logger,
		requestTimeout: o.RequestTimeout,
		streamTimeout:  o.StreamTimeout,
		maxBodyBytes:   o.MaxBodyBytes,
		now:            time.Now,
	}
}

// Handler returns the root HTTP handler with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /v1/chat/completions", s.handleChatCompletions)
	return s.withRequestID(s.withLogging(mux))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)

	var req ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request_too_large", "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_json", "request body is not valid JSON")
		return
	}
	if msg := validate(req); msg != "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", msg)
		return
	}
	p, err := s.router.For(req.Model)
	if err != nil {
		writeError(w, http.StatusNotFound, "invalid_request_error", "model_not_found", err.Error())
		return
	}

	if req.Stream {
		s.handleStream(w, r, p, req)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	start := s.now()
	res, err := p.Complete(ctx, toProviderRequest(req))
	if err != nil {
		s.writeUpstreamError(w, r, p.Name(), err)
		return
	}

	s.log.InfoContext(r.Context(), "completion",
		"request_id", requestIDFrom(r.Context()),
		"provider", p.Name(),
		"model", req.Model,
		"input_tokens", res.InputTokens,
		"output_tokens", res.OutputTokens,
		"upstream_ms", s.now().Sub(start).Milliseconds(),
	)

	writeJSON(w, http.StatusOK, ChatCompletionResponse{
		ID:      "chatcmpl-" + res.ID,
		Object:  "chat.completion",
		Created: s.now().Unix(),
		Model:   req.Model,
		Choices: []Choice{{
			Index:        0,
			Message:      ChatMessage{Role: "assistant", Content: res.Content},
			FinishReason: res.FinishReason,
		}},
		Usage: Usage{
			PromptTokens:     res.InputTokens,
			CompletionTokens: res.OutputTokens,
			TotalTokens:      res.InputTokens + res.OutputTokens,
		},
	})
}

func (s *Server) writeUpstreamError(w http.ResponseWriter, r *http.Request, name string, err error) {
	s.log.WarnContext(r.Context(), "upstream error",
		"request_id", requestIDFrom(r.Context()), "provider", name, "err", err)

	var ue *provider.UpstreamError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "upstream_error", "upstream_timeout", "provider timed out")
	case errors.Is(err, context.Canceled):
		// Client went away; nothing useful to send.
	case errors.As(err, &ue) && ue.Status == http.StatusTooManyRequests:
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "upstream_rate_limited", "provider rate limit hit")
	case errors.As(err, &ue) && ue.Status >= 400 && ue.Status < 500:
		writeError(w, http.StatusBadRequest, "invalid_request_error", "upstream_rejected", "provider rejected the request")
	default:
		writeError(w, http.StatusBadGateway, "upstream_error", "upstream_failed", "provider request failed")
	}
}

func validate(req ChatCompletionRequest) string {
	if req.Model == "" {
		return "model is required"
	}
	if len(req.Messages) == 0 {
		return "messages must not be empty"
	}
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "user", "assistant":
		default:
			return "unsupported message role: " + m.Role
		}
	}
	if req.MaxTokens != nil && *req.MaxTokens <= 0 {
		return "max_tokens must be positive"
	}
	return ""
}

func toProviderRequest(req ChatCompletionRequest) provider.Request {
	out := provider.Request{Model: req.Model, Temperature: req.Temperature}
	if req.MaxTokens != nil {
		out.MaxTokens = *req.MaxTokens
	}
	out.Messages = make([]provider.Message, len(req.Messages))
	for i, m := range req.Messages {
		out.Messages[i] = provider.Message{Role: m.Role, Content: m.Content}
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, typ, code, msg string) {
	writeJSON(w, status, ErrorResponse{Error: ErrorBody{Message: msg, Type: typ, Code: code}})
}

// ---- middleware ----

type ctxKey int

const requestIDKey ctxKey = 0

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			id = newID()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the real writer to flush streams.
func (sr *statusRecorder) Unwrap() http.ResponseWriter { return sr.ResponseWriter }

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.InfoContext(r.Context(), "http",
			"request_id", requestIDFrom(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
