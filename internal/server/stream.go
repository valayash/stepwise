package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/valayash/tollgate/internal/api"
	"github.com/valayash/tollgate/internal/provider"
)

// sseWriter writes server-sent events and flushes after each one so the client
// sees text as soon as it is generated. Headers are sent lazily on the first
// event: until then, an upstream failure can still become a normal JSON error
// with a real status code.
type sseWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	started bool
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	return &sseWriter{w: w, rc: http.NewResponseController(w)}
}

func (s *sseWriter) start() {
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // stop nginx-style proxies from buffering the stream
	s.w.WriteHeader(http.StatusOK)
	s.started = true
}

func (s *sseWriter) send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.w, "data: %s\n\n", b); err != nil {
		return err
	}
	return s.rc.Flush()
}

func (s *sseWriter) done() error {
	if _, err := fmt.Fprint(s.w, "data: [DONE]\n\n"); err != nil {
		return err
	}
	return s.rc.Flush()
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request, p provider.Provider, req api.ChatCompletionRequest) {
	ctx, cancel := context.WithTimeout(r.Context(), s.streamTimeout)
	defer cancel()

	sw := newSSEWriter(w)
	id := "chatcmpl-" + newID()
	created := s.now().Unix()

	chunk := func(delta api.ChunkDelta, finish *string) api.ChatCompletionChunk {
		return api.ChatCompletionChunk{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model,
			Choices: []api.ChunkChoice{{Index: 0, Delta: delta, FinishReason: finish}},
		}
	}
	// The first chunk announces the assistant role, as OpenAI does.
	begin := func() error {
		sw.start()
		return sw.send(chunk(api.ChunkDelta{Role: "assistant"}, nil))
	}

	start := s.now()
	res, err := p.Stream(ctx, toProviderRequest(req), func(text string) error {
		if !sw.started {
			if err := begin(); err != nil {
				return err
			}
		}
		return sw.send(chunk(api.ChunkDelta{Content: text}, nil))
	})

	if err != nil {
		if !sw.started {
			s.writeUpstreamError(w, r, p.Name(), err)
			return
		}
		s.log.WarnContext(r.Context(), "stream interrupted",
			"request_id", requestIDFrom(r.Context()), "provider", p.Name(), "err", err)
		if r.Context().Err() != nil {
			return // client disconnected: nobody to tell
		}
		// Mid-stream failure: send an OpenAI-style error event and end without [DONE],
		// so clients can tell the answer is incomplete.
		_ = sw.send(api.ErrorResponse{Error: api.ErrorBody{
			Message: streamErrorMessage(err), Type: "upstream_error", Code: "stream_interrupted",
		}})
		return
	}

	if !sw.started {
		if err := begin(); err != nil {
			return
		}
	}
	finish := res.FinishReason
	if finish == "" {
		finish = "stop"
	}
	_ = sw.send(chunk(api.ChunkDelta{}, &finish))

	usage := api.Usage{
		PromptTokens:     res.InputTokens,
		CompletionTokens: res.OutputTokens,
		TotalTokens:      res.InputTokens + res.OutputTokens,
	}
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		_ = sw.send(api.ChatCompletionChunk{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model,
			Choices: []api.ChunkChoice{}, Usage: &usage,
		})
	}
	_ = sw.done()

	s.log.InfoContext(r.Context(), "completion",
		"request_id", requestIDFrom(r.Context()),
		"provider", p.Name(),
		"model", req.Model,
		"stream", true,
		"input_tokens", usage.PromptTokens,
		"output_tokens", usage.CompletionTokens,
		"upstream_ms", s.now().Sub(start).Milliseconds(),
	)
}

func streamErrorMessage(err error) string {
	var se *provider.StreamError
	switch {
	case errors.As(err, &se):
		return "provider error: " + se.Type
	case errors.Is(err, context.DeadlineExceeded):
		return "stream timed out"
	case errors.Is(err, provider.ErrIncompleteStream):
		return "provider closed the stream early"
	default:
		return "stream failed"
	}
}
