// Command tollgate runs the multi-tenant LLM gateway.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/valayash/tollgate/internal/config"
	"github.com/valayash/tollgate/internal/provider"
	"github.com/valayash/tollgate/internal/server"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("tollgate exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	mock := provider.NewMock()
	mock.Delay = cfg.MockDelay
	routes := []provider.Route{{Prefix: "mock", Provider: mock}}
	if cfg.AnthropicAPIKey != "" {
		routes = append(routes, provider.Route{Prefix: "claude-", Provider: provider.NewAnthropic(cfg.AnthropicAPIKey)})
	} else {
		log.Warn("ANTHROPIC_API_KEY not set; claude-* models are disabled")
	}

	srv := server.New(server.Options{
		Router:         provider.NewRouter(routes...),
		Logger:         log,
		RequestTimeout: cfg.RequestTimeout,
		StreamTimeout:  cfg.StreamTimeout,
		MaxBodyBytes:   cfg.MaxBodyBytes,
	})

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("tollgate listening", "addr", cfg.Addr)
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return httpSrv.Shutdown(shutdownCtx)
}
