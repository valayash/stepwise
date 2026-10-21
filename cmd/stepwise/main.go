// Command stepwise runs the durable agent runtime.
//
//	stepwise dev         everything in one process, in memory, with the demo shop (no setup)
//	stepwise serve       API + workers + webhook dispatcher against Postgres
//	stepwise worker      workers only (scale these horizontally)
//	stepwise migrate     apply database migrations and exit
//	stepwise demo-shop   run the demo shop MCP server on its own
//	stepwise version
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/valayash/stepwise/internal/api"
	"github.com/valayash/stepwise/internal/breaker"
	"github.com/valayash/stepwise/internal/config"
	"github.com/valayash/stepwise/internal/demo"
	"github.com/valayash/stepwise/internal/engine"
	"github.com/valayash/stepwise/internal/gateway"
	"github.com/valayash/stepwise/internal/metrics"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/ratelimit"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/store/memory"
	"github.com/valayash/stepwise/internal/store/postgres"
	"github.com/valayash/stepwise/internal/tools"
	"github.com/valayash/stepwise/internal/webhook"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "dev":
		err = run(ctx, log, cfg, roles{api: true, worker: true, dispatcher: true, shop: true, dev: true})
	case "serve":
		err = run(ctx, log, cfg, roles{api: true, worker: true, dispatcher: true})
	case "api":
		err = run(ctx, log, cfg, roles{api: true, dispatcher: true})
	case "worker":
		err = run(ctx, log, cfg, roles{worker: true})
	case "migrate":
		err = migrate(ctx, cfg)
	case "demo-shop":
		err = serveShop(ctx, log, getenv("STEPWISE_SHOP_ADDR", ":8090"))
	case "version":
		fmt.Println("stepwise", version)
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Error("stepwise exited", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: stepwise <command>

  dev         run everything in one process, in memory, with the demo shop
  serve       API + workers + webhooks (needs STEPWISE_DATABASE_URL)
  api         API + webhooks only
  worker      workers only
  migrate     apply database migrations
  demo-shop   run the demo shop MCP server (STEPWISE_SHOP_ADDR, default :8090)
  version     print the version`)
}

type roles struct{ api, worker, dispatcher, shop, dev bool }

func run(ctx context.Context, log *slog.Logger, cfg config.Config, r roles) error {
	st, closeStore, err := openStore(ctx, cfg, r.dev)
	if err != nil {
		return err
	}
	defer closeStore()

	reg := metrics.NewRegistry()
	router, mock := buildRouter(cfg)
	runner := tools.NewRunner()
	breakers := breaker.NewSet(5, 30*time.Second, nil)

	var limiter ratelimit.Limiter = ratelimit.Unlimited{}
	if len(cfg.RateLimits) > 0 {
		limiter = ratelimit.NewLocal(cfg.RateLimits, nil)
		if cfg.DatabaseURL != "" {
			log.Warn("rate limits are per process; with several worker processes, divide limits by the process count")
		}
	}

	eng := engine.New(engine.Config{
		Workers: cfg.Workers, Lease: cfg.Lease, MaxAttempts: cfg.MaxAttempts,
		Triage: engine.TriageConfig{Enabled: cfg.TriageEnabled, Model: cfg.TriageModel, AutoRetry: cfg.TriageAutoRetry},
	}, engine.Deps{
		Store: st, Router: router, Tools: runner, Limiter: limiter, Breakers: breakers,
		Logger: log, Metrics: engine.NewMetrics(reg, breakers),
	})

	var wg sync.WaitGroup
	start := func(name string, fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); fn(); log.Info(name + " stopped") }()
	}

	if r.worker {
		start("engine", func() { eng.Run(ctx) })
	}
	if r.dispatcher {
		d := &webhook.Dispatcher{Store: st, Secret: []byte(cfg.WebhookSecret), Log: log}
		d.RegisterMetrics(reg)
		start("webhooks", func() { d.Run(ctx) })
	}

	if r.api || r.shop {
		mux := http.NewServeMux()
		if r.shop {
			shop := demo.NewShop(time.Now())
			mux.Handle("/demo/", http.StripPrefix("/demo", shop.Handler()))
		}
		if r.api {
			gw := gateway.New(gateway.Options{
				Router: router, Logger: log, RequestTimeout: cfg.RequestTimeout,
				StreamTimeout: cfg.StreamTimeout, MaxBodyBytes: cfg.MaxBodyBytes,
			})
			mux.Handle("/", (&api.Server{
				Store: st, Tools: runner, Router: router, Token: cfg.APIToken,
				Metrics: reg.Handler(), Gateway: gw.Handler(), Log: log, MaxBodyBytes: cfg.MaxBodyBytes,
			}).Handler())
		}
		srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		start("http", func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http server failed", "err", err)
			}
		})
		go func() {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
		addr := cfg.Addr
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		log.Info("stepwise listening", "ui", "http://"+addr, "mode", modeName(r), "store", storeName(cfg))
		if r.dev {
			fmt.Fprintf(os.Stderr, "\n  Stepwise dev server: http://%s\n  Demo shop MCP server: http://%s/demo/mcp\n  Models: %s (mock needs no API key)\n\n",
				addr, addr, strings.Join(router.Models(), ", "))
		}
	}
	_ = mock

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	return nil
}

func modeName(r roles) string {
	switch {
	case r.dev:
		return "dev"
	case r.api && r.worker:
		return "serve"
	case r.api:
		return "api"
	default:
		return "worker"
	}
}

func storeName(cfg config.Config) string {
	if cfg.DatabaseURL == "" {
		return "memory"
	}
	return "postgres"
}

func buildRouter(cfg config.Config) (*provider.Router, *provider.Mock) {
	mock := provider.NewMock()
	mock.Delay, mock.Latency, mock.FailRate = cfg.MockDelay, cfg.MockLatency, cfg.MockFailRate
	var routes []provider.Route
	if cfg.AnthropicAPIKey != "" {
		routes = append(routes, provider.Route{Prefix: "claude-", Provider: provider.NewAnthropic(cfg.AnthropicAPIKey)})
	}
	if cfg.OpenAIAPIKey != "" {
		oa := provider.NewOpenAI(cfg.OpenAIAPIKey)
		routes = append(routes, provider.Route{Prefix: "gpt-", Provider: oa}, provider.Route{Prefix: "o", Provider: oa})
	}
	routes = append(routes, provider.Route{Prefix: "mock", Provider: mock})
	return provider.NewRouter(routes...), mock
}

// openStore returns the Postgres store when a database URL is set, else memory.
func openStore(ctx context.Context, cfg config.Config, dev bool) (store.Store, func(), error) {
	if cfg.DatabaseURL == "" {
		if !dev {
			return nil, nil, errors.New("STEPWISE_DATABASE_URL is required (use `stepwise dev` to try Stepwise in memory)")
		}
		s := memory.New(nil)
		return s, func() { _ = s.Close() }, nil
	}
	db, listener, err := openPostgres(cfg.DatabaseURL)
	if err != nil {
		return nil, nil, err
	}
	if err := postgres.Migrate(ctx, db); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("migrate: %w", err)
	}
	s, err := postgres.New(db, listener)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return s, func() { _ = s.Close(); _ = db.Close() }, nil
}

func migrate(ctx context.Context, cfg config.Config) error {
	if cfg.DatabaseURL == "" {
		return errors.New("STEPWISE_DATABASE_URL is required")
	}
	db, _, err := openPostgres(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := postgres.Migrate(ctx, db); err != nil {
		return err
	}
	fmt.Println("migrations applied")
	return nil
}

// openPostgres is provided by the driver file compiled into the binary
// (see driver_*.go); it returns a pool and a LISTEN/NOTIFY listener.
var openPostgres = func(url string) (*sql.DB, postgres.Listener, error) {
	return nil, nil, errors.New("this binary was built without a Postgres driver")
}

func serveShop(ctx context.Context, log *slog.Logger, addr string) error {
	srv := &http.Server{Addr: addr, Handler: demo.NewShop(time.Now()).Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Info("demo shop listening", "mcp", "http://localhost"+addr+"/mcp")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level))
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
