# Tollgate build plan

Target: MVP on **Sat Oct 31, 2026**, repo public and polished on **Sun Nov 1**.
Weekdays ≈ 1.5 h build, weekends ≈ 6 h. Each day ends with a green CI run and a pushed commit.

## Milestones

### Day 1 — Thu Oct 22 · Skeleton ✅
- [x] Go module, project layout (`cmd/`, `internal/`)
- [x] OpenAI-compatible `POST /v1/chat/completions` (non-streaming)
- [x] Provider interface + model-prefix router
- [x] Mock provider (deterministic, for tests and load testing)
- [x] Anthropic Messages API adapter (system prompt lifting, stop-reason mapping)
- [x] OpenAI-shaped error envelope, upstream error mapping (429 / 4xx / 5xx / timeout)
- [x] Request ID + structured logging middleware (`log/slog`), body size limit, graceful shutdown
- [x] Table-driven tests with `httptest`, CI (vet, race, coverage)

### Day 2 — Fri Oct 23 · Streaming ✅ (done early, Oct 21)
- [x] `stream: true` → SSE passthrough (`text/event-stream`, flush per chunk via `http.ResponseController`)
- [x] Translate Anthropic stream events → OpenAI `chat.completion.chunk` deltas, `data: [DONE]`
- [x] Cancel upstream when the client disconnects (context propagation)
- [x] Token usage from the stream; `stream_options.include_usage` final chunk
- [x] Lazy headers: errors before the first token still return a JSON error with a real status
- [x] Mid-stream errors → OpenAI-style error event, no `[DONE]`
- [x] Tests: fake SSE upstream, error/cut-off/garbage streams, real client-disconnect test

### Day 3 — Sat Oct 24 · Postgres
- [ ] `docker-compose.yml`: Postgres + Redis
- [ ] Migrations with `goose`: `tenants`, `api_keys` (SHA-256 hashed, prefix shown), `usage_ledger`
- [ ] `pgx` pool, repository layer, auth middleware (`Authorization: Bearer tg_...`)
- [ ] Write one ledger row per request in a transaction
- [ ] Integration tests against a real Postgres (testcontainers or compose in CI)

### Day 4 — Sun Oct 25 · Rate limits & budgets
- [ ] Redis token bucket in a Lua script (atomic), per tenant: requests/min
- [ ] Monthly token budget: reserve on request, settle on response
- [ ] `429` with `Retry-After`; `X-RateLimit-*` headers
- [ ] Concurrency test: N goroutines can't exceed the limit

### Day 5 — Mon Oct 26 · Resilience
- [ ] OpenAI provider adapter
- [ ] Retries with exponential backoff + jitter (only 429/5xx/timeouts, never after streaming starts)
- [ ] Circuit breaker per provider (closed → open → half-open)
- [ ] Fallback chain per model alias (e.g. `smart` → Claude, then OpenAI)

### Day 6–7 — Tue Oct 27 / Wed Oct 28 · gRPC admin API
- [ ] `proto/admin/v1/admin.proto` with `buf`
- [ ] CreateTenant, CreateKey, RevokeKey, SetLimits, GetUsage (server-streaming usage tail as stretch)
- [ ] Auth interceptor (admin token), logging + recovery interceptors
- [ ] `grpcurl` examples in README

### Day 8 — Thu Oct 29 · Observability
- [ ] OpenTelemetry tracing: gateway span → provider span, trace ID in logs
- [ ] Prometheus `/metrics`: request count, latency histogram, tokens by tenant/provider, breaker state
- [ ] Jaeger in compose

### Day 9 — Fri Oct 30 · PII redaction + dashboard
- [ ] Redaction middleware: emails, phone numbers, card-like numbers (Luhn check), configurable per tenant
- [ ] Grafana dashboard JSON committed to `deploy/grafana/`

### Day 10 — Sat Oct 31 · Kubernetes (**MVP done**)
- [ ] Multi-stage, distroless Dockerfile
- [ ] Helm chart: Deployment, Service, ConfigMap, Secret, HPA, liveness/readiness
- [ ] Deploy to `kind`; `make kind-up`

### Day 11 — Sun Nov 1 · Proof (**repo public**)
- [ ] k6 load test against the mock provider: RPS and gateway-added p50/p99
- [ ] `pprof` pass, fix the top hot spot, Go benchmarks for the hot path
- [ ] README: architecture diagram, benchmark table, demo GIF, design decisions

## Stretch (only if ahead)
- Exact-match response cache in Redis
- MCP tool-call allowlist
- Cost-based routing
