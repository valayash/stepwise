# Reference

Full API, configuration and commands. The [README](../README.md) is the short version.

## API

| Method | Path | |
| --- | --- | --- |
| `POST` | `/v1/runs` | Start a run. `Idempotency-Key` header (or field) dedupes retries of the request itself |
| `GET` | `/v1/runs?status=&parent_run_id=&limit=` | List runs |
| `GET` | `/v1/runs/{id}` | Run, its steps and its sub-agents |
| `GET` | `/v1/runs/{id}/events` | Live updates (server-sent events) until the run ends |
| `POST` | `/v1/runs/{id}/steps/{step}/approve` · `/reject` | Decide a pending tool call (`note`, `decided_by`) |
| `POST` | `/v1/runs/{id}/cancel` · `/retry` | Cancel (cascades to sub-agents) · retry a failed run |
| `POST` | `/v1/runs/{id}/kill` | Simulate a worker crash mid-run (dev/serve only); the run resumes from its saved steps |
| `POST` | `/v1/runs/{id}/messages` | Continue a completed run as a conversation (`content`); the next reply arrives like any run's output. Send an `Idempotency-Key` header to make retries safe |
| `GET` | `/v1/approvals` | Inbox of every tool call waiting for a human |
| `GET` | `/v1/usage` | This month's runs and tokens, and the monthly limit |
| `GET` | `/v1/runs/{id}/deliveries` | Webhook delivery log |
| `POST` | `/v1/chat/completions` | OpenAI-compatible passthrough |
| `GET` | `/metrics` · `/healthz` · `/readyz` | Operations |

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `STEPWISE_ADDR` | `:8080` | Listen address |
| `STEPWISE_DATABASE_URL` | — | Postgres; empty only allowed in `dev` |
| `STEPWISE_API_TOKEN` | — | Require `Authorization: Bearer` on `/v1` |
| `ANTHROPIC_API_KEY` · `OPENAI_API_KEY` | — | Enable `claude-*` · `gpt-*`/`o*` models |
| `STEPWISE_WORKERS` | `8` | Concurrent runs per process |
| `STEPWISE_LEASE` | `30s` | Lease length; a dead worker's runs resume after this |
| `STEPWISE_MAX_ATTEMPTS` | `6` | Tries per step before a run fails |
| `STEPWISE_RATE_LIMITS` | — | e.g. `claude-=rpm:50,tpm:40000;gpt-=rpm:500` |
| `STEPWISE_REDIS_URL` | — | Share rate limits across every worker process |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Send traces, e.g. `http://jaeger:4318` |
| `STEPWISE_METRICS_ADDR` | — | Metrics + health port for worker-only processes |
| `STEPWISE_DEMO_SHOP_URL` | — | Shop URL the UI's example uses (reachable by workers) |
| `STEPWISE_ALLOWED_TOOL_HOSTS` | any | Comma-separated hosts tools may call (`api.shop.com,*.shop.com`), redirects included; set it whenever you run agent specs you don't fully control |
| `STEPWISE_RETENTION_DAYS` | keep | Delete finished runs (with sub-agents, steps and webhook deliveries) older than this many days |
| `STEPWISE_MONTHLY_TOKEN_LIMIT` | — | Refuse new runs (HTTP 429) once this month's tokens reach it; see `GET /v1/usage` |
| `STEPWISE_WEBHOOK_SECRET` | — | HMAC key for webhook signatures |
| `STEPWISE_SECRET_*` | — | Secrets tool `headers` may reference, e.g. `"Authorization": "Bearer ${STEPWISE_SECRET_SHOP}"`; resolved per call, only sent to the tool's own host, never stored |
| `STEPWISE_TRIAGE` · `_MODEL` · `_AUTO_RETRY` | `true` · auto · `false` | Self-healing triage agent |
| `STEPWISE_MOCK_LATENCY` · `_FAIL_RATE` | `0` | Make the mock model slow or flaky |

## Commands

| Command | What it runs |
| --- | --- |
| `stepwise dev` | API, UI, workers, webhooks and the demo shop in one process, in memory |
| `stepwise serve` | API + workers + webhooks against Postgres (`STEPWISE_DATABASE_URL`) |
| `stepwise api` / `worker` / `webhooks` | Run each service separately; they share only Postgres. Scale any of them horizontally |
| `stepwise migrate` | Apply database migrations |
| `stepwise demo-shop` | The demo shop MCP server on its own |
| `stepwise healthcheck` | Exit 0 if the local server is healthy (for containers without curl) |

## All features

| | |
| --- | --- |
| **Crash-safe execution** | Steps persisted before the next starts; replay on resume; leases + fencing tokens against zombie workers |
| **Exactly-once side effects** | Stable idempotency key per tool call (`Idempotency-Key` header for HTTP tools, `_meta` for MCP) |
| **Tools** | HTTP tools, any **MCP** server (Streamable HTTP; tools discovered with `tools/list`), and sub-agents; per-tool `headers` with `${STEPWISE_SECRET_*}` references for auth |
| **Conversations** | `POST /v1/runs/{id}/messages` continues a completed run: one run = one conversation, one audit trail, one budget; `max_turns` applies per message |
| **Human-in-the-loop** | `require_approval` per tool; approve or reject via API or UI; rejections are shown to the model so it can adapt; `approval_timeout` (e.g. `"24h"`) rejects calls nobody answered |
| **Sub-agents** | A tool can be another agent; children run in parallel as durable runs; tokens roll up to the parent's budget |
| **Self-healing triage** | When a run fails, a triage agent reads it, records a diagnosis, and asks (with approval) to retry. Provider outages are diagnosed by rules with no LLM call, and at most 10 triage runs are active at once, so a burst of failures can't start hundreds |
| **Provider resilience** | Token-bucket rate limits (RPM + TPM) shared by every worker through Redis, circuit breaker per provider, retries with jittered exponential backoff, `Retry-After` support |
| **Structured output** | `response_schema` (JSON Schema) on the agent: the final answer is JSON matching it, via the provider's native structured-output mode; anything else fails the run instead of returning bad JSON. Write strict schemas: `"additionalProperties": false` on every object and every property in `required` (both providers' strict modes need it) |
| **Budgets and limits** | Per-run token budget and `max_turns`; per-tool call limits (`"limits": {"issue_refund": 1}`); monthly token limit |
| **Webhooks** | Transactional outbox; HMAC-SHA256 signatures with timestamps; retries; dead-lettering |
| **Observability** | OpenTelemetry traces (one trace per run: the trace ID *is* the run ID; `gen_ai.*` attributes; `traceparent` sent to tools); Prometheus metrics; JSON logs; live updates over SSE |
| **Web UI** | Run list, step timeline with re-execution badges, approvals, cancel/retry — embedded, no build step |
| **Gateway** | OpenAI-compatible `/v1/chat/completions` passthrough (streaming included) for plain LLM calls |
| **Small dependency set** | Standard library plus `pgx` (Postgres), `go-redis` and the OpenTelemetry SDK |

## Layout

```
cmd/stepwise        the binary: dev, serve, api, worker, migrate, demo-shop
cmd/chaos           crash/fault-injection harness
internal/engine     workers, decide(), model + tool execution, triage agent
internal/store      Store interface, conformance suite, memory + Postgres stores
internal/provider   Anthropic, OpenAI and mock adapters; tool calling; streaming
internal/tools      HTTP/MCP/built-in tool runner; spec validation
internal/mcp        MCP client and server (Streamable HTTP)
internal/ratelimit  token-bucket limiter: local and Redis (atomic Lua)
internal/telemetry  OpenTelemetry setup; trace ID = run ID
internal/breaker    circuit breaker + backoff
internal/webhook    outbox dispatcher, HMAC signing
internal/api        REST API, SSE, embedded UI
internal/gateway    OpenAI-compatible passthrough
internal/demo       demo shop (MCP) with idempotent refunds
```
