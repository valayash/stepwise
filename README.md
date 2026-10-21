# Tollgate

A multi-tenant LLM gateway written in Go. Point any OpenAI-compatible client at Tollgate and it routes requests to Anthropic, OpenAI or a mock provider — with per-tenant keys, rate limits, token budgets, provider failover, PII redaction and full tracing.

> **Status:** under active development. See [docs/PLAN.md](docs/PLAN.md) for the build plan and progress.

## Features

| Feature | Status |
| --- | --- |
| OpenAI-compatible `/v1/chat/completions` | ✅ |
| Anthropic + mock providers, model-prefix routing | ✅ |
| OpenAI-shaped errors, upstream error mapping, timeouts | ✅ |
| SSE streaming with client-disconnect cancellation | ✅ |
| Tenants, hashed API keys, usage ledger (Postgres) | 🚧 |
| Rate limits + token budgets (Redis) | 🚧 |
| Retries, circuit breaker, provider fallback | 🚧 |
| gRPC admin API | 🚧 |
| OpenTelemetry traces + Prometheus metrics | 🚧 |
| PII redaction | 🚧 |
| Helm chart, kind deployment | 🚧 |

## Quick start

```bash
make run            # starts on :8080 with the mock provider

curl -s localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-1","messages":[{"role":"user","content":"hello"}]}'
```

Streaming works the same way as OpenAI's API (`-N` turns off curl's buffering):

```bash
TOLLGATE_MOCK_DELAY=150ms make run

curl -sN localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-1","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

To route `claude-*` models to Anthropic:

```bash
ANTHROPIC_API_KEY=sk-ant-... make run
```

Works with the official OpenAI SDKs by setting the base URL:

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8080/v1", api_key="unused-for-now")
client.chat.completions.create(model="mock-1", messages=[{"role": "user", "content": "hi"}])
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `TOLLGATE_ADDR` | `:8080` | Listen address |
| `ANTHROPIC_API_KEY` | — | Enables `claude-*` models |
| `TOLLGATE_REQUEST_TIMEOUT` | `60s` | Upstream timeout per non-streaming request |
| `TOLLGATE_STREAM_TIMEOUT` | `5m` | Upstream timeout per streaming request |
| `TOLLGATE_MOCK_DELAY` | `0` | Pause between mock stream words (demos, load tests) |
| `TOLLGATE_MAX_BODY_BYTES` | `1048576` | Max request body |

## Development

```bash
make test     # go test -race with coverage
make lint     # gofmt + go vet
make build    # binary in ./bin
```

## Layout

```
cmd/tollgate        entrypoint, wiring, graceful shutdown
internal/api        OpenAI-compatible wire types
internal/provider   provider interface, router, Anthropic + mock adapters
internal/server     HTTP handlers, SSE streaming, middleware
internal/config     env-based configuration
docs/PLAN.md        build plan
```

## License

MIT
