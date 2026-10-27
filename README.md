# Stepwise

**A durable runtime for AI agents.** You describe an agent in JSON (model, instructions, tools), and Stepwise runs it. Every step is saved before the next one starts, so crashes don't lose work, actions like refunds happen exactly once, and an agent can wait days for a human approval.

One Go binary plus Postgres. Works with Claude, OpenAI, or a free built-in mock model.

![CI](https://github.com/valayash/stepwise/actions/workflows/ci.yml/badge.svg)

## Try it

```bash
git clone https://github.com/valayash/stepwise && cd stepwise
go run ./cmd/stepwise dev        # no API keys, no database
```

Open http://localhost:8080 → **New run** → pick an example → **Start**. The refund example stops to ask for approval; click **Approve** and it finishes.

To see crash recovery, start with `STEPWISE_MOCK_LATENCY=3s` and click **Kill worker** while a run is working: it continues from its last saved step.

## An agent

```bash
curl -s localhost:8080/v1/runs -d '{
  "agent": {
    "model": "mock",
    "system": "You are a refund agent. Look up the order, check the policy, refund if eligible.",
    "tools": [{"type": "mcp", "url": "http://localhost:8080/demo/mcp"}],
    "require_approval": ["issue_refund"]
  },
  "input": "Earbuds arrived broken. order_id: ORD-1001, amount: 49.99"
}'
```

The run looks up the order, checks the policy, then waits for a human before `issue_refund`. Get results from `GET /v1/runs/{id}`, a `webhook_url`, or the live stream. Use `"model": "claude-sonnet-5-5"` with `ANTHROPIC_API_KEY`, or a `gpt-*` model with `OPENAI_API_KEY`, for a real model.

## How it works

1. A worker claims a run from Postgres, with a lease that other workers respect.
2. It reads the run's saved steps and decides the next one: call the model, run a tool, wait, or finish.
3. It saves the step's result before doing anything else.
4. If the worker dies, another one picks the run up and continues from the last saved step. Tool calls carry an idempotency key, so a repeated call can't refund twice.

## What's included

- **Tools:** HTTP and MCP, with credentials (`${STEPWISE_SECRET_*}`), a host allowlist, and per-tool call limits
- **People in the loop:** approvals with expiry, and conversations (`POST /v1/runs/{id}/messages`)
- **Results programs can use:** JSON output that matches a schema (`response_schema`)
- **Cost control:** token budgets, `max_turns`, a monthly token limit, prompt caching, shared rate limits
- **Sub-agents** that run in parallel, as durable runs
- **Operations:** webhooks, OpenTelemetry traces, Prometheus metrics, a web UI, automatic failure triage, retention

## Proof

1,000 refund agents ran on 8 worker processes while a worker was killed with `SIGKILL` every 250 ms, with 5% of model and tool calls failing on top:

| | |
| --- | --- |
| Runs completed | **1,000 / 1,000** |
| Runs lost | **0** |
| Orders refunded twice | **0** |

```bash
make chaos                          # in one process
DATABASE_URL=... make chaos-pg      # real processes on Postgres
```

[Full report](docs/chaos-report.md)

## Run it for real

```bash
docker compose up --build           # Postgres, Redis, API, workers, webhooks, Jaeger, Prometheus
```

Or with your own Postgres: set `STEPWISE_DATABASE_URL`, then `stepwise serve`, or run `api`, `worker` and `webhooks` as separate processes.

## Docs

- [Reference](docs/REFERENCE.md): every API endpoint, setting and command
- [Testing](docs/TESTING.md): how to check each feature
- [Design](docs/DESIGN.md): how it works inside, and why
- [Plan](docs/PLAN.md): what's done and what's next

MIT license
