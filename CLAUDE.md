# Stepwise

Durable runtime for AI agents in Go: every step is saved before the next, so runs survive crashes, side effects are exactly-once, and approvals can wait for days.

## Direction
- Open-source, self-hosted, model-agnostic runtime for agents that take real actions (refunds, payouts, account changes): approvals + full audit trail + proof (chaos test).
- Differentiators vs Temporal / Restate / Claude Managed Agents: one binary + Postgres, data stays on the customer's servers, works with any model, exactly-once tools and approvals built in.
- Roadmap and phases: see [docs/PLAN.md](docs/PLAN.md). Work through it in order; tick items as they land.

## How to write code here
- Minimal code. Reuse existing helpers and packages before writing anything new; prefer one shared helper over two copies.
- Don't add abstractions, options or files a feature doesn't need. Match the surrounding style and comment density.
- Every change keeps all tests and the chaos test green.

## Checks
```bash
gofmt -l . && go vet ./... && go test -race ./...
# Postgres/Redis tests run when these are set:
STEPWISE_TEST_DATABASE_URL=postgres://... STEPWISE_TEST_REDIS_URL=redis://... go test ./...
go run ./cmd/chaos -runs 200 -workers 4 -kill-every 120ms -latency 20ms -lease 300ms
```
