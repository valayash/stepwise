# Stepwise plan

## The idea

Developers call the Stepwise API to run agents. We run every agent durably on **our model
keys**, call their tools with their credentials, pause risky actions for a human approval,
and return results by webhook or API. Everything is visible in the built-in dashboard.

The edge over Temporal, Restate and Claude Managed Agents: one binary plus Postgres, any model,
exactly-once tools, approvals and an audit trail built in.

## Scope for now

- **One user.** Auth is the single static key in `STEPWISE_API_TOKEN` (already supported).
  No login, no orgs, no billing. See [Not now](#not-now).
- **Our model keys** from the environment (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`).

## Rules

- **Minimal code.** Reuse what exists. New code only where nothing fits.
- **Independent packages.** Each package does one job and depends on as little as possible.
  See [Package boundaries](#package-boundaries).
- **Independent services.** API, workers and the webhook dispatcher run as separate processes
  from the same binary, sharing only Postgres (and Redis, if configured).
- **Green before merge.** Tests pass and the chaos test stays at 0 lost runs, 0 doubled refunds.

## Done so far

- [x] Durable engine, Postgres and memory stores, API, UI, approvals, sub-agents, webhooks, MCP
- [x] Chaos test: workers SIGKILLed every 250 ms gives 0 lost runs, 0 double refunds
- [x] Redis rate limits, OpenTelemetry, Docker Compose
- [x] Codebase simplified (shared store hub, provider HTTP/SSE, API handlers)
- [x] Tool auth: per-tool `headers` with `${STEPWISE_SECRET_*}` references

---

## Package boundaries

The dependency graph (enforced by `internal/archtest`):

```
leaves (depend on nothing internal): breaker, config, mcp, metrics, provider, ratelimit, telemetry
core      → provider            (chat types: Message, ToolCall, Response)
store     → core                memory, postgres → store, core
tools     → core, mcp, telemetry
webhook   → store, breaker, metrics
engine    → core, store, tools, provider, ratelimit, breaker, metrics, telemetry   (the orchestrator)
api       → core, store, tools, provider
gateway   → provider            (owns its OpenAI wire types)
cmd/*     → everything          (wiring only)
```

Target rules:
1. **Leaves stay leaves.** `provider`, `mcp`, `breaker`, `ratelimit`, `metrics` and `telemetry`
   import no internal package. Each could be its own module.
2. **`core` depends only on `provider`**, for the shared chat types. Nothing else.
3. **Edge packages never import each other.** `api`, `gateway`, `webhook` and `engine` only
   meet in `cmd/stepwise`, which does the wiring.
4. **`config` is a leaf.** It reads strings, numbers and durations. Feature packages parse their own formats.
5. **A test enforces it.** One test runs `go list` and fails on any import outside these rules.

## Phase 1: Clean boundaries and independent services

- [x] Move the OpenAI wire types from `api/types.go` into `gateway` (gateway → provider only)
- [x] `config` keeps `STEPWISE_RATE_LIMITS` as a string; `cmd/stepwise` parses it with `ratelimit.ParseRules`
- [x] Dependency-rules test (`internal/archtest`) encoding the rules above
- [x] `stepwise webhooks` role, so the dispatcher can run on its own like `api` and `worker`
- [x] Docker Compose runs api, worker and webhooks as separate services

**Done when:** the dependency test passes, and each of api, worker and webhooks can be started,
stopped and scaled alone without affecting the others' work.

## Phase 2: Core features, guardrails and cost

Core means: agents run reliably, safely and cheaply, and results are usable. No new tables or columns.

- [x] **Spend limit.** Optional `STEPWISE_MONTHLY_TOKEN_LIMIT`: new runs are rejected once this
      month's token usage passes it. Usage also exposed as `GET /v1/usage`.
- [x] **Prompt caching.** Anthropic `cache_control` on the system prompt, tools and latest
      message, so each turn doesn't pay full price for the conversation again.
- [x] **Per-tool call limits.** `"limits": {"issue_refund": 1}` on the agent; extra calls are
      returned to the model as errors, like unknown tools.
- [ ] **Tool host allowlist.** Optional `STEPWISE_ALLOWED_TOOL_HOSTS`: tools may only call
      these hosts, so a spec can't reach internal addresses.
- [ ] **Approval expiry.** `"approval_timeout": "24h"` on the agent: unanswered approvals are
      auto-rejected and the model is told why.
- [ ] **Structured output.** An optional `response_schema` on the agent makes the final answer
      JSON matching the schema, stored in the existing `output` column.
- [ ] **Retention.** Optional `STEPWISE_RETENTION_DAYS`: finished runs older than that are deleted.

**Done when:** spending stops at the limit, repeated turns hit the prompt cache, a run can't
refund twice or call internal hosts, no approval waits forever, programs get valid JSON, and
old runs are cleaned up.

## Phase 3: Launch

- [ ] Deploy: one managed host with Postgres (daily backups), TLS and a domain; the API token is set
- [ ] Docs: a 5-minute `curl` quickstart, API reference, the tool contract (`Idempotency-Key`,
      `headers`), approvals, webhooks
- [ ] Landing page: the chaos-test story and a comparison
- [ ] Open-source release: README polish, GitHub release binaries
- [ ] Check Anthropic and OpenAI terms for running agents for others on our keys

## Alongside the build: find users

- Talk to teams running agents in production, starting with Indian agent startups and
  fintech/BFSI. Ask what breaks today.
- Get 2–3 design partners onto it after Phase 2. Their needs decide what moves up from "Not now".

## Not now

Pulled in only when real users need them:

- Saved agents: create an agent once (with `{{variables}}` in its instructions), then start runs
  by name with values; an `agents (name, version, spec)` table
- Live token streaming into the run timeline (ephemeral deltas over the `/events` stream)
- Usage tab in the UI (the `/v1/usage` API lands with the spend limit)
- Multi-tenancy: `org_id` on runs, per-org API keys, isolation tests
- Login and accounts: users, sessions, a signup flow
- Billing: Razorpay and Stripe, plans, prepaid credits
- Per-org webhook secrets, tool secrets and model keys
- Python and TypeScript SDKs, fair scheduling, customer-hosted workers, SSO, SOC 2
