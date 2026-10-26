# Stepwise plan

## The idea

Developers call the Stepwise API to run agents. We run every agent durably on **our model
keys**, call their tools with their credentials, pause risky actions for a human approval,
and return results by webhook or API. Everything is visible in the built-in dashboard.

The edge over Temporal, Restate and Claude Managed Agents: one binary plus Postgres, any model,
exactly-once tools, approvals and an audit trail built in.

## Agents we run

Stepwise is for agents that **take real actions** and must not lose work or repeat it. What we
target, and what we leave to other tools:

| We target | Example | Why Stepwise fits |
| --- | --- | --- |
| **Action agents with approvals** | Support refunds, payouts, account changes, order fixes | Exactly-once tools, `require_approval`, `limits`, audit trail |
| **Back-office workflows** | KYC and onboarding checks, invoice processing, CRM updates | Long runs survive crashes; JSON results via `response_schema` |
| **Operations and triage** | IT or ops tickets: investigate, then fix with sign-off | Tools over HTTP/MCP; approvals that can wait days |
| **Extraction and decisions for programs** | Classify a case, decide eligibility, return JSON | `response_schema`, budgets, spend limit |
| **Orchestrator with specialists** | Research sub-agents feed a deciding agent | Sub-agents as durable child runs, in parallel |

Not our target (better served elsewhere): coding agents that need a sandbox or computer use,
real-time voice or chat that needs streamed tokens, open-ended browsing agents, and agent
"swarms" without a clear leader.

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
- [x] **Tool host allowlist.** Optional `STEPWISE_ALLOWED_TOOL_HOSTS`: tools may only call
      these hosts, so a spec can't reach internal addresses.
- [x] **Approval expiry.** `"approval_timeout": "24h"` on the agent: unanswered approvals are
      auto-rejected and the model is told why.
- [x] **Structured output.** An optional `response_schema` on the agent makes the final answer
      JSON matching the schema, stored in the existing `output` column. Uses native structured output
      (Anthropic `output_config.format`, OpenAI `response_format`): forced tool use 400s on current models.
- [x] **Retention.** Optional `STEPWISE_RETENTION_DAYS`: finished runs older than that are deleted.

**Done when:** spending stops at the limit, repeated turns hit the prompt cache, a run can't
refund twice or call internal hosts, no approval waits forever, programs get valid JSON, and
old runs are cleaned up.

## Phase 2b: Performance (from the code review)

Ranked by impact for the code it costs. Numbers come from the review's own Postgres
measurements and should be re-checked when each item is built.

- [ ] **Claim query.** Split `ClaimRuns` into two `LIMIT … SKIP LOCKED` branches, each on its
      partial index, then `UNION ALL`. Measured: 13.7 ms → 0.2 ms per claim with 2,000 queued,
      ~7x claims/s.
- [ ] **Webhook dispatcher wake-ups.** Stop claiming deliveries on every `run_changed` event;
      poll, or wake only on a deliveries event.
- [ ] **Live view (SSE) queries.** Drop `isChild` (a `GetRun` per fleet event per viewer;
      parents are already notified) and don't resend unchanged snapshots for runs with children.
- [ ] **NOTIFY volume.** `pg_notify` serializes commits (measured −69% tps at 32 clients).
      Stop notifying `run_changed` from BeginStep and SaveStep, and batch the rest.
- [ ] **Deferred yields.** Don't send `runs_ready` for runs requeued into the future.
- [x] **Approvals inbox N+1.** Fixed with `PendingApprovals` (one query).
- [ ] **Connection pool.** Idle connections = open connections.
- [ ] **Fewer writes per step.** `fillfactor` on `runs`, drop the redundant `touch`, lighter
      `lockFenced`.
- [ ] **Triage storms.** Skip the LLM triage call when the rules already classify a failure as transient.

## Future: multi-agent

Research and fit: [docs/research/multi-agent.md](research/multi-agent.md). Orchestrator–workers
already works (sub-agents as tools, parallel, durable). Pull these in only when users need them,
in this order:

- [ ] Cap parallel sub-agents per run (`max_parallel_agents`)
- [ ] Structured hand-back: a sub-agent's `response_schema` JSON reaches the parent as data
- [ ] One trace across agents (child traces linked to the parent)
- [ ] Hand-offs (`"type": "handoff"`: another agent takes over the rest of the run)
- [ ] A2A: call remote agents as a tool type, then expose Stepwise agents as A2A servers
- [ ] Shared state / peer messaging, only with strong evidence it's needed

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
