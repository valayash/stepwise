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

The dependency graph today:

```
leaves (depend on nothing internal): breaker, mcp, metrics, provider, ratelimit, telemetry
core      → provider            (chat types: Message, ToolCall, Response)
store     → core                memory, postgres → store, core
tools     → core, mcp, telemetry
webhook   → store, breaker, metrics
engine    → core, store, tools, provider, ratelimit, breaker, metrics, telemetry   (the orchestrator)
api       → core, store, tools, provider
gateway   → api, provider       ✗ only uses OpenAI wire types that live in api/types.go
config    → ratelimit           ✗ a settings loader shouldn't depend on a feature package
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

- [ ] Move the OpenAI wire types from `api/types.go` into `gateway` (gateway → provider only)
- [ ] `config` keeps `STEPWISE_RATE_LIMITS` as a string; `cmd/stepwise` parses it with `ratelimit.ParseRules`
- [ ] Dependency-rules test (`internal/archtest`) encoding the rules above
- [ ] `stepwise webhooks` role, so the dispatcher can run on its own like `api` and `worker`
- [ ] Docker Compose runs api, worker and webhooks as separate services

**Done when:** the dependency test passes, and each of api, worker and webhooks can be started,
stopped and scaled alone without affecting the others' work.

## Phase 2: Features for real use

Ordered by what a real user hits first:

- [ ] **Saved agents.** `POST /v1/agents` stores a named, versioned spec. Runs start with
      `{"agent": "refund-agent", "input": "..."}`. Reuses the spec validation (`tools.Prepare`) and store patterns.
- [ ] **Structured output.** An optional `response_schema` on the agent makes the final answer
      JSON matching the schema, returned in `output_json`. Reuses the tool-calling path:
      the final answer becomes a forced "respond" tool.
- [ ] **Usage view.** `GET /v1/usage?from=&to=` returns tokens and runs per day per model,
      with estimated cost. One query over `tokens_used`, which we already record. Adds a usage tab in the UI.
- [ ] **Live token streaming.** Model text appears in the run timeline as it is generated.
      Ephemeral deltas go over the existing `/events` stream; the saved step stays the source of truth.
- [ ] **Spend guard.** Optional `STEPWISE_MONTHLY_TOKEN_LIMIT`: new runs are rejected once
      this month's usage passes it. Protects our model keys. Reuses the usage query.

**Done when:** a user saves an agent once, starts runs by name, gets JSON results, watches
tokens stream in the UI, and sees what they spent.

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

- Multi-tenancy: `org_id` on runs, per-org API keys, isolation tests
- Login and accounts: users, sessions, a signup flow
- Billing: Razorpay and Stripe, plans, prepaid credits
- Per-org webhook secrets, tool secrets and model keys
- Python and TypeScript SDKs, fair scheduling, customer-hosted workers, SSO, SOC 2
