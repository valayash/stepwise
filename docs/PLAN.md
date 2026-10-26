# Stepwise plan: from runtime to platform

## The idea

Developers sign up on the Stepwise platform, get an API key and call our API to run agents.
We run every agent durably, using **our model keys** by default. Their tools are called with
the credentials they give us, risky actions wait for a human approval, and they get results
by webhook or API. They see every run, approval, key and their usage in **our dashboard**.

The same code also ships as an **open-source, self-hosted** binary: one process plus Postgres,
with data staying on the customer's servers. That is our edge over Temporal, Restate and
Claude Managed Agents: any model, exactly-once tools, approvals and audit trail built in.

## Rules for building it

- **Minimal code.** Each phase reuses what exists; new code only where nothing fits.
- **Self-hosted keeps working.** With no orgs configured, Stepwise behaves exactly as today
  (one built-in `default` org, `STEPWISE_API_TOKEN` still works).
- **Green before merge.** Each phase ships with tests and keeps the chaos test at 0 lost, 0 doubled.

## Where we are

- [x] Durable engine, Postgres + memory stores, API, UI, approvals, sub-agents, webhooks, MCP
- [x] Chaos test: workers SIGKILLed every 250 ms → 0 lost runs, 0 double refunds
- [x] Redis rate limits, OpenTelemetry, Docker Compose
- [x] Codebase simplified (shared store hub, provider HTTP/SSE, API handlers)
- [x] Tool auth: per-tool `headers` with `${STEPWISE_SECRET_*}` references

---

## Phase 1: Multi-tenancy and API keys (backend)

**Goal:** many customers share one deployment and can never see each other's data.

**Design (reuse first):**
- One new column, `runs.org_id`. Steps and deliveries belong to runs, so they inherit it,
  and a sub-agent copies its parent's org in `SpawnChild`. No other table changes.
- The engine stays org-agnostic: workers claim any run. Isolation is enforced in **one
  place**, the API: a single `ownRun(ctx, id)` helper used by every `/v1/runs/{id}...`
  handler returns 404 for another org's run. `ListFilter` gains `OrgID`, which also scopes
  the list, approvals and children views.
- API keys live in a new `api_keys` table (`id, org_id, name, prefix, sha256 hash,
  created_at, last_used_at, revoked_at`). The format is `sw_live_<random>`, shown once and
  stored hashed. The existing `auth()` middleware resolves the key to an org and puts it in
  the request context.
- `orgs` table: `id, name, plan, created_at`.

**Work:**
- [ ] Migration `002_orgs.sql`: `orgs`, `api_keys`, `runs.org_id` (backfilled to `default`), index `(org_id, created_at)`
- [ ] `core.Run.OrgID`, `core.NewRun.OrgID`; both stores set and filter it
- [ ] Store methods: `CreateAPIKey`, `LookupAPIKey`, `RevokeAPIKey`, `ListAPIKeys` (memory + Postgres + conformance tests)
- [ ] `auth()` → org in context; `ownRun()` guard on every run route; SSE checks the org too
- [ ] `stepwise keys create --org <name>` CLI for self-hosted and bootstrapping
- [ ] Conformance + API tests: org A can't get, list, approve, cancel, retry or stream org B's runs

**Done when:** two orgs with separate keys run agents side by side and every cross-org request returns 404.

## Phase 2: Accounts and dashboard

**Goal:** people sign up, log in, create API keys and see their runs in our web dashboard.

**Design (reuse first):**
- **Extend the existing embedded UI** (`internal/api/ui.html`, no build step). It already
  has the runs list, step timeline, approvals and cancel/retry. We add login, keys and
  usage screens, not a new frontend.
- The dashboard calls the same `/v1` API. `auth()` accepts **either** an API key **or** a
  session cookie, and both resolve to an org. No separate dashboard backend.
- Email + password with bcrypt (`golang.org/x/crypto`, already a dependency). Sessions are
  random tokens stored hashed with an expiry. Google login comes later if users ask.

**Work:**
- [ ] Migration: `users (id, email, password_hash)`, `memberships (user_id, org_id, role)`, `sessions`
- [ ] `POST /auth/signup` (creates user + org + first key), `/auth/login`, `/auth/logout`
- [ ] Session cookie: HttpOnly, Secure, SameSite=Lax; CSRF check on dashboard POSTs
- [ ] `/v1/keys` (list, create, revoke) for the dashboard
- [ ] UI: login/signup screen, API keys page, org name in the header; existing pages scoped automatically
- [ ] Approvals record the logged-in user as `decided_by`

**Done when:** a new user signs up, copies a key, starts a run with `curl`, and sees it live in the dashboard.

## Phase 3: Our model keys, quotas and usage

**Goal:** customers use our Anthropic/OpenAI keys safely, and we can see and cap what each org costs.

**Design (reuse first):**
- Model keys stay server config (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`); nothing to store per org.
- Usage = `SUM(tokens_used)` over an org's runs grouped by day and model. It's one query on
  data we already record, with no new table until it gets slow.
- Plans are a small Go map (`free`, `pro`, …) giving monthly tokens, concurrent runs and
  rate limits. `CreateRun` rejects runs over quota with a clear 402/429. Per-run budgets
  (already enforced) cap a single runaway agent.
- Per-org rate limits reuse the existing limiter (local or Redis) with an org-prefixed key,
  so one customer can't use up our provider limits.

**Work:**
- [ ] `orgs.plan` + plans map; quota check in `createRun`
- [ ] `GET /v1/usage?from=&to=`: tokens and runs per day per model, with estimated cost from a price table
- [ ] Org-scoped limiter key; concurrent-runs cap per org
- [ ] Dashboard usage page (reuses the existing UI style; a simple table + bar chart)
- [ ] Alert when an org hits 80% / 100% of its quota (webhook + dashboard banner)

**Done when:** a free org that exceeds its quota is stopped cleanly, and the dashboard shows exactly what it used.

## Phase 4: Per-org webhooks and tool secrets

**Goal:** every customer has its own webhook signing secret and its own tool credentials.

**Design (reuse first):**
- `orgs.webhook_secret`. The dispatcher already signs with a secret; it now takes the run's
  org secret instead of the global one. The global secret remains the fallback for self-hosted.
- Tool secrets: a `secrets (org_id, name, encrypted_value)` table, encrypted with AES-GCM
  under `STEPWISE_ENCRYPTION_KEY`. `withHeaders` already resolves `${STEPWISE_SECRET_*}`;
  it gets an extra lookup that checks the run's org secrets before the environment, so
  agent specs don't change.

**Work:**
- [ ] Webhook secret per org: generate at signup, rotate from the dashboard
- [ ] Secrets store + encryption helper + `/v1/secrets` (write-only: values never returned)
- [ ] Pass the org's secrets into `tools.Call`; tests that org A can't use org B's secret
- [ ] Dashboard: webhook settings and secrets pages, delivery log (API already exists)

**Done when:** two orgs call tools with different credentials and verify webhooks with different secrets.

## Phase 5: Launch

**Goal:** a public, hosted Stepwise that strangers can try, plus the open-source release.

- [ ] Deploy: API + workers on a managed host, managed Postgres with daily backups, Redis, a domain with TLS
- [ ] Signup protection: email verification, a small free quota, rate limit on `/auth/*`
- [ ] Docs site: quickstart (`curl` in 5 minutes), API reference, tool contract (`Idempotency-Key`), approvals, webhooks
- [ ] Landing page: the chaos-test story, comparison, waitlist/signup
- [ ] Legal: terms, privacy policy, data retention; check Anthropic/OpenAI commercial terms for serving third parties; India DPDP Act basics
- [ ] Open-source release: license check, README polish, GitHub release binaries

## Phase 6: Billing

- [ ] Payments: **Razorpay** for Indian customers, Stripe for international. Plans first, metered usage after.
- [ ] Prepaid credits for the model usage we pay for, so a runaway customer can't run up our bill
- [ ] Invoices and a billing page in the dashboard

## Later (pull in when customers ask)

- Live token streaming into runs (design ready: ephemeral deltas over the existing event stream)
- Saved, versioned agents (`POST /v1/agents`, then start runs by agent name)
- Structured output (`response_schema` on the agent → JSON output)
- Python and TypeScript SDKs (thin wrappers over the HTTP API)
- Fair scheduling across orgs in `ClaimRuns`
- Bring-your-own model key per org (encrypted like tool secrets)
- Customer-hosted workers for tools behind firewalls
- Teams: invites and roles; SSO; audit-log export; SOC 2

## Alongside the build: find users

- Talk to 10–20 teams running agents in production (start with Indian agent startups and fintech/BFSI); ask what breaks today
- Get 2–3 design partners onto the hosted beta after Phase 2
- Let what they hit decide the order of Phase 3+ and the "Later" list

## Decisions to confirm

| Decision | Recommended | Why |
| --- | --- | --- |
| Dashboard tech | Extend the embedded UI | Reuses the runs and approvals screens we have; no second codebase |
| Login | Email + password first | Smallest secure option; add Google later |
| Model keys | Ours by default; BYO key later | Easiest onboarding; matches the idea |
| First market | Indian agent startups, fintech | Gap in the local market; self-hosted + data-in-India angle |
| Payments | Razorpay + Stripe | Indian and international customers |
| Order | Phases 1 → 2 → 3, then 4/5 by user need | Each phase builds on the previous one |
