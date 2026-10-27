# Testing Stepwise

Four levels, from no setup to production-like. Each one checks more of the real system.

## 1. Automated tests (no setup)

```bash
make test        # unit + integration tests with the race detector
make chaos       # 1,000 agents while workers are killed at random; fails on any lost run or double refund
```

To include the Postgres and Redis tests, point them at throwaway databases:

```bash
STEPWISE_TEST_DATABASE_URL=postgres://user:pass@localhost:5432/stepwise_test \
STEPWISE_TEST_REDIS_URL=redis://localhost:6379/0 make test
DATABASE_URL=postgres://... make chaos-pg      # real worker processes killed with SIGKILL
```

Every feature has a test; a few (per-tool limits, retention, invalid JSON from a model) are
**only** practical to check this way, because the free `mock` model never misbehaves.

## 2. By hand in the UI (no API keys)

```bash
STEPWISE_MONTHLY_TOKEN_LIMIT=100000 \
STEPWISE_ALLOWED_TOOL_HOSTS=localhost,127.0.0.1 \
go run ./cmd/stepwise dev
```

Open http://localhost:8080 → **New run** → pick an **Example** → **Start**.

| Example | What you should see |
| --- | --- |
| Refund with approval | Runs 5 steps, then **waiting** with Approve/Reject on `issue_refund`. Approve → completes with a refund ID (`RF-…`). Reject with a note → completes, and the model's answer mentions your note. |
| Approval expires after 20s | Waits; after 20 s plus up to 30 s for the sweep, the tool call shows **"rejected by stepwise: No decision within 20s"** and the run completes without refunding. |
| Structured JSON output | Completes with output exactly `{"amount":49.99,"order_id":"ORD-1001","reason":"damaged"}`. |
| Sub-agent (parallel research) | Completes; a child run (↳) appears in the list and its tokens count toward the parent. |
| Tool on a blocked host | Refused **before** the run starts: "tool url … is not in the allowed tool hosts". |
| Token budget too small | **Failed** with "token budget used up", plus a **Triage diagnosis: budget**. |

The header shows this month's tokens against the limit and how many approvals are waiting. To
see the spend limit refuse runs, restart with `STEPWISE_MONTHLY_TOKEN_LIMIT=1000` and start a
few runs: new ones fail with HTTP 429 "monthly token limit … reached".

Crash recovery by hand: start a refund, and while it waits for approval stop the server (Ctrl+C)
and start it again. In `dev` mode data is in memory, so this needs Postgres (level 4).

## 3. With a real model

```bash
export ANTHROPIC_API_KEY=sk-ant-...      # or OPENAI_API_KEY
go run ./cmd/stepwise dev
```

Then run all the checks below in one go (a few cents of tokens):

```bash
./examples/level3.sh                          # default claude-opus-5-5; MODEL=claude-sonnet-5-5 or gpt-… to switch
```

It prints PASS/FAIL per check. Run against `mock` it fails every check, which is expected: the
mock can't reason. By hand, change `"model": "mock"` in any UI example to a real model.
What the checks cover:

- **The agent reasons**: it should refuse a refund the policy doesn't allow (try `order_id: ORD-1003`).
- **Prompt caching works**: after a run with several turns, http://localhost:8080/metrics should
  show `stepwise_llm_tokens_total{direction="cached_input",…}` above zero. If it stays at zero,
  caching isn't taking effect.
- **Structured output** returns valid JSON from a real model.
- Don't set `temperature` for current Claude models: they reject it.
- **Known risk to watch:** current Claude models always think, and Stepwise doesn't yet send the
  model's thinking blocks back on the next turn. That's accepted by the API but drops the
  model's reasoning between turns; if multi-step runs look worse than expected, this is why.

## 4. Production-like (Postgres, separate processes)

```bash
export STEPWISE_DATABASE_URL=postgres://user:pass@localhost:5432/stepwise
go run ./cmd/stepwise migrate
go run ./cmd/stepwise api        # terminal 1: API + UI on :8080
go run ./cmd/stepwise worker     # terminal 2 (start several to share the work)
go run ./cmd/stepwise webhooks   # terminal 3: webhook delivery
```

Or everything at once with `docker compose up --build` (adds Redis, Jaeger and Prometheus).

Then try what only this setup shows:

- **Kill a worker mid-run** (`kill -9` its process) and watch another worker finish the run
  after the lease expires (30 s by default; `STEPWISE_LEASE=5s` makes it quicker).
- **Restart everything while a run waits for approval**; approve it afterwards and it completes.
- **Webhooks**: add `"webhook_url": "https://<your test endpoint>"` to a run; stop the
  `webhooks` process and deliveries wait, start it and they arrive.
- **Traces** in Jaeger (http://localhost:16686): one trace per run, with a span per model and
  tool call.
