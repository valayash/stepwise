# Stepwise design

This document explains how Stepwise works and why it is built this way. Each section states the decision, the reason, the alternatives and the trade-off.

## 1. Goals

- **No lost work.** A crash at any instant loses at most the step in flight.
- **Exactly-once side effects** for tools that honour an idempotency key.
- **Waiting costs nothing.** A run blocked on a human or a sub-agent holds no worker.
- **Share providers fairly** across many workers, and degrade gracefully when one is down.
- **Simple to run:** one binary and one database.

Non-goals: arbitrary workflow code (Temporal's domain), sub-second scheduling, multi-region.

## 2. Data model

```
runs    one agent execution: spec, input, status, lease, budget, parent link
steps   the run's history: one row per model call or tool call, unique (run_id, seq)
webhook_deliveries   transactional outbox
```

A run's state is **derived from its steps**. There is no separate "conversation" column: the message history the model sees is rebuilt from completed steps on every claim (`conversation()` in `engine/decide.go`). That makes crash recovery and inspection the same code path.

Run lifecycle:

```
queued ──claim──▶ running ──▶ completed | failed
   ▲                 │
   └── retry/yield ──┤
                     ▼
                  waiting (approval or sub-agents) ──resolved──▶ queued
cancelled: from any non-terminal state
```

Step statuses: `running` (started; may re-run after a crash) → `completed`; tool steps can also be `pending_approval` → `approved` → `running`, or `waiting_child`.

## 3. Claiming work

**Decision:** Postgres is the queue. Workers claim with

```sql
WITH claimable AS (
  SELECT id FROM runs
  WHERE (status = 'queued' AND available_at <= now())
     OR (status = 'running' AND lease_expires_at < now())   -- orphaned by a dead worker
  ORDER BY available_at LIMIT $n
  FOR UPDATE SKIP LOCKED)
UPDATE runs SET status = 'running', claims = claims + 1, lease_expires_at = now() + $lease ...
```

**Why:** the run state and the queue live in one database, so "claim the run" and "change its state" are one transaction. With a separate broker (Kafka, SQS, Redis), every hand-off between broker and database is a place where a crash can leave them disagreeing. `SKIP LOCKED` lets many workers claim concurrently without blocking each other or getting the same row.

**Trade-off:** Postgres is not a high-throughput broker. It is the right choice when each job is seconds to minutes of LLM work (Stepwise's case), not millions of tiny messages per second. Partial indexes on `status = 'queued'` and `status = 'running'` keep claims fast as finished runs pile up.

Wake-ups use `LISTEN/NOTIFY`, sent **inside** the transaction that made the change — Postgres delivers a notification only if its transaction commits, so a worker is never woken for a change that rolled back. Workers also poll (1 s + jitter) because notifications are best-effort.

## 4. Leases and fencing tokens

A claim is a **lease** (default 30 s) renewed by a heartbeat every lease/3. If a worker dies, its lease expires and another worker claims the run.

Leases alone are not enough. A worker can *pause* — a long GC, a stalled VM, a network partition — longer than its lease, then wake up and keep writing, while a second worker is already running the same run. Two writers corrupt the history.

**Decision:** every claim increments `claims`, and that number is the **fencing token**. Every worker write runs in a transaction that locks the run row and checks `status = 'running' AND claims = <my token>`. A stale worker's token is lower, so all its writes fail with `ErrFenced` and it abandons the run.

The token is checked by the store, not trusted from the worker's clock, and lease times use the database's `now()`, so clock drift between machines cannot make two workers believe they own a run.

## 5. Replay and `decide()`

Each claim loads the steps and calls `decide(run, steps)`, a **pure function** returning the next action:

1. A step still `running` was interrupted → re-execute it (same seq, same idempotency key).
2. An `approved` tool step → execute it.
3. The model's last turn requested tools without steps → start them, in order.
4. Tools waiting on a human or sub-agent → `wait` (release the worker).
5. All tool results in → call the model again (unless `max_turns` or the budget is reached).
6. The model gave a final answer → complete.

Because the decision depends only on saved state, a worker that resumes after a crash continues exactly where the dead one stopped, with no special recovery code.

**Execution guarantee:** steps are *at-least-once*; side effects are *effectively-once* when tools honour the idempotency key. A completed step is never executed again (`BeginStep` refuses it). Only the step that was in flight at the crash re-runs.

**Cost:** model calls are not idempotent, so a crash during a model call pays for that call twice. The chaos test measures this: about 13% of model calls were re-executed with a worker process killed every 250 ms — a deliberately extreme rate.

## 6. Idempotency keys

The key for a tool call is `run_id:tool_call_id`. The `tool_call_id` comes from the model's response, which is **saved before any tool runs**, so the ID never changes across retries or crashes. HTTP tools receive it as `Idempotency-Key`; MCP tools in `_meta["stepwise/idempotency_key"]`.

The chaos test checks the full chain. In the Postgres run (375 `SIGKILL`s), 38 times a worker died after the shop issued a refund but before the worker saved the result. Each time, the next worker called again with the same key and the shop returned the original refund. 1,000 orders, 1,000 refunds.

## 7. Human approvals and waiting

Tools listed in `require_approval` create a step in `pending_approval` instead of executing. When nothing else can progress, the worker **yields** the run as `waiting` and moves on. The run holds no worker, goroutine or memory while it waits — days are fine.

A decision updates the step and requeues the run in one transaction. A rejection becomes a tool error ("The reviewer rejected this action. Reviewer note: …") that the model sees, so the agent can adapt instead of failing.

Race handled: a decision can arrive while a worker still holds the run. `Yield(waiting)` re-checks for blockers inside its transaction and queues the run instead if nothing blocks any more.

## 8. Sub-agents

A tool of type `agent` spawns a child run. `SpawnChild` creates the child and marks the parent's step `waiting_child` in one transaction, and it is idempotent per step (a crash between "step started" and "child created" re-uses the child). Several sub-agent calls in one model turn run **in parallel** as independent durable runs.

When a child finishes, the same transaction completes the parent's step with the child's answer, adds the child's tokens to the parent's budget, and requeues the parent if nothing else blocks it.

**Deadlock avoidance:** finishing a child locks child then parent. Cancel locks the whole tree **deepest first** (a recursive CTE ordered by depth) — the same child-before-parent order — so the two can never wait on each other in a cycle.

## 9. Rate limiting

Providers limit requests per minute (RPM) and tokens per minute (TPM). Each rule is two **token buckets**.

The token cost of a call is unknown until it returns, so a worker **reserves an estimate** (prompt characters / 4 + the full output allowance) and **settles** the difference afterwards — refunding unused tokens, or putting the bucket into debt if it went over.

When a bucket is empty the run is **requeued with a delay** rather than the worker sleeping. Sleeping would idle a worker slot; requeueing keeps workers busy on runs that can proceed.

**Fail-open:** if the limiter itself is unavailable, calls proceed. Providers still enforce their limits, and their 429s are handled as retries. Stopping every agent because the limiter is down would be worse.

**Today vs next:** buckets are per process. With N worker processes, divide limits by N. The next step is a Redis implementation (one Lua script doing refill + take atomically) so the whole fleet shares one budget.

## 10. Retries, backoff and the circuit breaker

Errors are classified:

| Error | Handling |
| --- | --- |
| Model 429, 5xx, timeout, broken stream | Retry with backoff; `Retry-After` respected |
| Model 4xx (bad request, auth) | Fail the run (`fatal`) — retrying gives the same answer |
| Tool 4xx / tool says "no" | Returned to the model as a tool error; the agent decides |
| Tool 5xx, timeout, connection error | Retry with backoff; after `max_attempts`, returned to the model as an error |

Backoff is exponential with **equal jitter** (half fixed, half random), so a thousand runs failing together don't retry in synchronized waves, and the delay never collapses to zero.

A **circuit breaker** per provider opens after 5 consecutive failures, rejects calls for 30 s, then lets exactly one probe through (half-open). While it's open, runs are requeued without spending their retry budget on a provider that is known to be down.

## 11. Webhooks

**Transactional outbox:** a delivery row is inserted in the same transaction as the change it reports. The webhook can't be lost (the change committed, so the row exists) and can't describe a change that rolled back.

A dispatcher claims due rows with `SKIP LOCKED`, POSTs them, and retries with backoff up to 8 times before marking them dead. Delivery is **at-least-once**; receivers dedupe with the `Stepwise-Delivery-Id` header.

Signatures follow the Stripe scheme: `Stepwise-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>`. Signing the timestamp lets receivers reject replays of an old captured request.

## 12. Self-healing triage

When a top-level run fails, Stepwise starts a **triage agent** with three built-in tools: `get_failed_run`, `record_diagnosis` and `retry_run`. A rule-based classifier adds a suggested category (transient, bad_input, tool_bug, agent_loop, budget) to the prompt; the model confirms or corrects it.

`retry_run` requires human approval by default — an agent should not silently spend more money on a failing run. Built-in tools are scoped: a triage run can only read or retry the run it was created for.

## 13. Testing strategy

- **Conformance suite** (`store/storetest`): 15 behaviours every store must pass — exclusive claims, lease expiry and fencing, resume semantics, approvals, sub-agent resolution, cancel cascades, the outbox, and 8 goroutines racing to claim 40 runs.
- **Engine integration tests** with real HTTP and MCP: the refund flow, crash after a side effect, transient retries, exhaustion → triage → approved retry, sub-agents, budgets, cancellation, rate-limit deferral, graceful shutdown, invented tool names.
- **Race detector** on every test, repeated runs for timing-sensitive tests (this found a real race in the in-memory store).
- **Chaos harness** (`cmd/chaos`): with `-db`, real `stepwise worker` OS processes on Postgres killed with `SIGKILL`; without it, in-process workers with simulated kills. Faults are injected on top and every invariant is verified at the end. Both run in CI.

## 14. Known limitations

- Rate limits are per process (Redis limiter planned).
- Streaming is available in the gateway, not yet surfaced token-by-token for durable runs.
- A crash during a model call re-pays for that call. Providers don't offer idempotent completions, so this is inherent; it is measured rather than hidden.

## 15. Interview questions this project answers

**How do you guarantee a refund happens once if the worker can die at any time?**
You can't make the call itself exactly-once across a crash, so make it at-least-once and idempotent. The tool call ID is saved with the model's response before any tool runs; the tool receives `run_id:tool_call_id` as its idempotency key; a retry with the same key returns the original result. The chaos test shows 38 such retries across 375 process kills and zero double refunds.

**What happens if a worker freezes for a minute and then wakes up?**
Its lease expired, another worker claimed the run with a higher fencing token, and every write from the frozen worker is rejected because its token no longer matches. It abandons the run.

**Why Postgres instead of Kafka or Redis for the queue?**
State and queue in one transaction. Claiming a run and changing its state can't disagree. LLM jobs take seconds, so Postgres throughput is more than enough, and it's one fewer system to operate.

**How does `SKIP LOCKED` help?**
Concurrent claimers skip rows another transaction has locked instead of waiting for them, so N workers claim N different runs in parallel with no contention.

**Why requeue instead of sleeping when rate-limited?**
A sleeping worker holds a slot doing nothing. Requeueing frees it for runs that can proceed, and `available_at` brings the limited run back when capacity returns.

**How do you avoid deadlocks between sub-agents finishing and a parent being cancelled?**
Consistent lock order: always child before parent. Cancel locks the tree deepest-first with a recursive CTE.

**What does it cost to survive crashes?**
Measured: about 13% extra model calls with a worker process `SIGKILL`ed every 250 ms. Completed steps are never repeated.

**What would you build next?**
A Redis token bucket so the whole fleet shares one rate limit; OpenTelemetry traces with a span per step; partitioning the steps table by time for very large histories.
