# Runtime optimizations: research and fit for Stepwise

Status: research, written October 2026. Nothing here is scheduled; candidates for [PLAN.md](../PLAN.md).
Scope: what a runtime that **calls hosted LLM APIs** can do. Most agent-serving papers (Autellix,
Continuum) optimize GPU schedulers and KV caches; only their *policies* carry over to a client.
Vendor docs and blogs are marked **(vendor)**. arXiv was unreachable from the research
environment, so paper numbers were checked against abstracts on conference pages and arXiv
listings, not full PDFs.

## 1. Summary

| # | Idea | Source | Expected gain | Code | Risk |
| --- | --- | --- | --- | --- | --- |
| 1 | **Provider 429s become fleet-wide backpressure**, not failures: pause the model's bucket until `retry-after`, don't trip the breaker, don't spend retry attempts | HiveMind (2026), Netflix concurrency-limits (2018), Anthropic rate-limit docs (vendor) | No runs failed as `retries_exhausted` under quota pressure; fewer wasted 429 calls and claims | ~50 lines (engine, ratelimit Local + Lua) | Low |
| 2 | **Cache-aware rate-limit accounting**: charge uncached input + real output, not the whole prompt + `max_tokens` | Anthropic rate-limit docs (vendor); "Don't Break the Cache" (2026) | Several times more usable Anthropic throughput for long cached runs (80% hit rate → 5x ITPM per Anthropic) | ~25 lines (provider, engine) | Low |
| 3 | **Cache-aligned observation masking** for the planned "shrink old tool results": mask, don't summarize; move the mask boundary in chunks | Complexity Trap (NeurIPS 2025), ACON (ICML 2026), Anthropic context editing (vendor) | ~50% lower cost on long runs at equal success (SWE-bench); prompt cache mostly preserved | ~30 lines (`conversation()`) | Low–medium |
| 4 | **Least-attained-service order for rate-limited requeues**, with jitter | Autellix (2025), AgentRM (2026) | Lower median run completion time under contention; no synchronized retry herds | ~10 lines (`admit`) | Low |
| 5 | **Discount tier for non-urgent runs** (`"service_tier": "flex"`/batch) | OpenAI flex processing, Anthropic Message Batches (vendor) | 50% off model tokens for bulk/back-office runs | ~15 lines (OpenAI flex); ~150 (Anthropic batch) | Medium (latency) |

Common thread: Stepwise's bottleneck is **provider quota and price**, not workers or Postgres.
The cheapest gains are in how the limiter, breaker and context builder treat the provider.

## 2. Recommendations

### 2.1 Provider 429s as fleet-wide backpressure

**Research.** HiveMind (arXiv 2604.17111, 2026) is a proxy that schedules concurrent agents
sharing a rate-limited endpoint. Its motivating incident: 3 of 11 parallel agents died from
resets and 502s although aggregate capacity sufficed. It reads provider rate-limit headers after
each response and pauses all agents when remaining capacity falls under a threshold (default 10%,
≤2 requests left). It adds AIMD backpressure with circuit breaking and transparent retry. A
third-party review reports 72–100% fewer failures across 7 scenarios with 5–50 agents, with
transparent retry the most important piece. Treat these as unreplicated: a review notes the
contention setup is under-specified. Netflix's concurrency-limits (2018) is the classic
engineering source for AIMD/gradient limits against a shared backend. Anthropic returns
`retry-after` plus `anthropic-ratelimit-{requests,input-tokens,output-tokens}-{limit,remaining,reset}`
**(vendor)**.

**Stepwise today.** In `internal/engine/engine.go` `callLLM`, a 429 is `provider.Retryable`, so it
calls `br.Failure()` and `retryLater`. Two consequences:
- Five consecutive 429s open that worker's **circuit breaker** for 30 s for the whole provider,
  across all models. The breaker exists for outages; quota exhaustion is not one.
- `retryLater` increments `run.Attempts`. With `MaxAttempts` 6, a run that keeps meeting 429s
  **fails as `retries_exhausted`**, then starts triage. Sustained quota pressure turns into failed
  runs. Meanwhile `admit` already requeues rate-limited runs *without* spending attempts. 429s
  should take that same path.
- Each worker finds the 429 on its own. Without `STEPWISE_RATE_LIMITS` set, nothing shares the signal.

**Sketch (minimal).**
1. In `callLLM`, branch on `UpstreamError.Status == 429` (and `StreamError.Type == "rate_limit_error"`)
   before the generic path: `br.Abort()` (not `Failure`), then
   `x.e.limiter.Pause(ctx, model, d)` with `d = retryAfter(err)` (fallback: `breaker.Backoff`). Then
   yield like `admit` does: queued, `AvailableAt = now+d`, attempts unchanged.
2. Add one method to `ratelimit.Limiter`: `Pause(model, d)` sets both buckets' level to
   `-(rate × d)`, so refill reaches zero at `now+d`. This reuses the debt that `Settle` already
   allows. Local: 5 lines. Redis: one more small Lua script beside the reserve/settle ones. Then
   every worker's next `Reserve` returns a wait without calling the provider. For a model with no
   rule, `Pause` installs a temporary rule.
3. Later, optionally: put `*-remaining` headers on `provider.Response` and clamp the bucket in
   `Settle`. That is HiveMind's proactive layer. It also helps learn limits when no rule is configured.

**Measure.** The chaos harness injects only 503s (`-llm-fault-rate`). Add a 429-with-`Retry-After`
mode to its mock and compare runs failed, `stepwise_rate_limited` / retries metrics, and model
calls per completed run. Invariants (0 lost, 0 double refunds) must hold.

### 2.2 Cache-aware rate-limit accounting

**Research.** Anthropic's docs **(vendor)**: for most current models only `input_tokens +
cache_creation_input_tokens` count toward ITPM, `cache_read_input_tokens` do not, and OTPM counts
actual output; "`max_tokens` does not factor into OTPM". Their example: 2M ITPM at an 80% hit
rate processes 10M input tokens/min. "Don't Break the Cache" (Lumer et al., arXiv 2601.06007, 2026)
measures prompt caching on multi-turn agent tasks across OpenAI, Anthropic and Google: **41–80%
lower cost and 13–31% faster TTFT**.

**Stepwise today.** `ratelimit.EstimateTokens` reserves `promptChars/4 + maxOutput`, and `callLLM`
settles `resp.InputTokens + resp.OutputTokens`. `anthropicUsage.prompt()` includes cache reads. So
Stepwise's TPM bucket charges a 30k-token cached history every turn, while Anthropic charges
only the new ~1–3k. On long runs the local limiter throttles several times harder than the
provider does. Budgets should keep counting all tokens; only the limiter is over-strict.

**Sketch.** Add `RateTokens int` to `provider.Response`. The Anthropic adapter sets it to
`input + cache_creation + output`; OpenAI leaves it at input + output. OpenAI's docs found here
don't say that cached tokens are exempt from TPM, so don't assume it. In `callLLM`, settle
`resp.RateTokens - estimate`. For the estimate, subtract the previous completed LLM step's
`InputTokens + OutputTokens` (a likely-cached prefix) from `promptChars/4` when that step finished
under 5 minutes ago. Use `x.steps`, which is already loaded. Stays inside `engine.go`; no new package.

**Measure.** The `cached_input` token metric already exists. Run a long multi-turn agent against
Anthropic with a TPM rule. Compare `rate_limited` events and runs/minute before and after, and
check the provider never returns 429.

### 2.3 Cache-aligned observation masking (refines the planned item)

**Research.**
- The Complexity Trap (Lindenbauer et al., arXiv 2508.21433, NeurIPS 2025): on SWE-bench Verified
  across five model setups, replacing old observations with a placeholder **halves cost versus
  the raw agent and matches or beats LLM summarization**. Example: Qwen3-Coder 480B went from
  53.8% to 54.8% solved, 52% cheaper. A hybrid saves a further 7–11%.
- ACON (ICML 2026): learned compression cuts peak tokens 26–54% with task success largely kept.
- Anthropic's context editing **(vendor)**: −84% tokens in a 100-turn search eval, +29% on an
  internal benchmark. Its docs warn that clearing **invalidates the prompt cache**. They add
  `clear_at_least` so a cache break is worth it, plus `keep` (default 3 recent tool uses) and
  `exclude_tools`.
- Manus's engineering blog **(vendor)**: cache hit rate is "the single most important metric";
  keep context append-only and serialization deterministic.

**Stepwise mapping.** `conversation()` in `internal/engine/decide.go` rebuilds history from saved
steps on each claim. It is deterministic and replay-safe, which is the right place for masking.
Tool specs are resolved once at run creation (`tools.Resolve`) and stored with the run, so the
`tools → system` prefix is already stable. The danger is a mask that moves every turn: each move
rewrites the prefix and turns cheap cache reads (0.1x) into writes (1.25x).

**Sketch.** Only when the prompt estimate passes a threshold (e.g. 40k tokens), replace the
`Content` of completed tool steps older than the last `keep` (3) with
`"[result removed to save context; call the tool again if needed]"`. Move the boundary only in
chunks: mask steps with `seq <= floor((lastSeq - keep)/chunk)*chunk`, chunk ≈ 10 tool results.
The prefix then breaks once per chunk, not every turn (the `clear_at_least` idea). Keep error
results and `require_approval` tools unmasked, since they are the audit-relevant ones. Use a
placeholder, not an LLM summary: per the research it is cheaper and as accurate, and it needs no
extra model call. ~30 lines in `conversation()`, zero schema changes. Leave Anthropic's
server-side `context_management` alone: it is Anthropic-only and would hide from the audit trail
what the model actually saw.

**Measure.** Use a long mock agent (30+ tool turns, large results). Compare total input tokens,
`cached_input` share and cost per run with masking off, masking every turn, and chunked masking.
Run success must not drop. The chaos test checks replay stays deterministic.

### 2.4 Least-attained-service for rate-limited requeues

**Research.** Autellix (Luo et al., arXiv 2502.13965, 2025) treats a whole agent program as the
scheduling unit. PLAS ranks each call by the cumulative service its program has already
received, so short programs aren't stuck behind long ones. The paper reports 4–15x program
throughput over vLLM at equal latency. **That number is GPU-side and does not transfer.** The
policy does: when capacity is scarce, give it to the runs that have used least. AgentRM (arXiv
2603.13110, 2026) applies a multi-level feedback queue with rate-limit-aware admission to agent
fleets.

**Stepwise mapping.** `ClaimRuns` (`internal/store/postgres`) orders `ready` by `available_at`
(FIFO). Under a binding limit, `admit` requeues every blocked run at `now + wait`. All of them
compute nearly the same `wait` from the same bucket, so they return together: one wins, the rest
requeue. That costs a claim transaction and `ListSteps` each, and the order ignores how much
each run has consumed.

**Sketch.** One line in `admit`:
`wait += time.Duration(float64(wait) * (frac + rand.Float64()*0.2))`, where
`frac = TokensUsed / BudgetTokens`. Light runs come back first, heavy runs at most ~2x later
(bounded, so nothing starves), and the jitter breaks the herd. The claim query and its partial
indexes stay as they are. Per-tenant fair scheduling stays in "Not now".

**Measure.** Run chaos with a tight TPM rule and mixed run lengths (needs two small harness flags).
Report p50/p95 run completion time, claims per completed run, and the pgbench-measured claim rate.
Expect the same throughput, a lower median and fewer wasted claims.

### 2.5 Discount tier for non-urgent runs

**Evidence (vendor).** Anthropic Message Batches: all usage at **50%** of standard price; most
batches finish within an hour, 24 h hard limit; stacks with prompt caching (use the 1-hour TTL).
OpenAI flex processing: Batch API rates on the normal endpoint, slower responses and occasional
429 "resource unavailable"; model support is limited and changes.

**Stepwise mapping.** The target agents (back-office, extraction, reconciliation, the planned bulk
runs and schedules) rarely need second-level latency, and "waiting is free" in Stepwise.
- **Step 1 (~15 lines):** `"service_tier": "flex"` on the agent spec, passed through by the OpenAI
  adapter with a longer `LLMTimeout`. With item 2.1, a flex 429 is a requeue, not a failure.
- **Step 2 (~150 lines, only if bulk runs land):** Anthropic batch. `callLLM` submits a one-request
  batch, records the batch ID on the running LLM step, and yields with
  `AvailableAt = now + poll`. A later claim polls and completes the step. Bonus: a crash during the
  call no longer re-pays for it (today ~13% of model calls under chaos), because the batch ID is
  durable.

**Measure.** Cost per completed run from token metrics × price; wall-time distribution. The chaos
invariants must hold with the async step state.

### Smaller items worth doing alongside

- **OpenAI `prompt_cache_key` = run ID** (one line in `openai.go`). OpenAI routes by this key to
  the same cache machine **(vendor)**. It is required for the more reliable matching on GPT-5.6+,
  and they advise keeping each key under ~15 RPM, which one sequential run always is.
- **1-hour cache TTL for agents with sub-agent tools.** Children usually take longer than the
  5-minute default TTL. A 1h write costs 2x versus 1.25x **(vendor)**, so use it only when gaps
  are expected.
- **Repeat-call detection in `decide()`.** MAST (Cemri et al., NeurIPS 2025 D&B, 1,600+ traces,
  14 failure modes) lists step repetition as a top specification failure. If the same tool name
  and input returned the same result 3 times in a row, fail with a loop reason. Triage already
  has an `agent_loop` category. This is pure and replay-safe, ~15 lines. It only saves tokens up
  to `max_turns` (default 20), hence low priority.

## 3. Researched, not recommended

- **Speculative actions** (Ye et al., ICLR 2026): up to 55% next-action accuracy → at most 20%
  lower latency, paid for in extra calls. Speculating side-effecting tools conflicts with
  exactly-once.
- **Sherlock** (2025): speculative execution plus selective verifiers, −48.7% time. Built for
  fixed workflow DAGs; Stepwise agents choose steps at runtime.
- **LLMCompiler** (ICML 2024): up to 3.7x faster and 6.7x cheaper than ReAct, but only 1.35x over
  native parallel function calling (v1 abstract). The planned parallel tool execution captures most of that.
- **AsyncLM** (2024): 1.6–5.4x lower latency, but needs fine-tuned models and control of decoding.
- **Autellix PLAS/ATLAS and Continuum KV-cache TTL pinning** (ICLR 2026 submission): GPU-server
  internals. Only the scheduling policy carries over (item 2.4); for KV-cache pinning, the closest
  client-side knob is the provider TTL.
- **Learned routers and cascades** (RouteLLM, ICLR 2025: >2x cheaper on chat benchmarks;
  2026 agent cascades: ~46% savings): the cascade work reports that cheap draft models fail
  tool-call schemas. For refund agents, a wrong cheap answer costs more than the saving. Users
  already choose a model per agent and per sub-agent.
- **LLM summarization for compaction**: masking matches it at lower cost (item 2.3) and adds no
  extra model call to replay.
- **Latency-gradient concurrency limits** (Netflix Gradient/Vegas): LLM latency mostly tracks
  output length, so RTT is a noisy overload signal. 429s and headers are explicit.
- **Durable-execution papers** (LogAct 2026, "Resume Means Resume" 2026): they confirm the existing
  design (a log of actions, plus idempotency for external effects) and add nothing new to build.
- **Trajectory-replay evals** (Chronicle, 2026: cut-point replay from recorded runs; τ-bench pass^k,
  ICLR 2025: pass^8 < 25% in retail): already planned as "evals by replaying saved runs". Chronicle
  supports building it as "replay saved steps up to seq N, then the new model/prompt" with pass^k
  over repeats. It is a feature, not an optimization.

## 4. Sources

- [HiveMind: OS-Inspired Scheduling for Concurrent LLM Agent Workloads](https://arxiv.org/abs/2604.17111) — 2026, arXiv preprint
- [AgentRM: An OS-Inspired Resource Manager for LLM Agent Systems](https://arxiv.org/abs/2603.13110) — 2026, arXiv preprint
- [Netflix: Performance Under Load (concurrency-limits)](https://netflixtechblog.medium.com/performance-under-load-3e6fa9a60581) — 2018, Netflix Tech Blog
- [Anthropic: Rate limits](https://platform.claude.com/docs/en/api/rate-limits) — 2026, vendor docs
- [Anthropic: Prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching) — 2026, vendor docs
- [Anthropic: Context editing](https://platform.claude.com/docs/en/build-with-claude/context-editing) — 2025–26, vendor docs
- [Anthropic: Managing context on the Claude Developer Platform](https://claude.com/blog/context-management) — Sep 2025, vendor blog
- [Anthropic: Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents) — Sep 2025, vendor engineering blog
- [Anthropic: Batch processing](https://platform.claude.com/docs/en/build-with-claude/batch-processing) — vendor docs
- [OpenAI: Prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching) — 2026, vendor docs
- [OpenAI: Flex processing](https://developers.openai.com/api/docs/guides/flex-processing) — 2025–26, vendor docs
- [Manus: Context Engineering for AI Agents](https://manus.im/blog/Context-Engineering-for-AI-Agents-Lessons-from-Building-Manus) — Jul 2025, vendor engineering blog
- [Don't Break the Cache: Prompt Caching for Long-Horizon Agentic Tasks](https://arxiv.org/abs/2601.06007) — 2026, arXiv preprint
- [The Complexity Trap: Observation Masking vs LLM Summarization](https://neurips.cc/virtual/2025/131688) — 2025, NeurIPS ([arXiv 2508.21433](https://arxiv.org/abs/2508.21433))
- [ACON: Optimizing Context Compression for Long-horizon LLM Agents](https://proceedings.mlr.press/v306/kang26b.html) — 2026, ICML
- [Autellix: An Efficient Serving Engine for LLM Agents as General Programs](https://arxiv.org/abs/2502.13965) — 2025, arXiv preprint
- [Continuum: Multi-Turn Agent Scheduling with KV Cache Time-to-Live](https://arxiv.org/abs/2511.02230) — 2025, arXiv (ICLR 2026 submission)
- [Why Do Multi-Agent LLM Systems Fail? (MAST)](https://arxiv.org/abs/2503.13657) — 2025, NeurIPS Datasets & Benchmarks
- [Speculative Actions: A Lossless Framework for Faster AI Agents](https://iclr.cc/virtual/2026/poster/10009726) — 2026, ICLR
- [Sherlock: Reliable and Efficient Agentic Workflow Execution](https://arxiv.org/abs/2511.00330) — 2025, arXiv preprint
- [LLMCompiler: An LLM Compiler for Parallel Function Calling](https://icml.cc/virtual/2024/poster/32829) — 2024, ICML
- [Asynchronous LLM Function Calling (AsyncLM)](https://arxiv.org/abs/2412.07017) — 2024, arXiv preprint
- [RouteLLM: Learning to Route LLMs from Preference Data](https://proceedings.iclr.cc/paper_files/paper/2025/hash/5503a7c69d48a2f86fc00b3dc09de686-Abstract-Conference.html) — 2025, ICLR
- [RLM-Cascade: Response-Level Speculative Decoding for Cost-Efficient LLM API Serving](https://arxiv.org/abs/2606.22840) — 2026, arXiv preprint
- [LogAct: Enabling Agentic Reliability via Shared Logs](https://arxiv.org/abs/2604.07988) — 2026, arXiv preprint
- [Chronicle: Cut-Point Replay for Regression Testing of LLM Agents](https://arxiv.org/abs/2609.20625) — 2026, arXiv preprint
- [τ-bench: Tool-Agent-User Interaction in Real-World Domains](https://proceedings.iclr.cc/paper_files/paper/2025/hash/1b126cc38b8638e07bef37e7b2bb72bf-Abstract-Conference.html) — 2025, ICLR
