## Chaos test: PASS

1000 refund-agent runs (7 steps each, one human approval) on 8 worker processes × 4 concurrent runs, 5% injected tool failures, 5% injected model failures, a worker killed every ~120ms with no graceful shutdown.

| Check | Result |
| --- | --- |
| Runs completed | **1000 / 1000** |
| Runs lost or stuck | **0** |
| Runs failed | 0 |
| Refunds issued | **1000** (one per run) |
| Orders refunded twice | **0** |
| Repeated refund calls absorbed by idempotency keys | 35 |
| Step histories with gaps or duplicates | **0** |
| Workers killed | 79 |
| Runs taken over from a dead or yielding worker (claims beyond the first) | 1701 |
| Steps executed in total | 7000 |
| Model calls re-executed after a crash or transient failure | 385 (9.6% of model steps) |
| Tool calls re-executed after a crash or transient failure | 312 |
| Injected tool faults | 154 |
| Wall time | 11.438s (612 steps/s) |

Re-executed model calls are the cost of a crash: a call whose answer was not saved yet runs again. Completed steps are never repeated, and side effects stay exactly-once because tools receive a stable idempotency key.
