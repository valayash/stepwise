# Multi-agent systems: research and fit for Stepwise

Status: research for the future. Nothing here is scheduled yet; see "Future: multi-agent" in
[PLAN.md](../PLAN.md). Written October 2026. Most industry sources are vendor or practitioner
blogs, so their numbers are indicative, not benchmarks.

## Short answer

- **Can Stepwise support multi-agent systems?** Partly today, and the rest fits the design.
  The most common production pattern, an orchestrator that delegates to specialist sub-agents,
  already works: sub-agents are tools, each runs as its own durable run, several run in
  parallel, tokens roll up into the parent's budget, and cancelling cascades.
- **Should we build more now?** No. Industry guidance is to start with one well-prompted agent
  and add agents only when a single one demonstrably fails. Multi-agent setups cost an estimated
  2–5x more tokens, and most of their failures happen at the hand-offs between agents.
- **Our advantage when we do:** the hardest multi-agent failures are lost state between agents,
  retries that repeat an action (a payment made twice), and runaway loops. Stepwise already
  solves those for one agent (saved steps, exactly-once tools, budgets and limits). Extending
  them across agents is the pitch.

## The main patterns

| Pattern | How it works | Typical use | Stepwise today |
| --- | --- | --- | --- |
| **Orchestrator–workers** (supervisor, "agents as tools") | One agent plans, calls specialists as tools, merges their answers. The orchestrator keeps control of the final reply. | The production default: research then act, triage then resolve | ✅ `"type": "agent"` tools; durable child runs; parallel fan-out in one turn; up to 3 levels deep |
| **Fan-out / fan-in** | Independent tasks run in parallel, results are combined | Check 20 invoices, research 5 vendors | ✅ via the orchestrator (one call per task). No cap on parallel children yet |
| **Sequential pipeline** | Agent A's output is agent B's input, in a fixed order | Extract → validate → file | ⚠️ Possible by chaining runs from the caller's code, or one orchestrator calling sub-agents in order; no built-in pipeline |
| **Hand-off** | Control moves to another agent, which owns the next turns and the final reply (no manager in the loop) | Front-desk agent routes to billing or refunds specialist | ❌ Not supported |
| **Hierarchical** | Supervisors of supervisors | Large org-like workflows | ✅ Nesting up to 3 levels |
| **Debate / critic** | One agent drafts, another reviews or argues | Quality checks before a risky action | ⚠️ Expressible as orchestrator + critic sub-agent; nothing special needed |
| **Swarm / blackboard** | Peers share a common state or message each other freely, without a fixed leader | Open-ended exploration | ❌ No shared state or agent-to-agent messages. Sources report decentralized designs fail far more often; low priority |
| **Across systems** (A2A) | Agents from different vendors discover and call each other over a standard protocol | Our agent asks a partner's agent to do part of a job | ❌ Not supported. See A2A below |

## What the sources say

- **Orchestrator–workers is the default.** It's natively supported by the OpenAI Agents SDK,
  LangGraph and the Claude Agent SDK, and suits workflows with clear task boundaries. Hand-offs
  fit routing that can't be predicted in advance.
- **OpenAI frames the choice as "who owns the final reply":** agents-as-tools keeps the manager
  in control; a hand-off gives the specialist the rest of the conversation.
- **Cost:** an estimated 2–5x more tokens than one agent; using a strong model for the
  orchestrator and cheaper ones for workers is the usual saving.
- **Failure modes** (the MAST taxonomy from NeurIPS 2025 is the main study: 14 failure modes
  across 1,600+ traces):
  - Breaks happen at the seams: hand-offs, coordination, and agents acting on stale shared state.
  - Retries after timeouts repeat side effects ("a payment processed twice").
  - Agents with conflicting instructions pass work back and forth forever.
  - Errors compound: ten steps at 99% each give about 90% end to end.
- **Guidance:** ship one agent first; when going multi-agent, use a strict hierarchy, narrow
  specialists and human review at milestones.
- **Claude Managed Agents** (Anthropic's hosted product) now offers multi-agent "workflows"
  (a lead agent plus a roster of sub-agents, often a cheaper model for reading-heavy work).
  This confirms the orchestrator pattern as the one to invest in.

## A2A (Agent2Agent protocol)

- An open protocol for agents from different vendors to work together, now under the Linux
  Foundation alongside MCP. The Foundation reported 150+ supporting organizations and support in
  Google, Microsoft and AWS platforms at its one-year mark (adoption depth is debated).
- Built on HTTP, JSON-RPC and SSE. Agents publish an **Agent Card** (JSON describing what they
  do and how to reach them); work is exchanged as **tasks** with status updates and artifacts.
  v1.0 is reported to add signed Agent Cards.
- **Fit for Stepwise is unusually good:** an A2A task maps directly onto a Stepwise run
  (create, status, stream updates, result), and our `/events` stream is already SSE.
  - **As a server:** expose agents as A2A endpoints with Agent Cards, so outside agents can
    hand durable, approval-gated work to Stepwise.
  - **As a client:** a new tool type `"a2a"`, next to `http` and `mcp`, that calls a remote agent
    with the same idempotency key and retry rules.

## What multi-agent support would need (when users ask)

In rough order of value for the code it costs:

1. **Cap parallel sub-agents per run** (e.g. `max_parallel_agents`), so one orchestrator turn
   can't start 100 children. Small.
2. **Structured hand-back.** A sub-agent with `response_schema` already returns JSON; pass it to
   the parent as structured data rather than text. Small.
3. **Cheaper workers by default.** Document and test "strong orchestrator, cheap workers";
   per-sub-agent models already work.
4. **One trace across agents.** Today each run has its own trace (trace ID = run ID). Link child
   traces to the parent span so a whole multi-agent job reads as one tree. Small.
5. **Hand-offs.** A `"type": "handoff"` tool: the run continues under the target agent's spec
   for the following turns, recorded as a step so replay stays deterministic. Medium.
6. **A2A client tool, then A2A server.** Medium each.
7. **Shared state / blackboard, peer messaging.** Large, and the evidence says it fails most
   often. Only if real users need it.

## Sources

- [Multi-agent system architecture: a comparison guide (Openlayer)](https://www.openlayer.com/blog/multi-agent-system-architecture-guide)
- [6 Multi-Agent Orchestration Patterns for Production (Beam AI)](https://beam.ai/agentic-insights/multi-agent-orchestration-patterns-production)
- [Multi-Agent Orchestration: 5 Patterns That Work in 2026 (Digital Applied)](https://www.digitalapplied.com/blog/multi-agent-orchestration-5-patterns-that-work)
- [Benchmarking Multi-Agent LLM Architectures for Financial Document Processing (arXiv)](https://arxiv.org/pdf/2603.22651)
- [Multi-agent orchestration in 2026: supervisor, swarm, handoffs and A2A (Reactify)](https://www.reactify-solutions.com/articles/multi-agent-orchestration-2026)
- [Orchestration and handoffs (OpenAI docs)](https://developers.openai.com/api/docs/guides/agents/orchestration)
- [Agents as Tools vs Handoffs (Towards AI)](https://pub.towardsai.net/agents-as-tools-vs-handoffs-understanding-the-two-patterns-behind-modern-ai-systems-6a3b7f55b157)
- [Multi-Agent Handoffs Are Where Systems Actually Break](https://urandom.io/blog/2026-03-30-multi-agent-handoffs-are-where-systems-break)
- [A2A protocol surpasses 150 organizations (Linux Foundation)](https://linuxfoundation.org/press/a2a-protocol-surpasses-150-organizations-lands-in-major-cloud-platforms-and-sees-enterprise-production-use-in-first-year)
- [Google A2A Protocol in 2026: Adoption, Hype, and Reality](https://www.glukhov.org/ai-systems/comparisons/a2a-protocol-2026-adoption/)
- [A2A Joins MCP at the Linux Foundation (FlowHunt)](https://www.flowhunt.io/blog/agentic-ai-foundation-a2a-mcp-standards/)
- [Getting Up to Speed on Multi-Agent Systems, Part 4: Why It Breaks](https://christophermeiklejohn.com/ai/agents/mas-series/2026/04/27/mas-series-04-wave-two.html)
- [Multi-Agent System Reliability: Failure Patterns (Maxim)](https://www.getmaxim.ai/articles/multi-agent-system-reliability-failure-patterns-root-causes-and-production-validation-strategies/)
- [Ways multi-agent AI fails in production (TechAhead)](https://www.techaheadcorp.com/blog/ways-multi-agent-ai-fails-in-production/)
- [Multi-agent systems in production: what works (Pixelfield)](https://pixelfield.co.uk/blog/multi-agent-systems-production-what-works/)
