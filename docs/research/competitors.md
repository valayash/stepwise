# Competitors and what this product category is called

Written October 2026. Most comparisons and reviews online are vendor-written or SEO-driven, and
independent user reviews of these products are scarce; where a claim is a vendor's own or comes
from one secondary source, it says so. Check pricing and limits on each vendor's site.

## 1. What this kind of product is called

"Agent Runtime Environment" (the Reddit post's term) isn't a standard name. These are the terms
the industry actually uses, which work best as search terms:

| Search term | What you'll find |
| --- | --- |
| **"durable execution" + "AI agents"** | Closest to Stepwise: Temporal, Restate, Inngest, DBOS, Trigger.dev, Hatchet |
| **"agent runtime"** | Managed places to run agents: AWS Bedrock AgentCore Runtime, Microsoft Foundry hosted agents |
| **"agent platform" / "managed agents"** | Full hosted services: Claude Managed Agents, Google Vertex AI Agent Engine, Microsoft Foundry Agent Service |
| **"agent orchestration"** | Frameworks plus hosting: LangGraph and LangSmith Deployment, CrewAI, Mastra |
| **"agent infrastructure" / "agentic AI stack"** | Overviews that place all of the above in layers |
| **"stateful agents" / "agent memory"** | Memory-first platforms: Letta |

**Stepwise in one line:** a *self-hosted, durable agent runtime*: it runs agents defined as data
(model, instructions, tools) with durable execution, exactly-once tool calls and human approvals
built in.

## 2. Is Stepwise what the Reddit post describes?

Mostly yes, with a different emphasis. The post lists 14 components; here is where Stepwise stands:

| Component | Stepwise | Notes |
| --- | --- | --- |
| Execution engine | ✅ | Workers, `decide()` loop, leases and fencing |
| State and workflow management | ✅ **strongest area** | Every step saved; runs survive crashes; waits for days |
| Tool and API invocation | ✅ | HTTP and MCP tools, auth headers, host allowlist, exactly-once keys |
| Human-in-the-loop | ✅ | Approvals, rejections shown to the model, expiry |
| Analytics and monitoring | ✅ | OpenTelemetry traces, Prometheus metrics, UI, usage API |
| Scalability and distribution | ✅ | Many worker processes on Postgres; measured under SIGKILL chaos |
| Security and sandboxing | ⚠️ | Fencing, tool host allowlist, secrets never stored, spend and tool limits. **No code sandbox**: Stepwise doesn't run agent-written code; agents act only through tools |
| Memory: short-term | ✅ | A run's saved steps are its working memory |
| Memory: long-term (across runs) | ❌ | Not built in; plug in a memory or search tool over MCP |
| Session management (multi-turn) | ❌ planned | Conversations are Phase 3. Hierarchy (parent and child runs) exists |
| Inter-agent communication | ⚠️ | Sub-agents as tools, in parallel. No hand-offs or A2A yet ([multi-agent research](multi-agent.md)) |
| Knowledge management (grounding) | ❌ | Treated as a tool (RAG over MCP), not part of the runtime |
| Simulation and testing | ⚠️ | Unusually strong for the *runtime* (chaos test with crash and fault injection); nothing yet for testing *agent behavior* (evals) |
| Auto-tuning / reinforcement learning | ❌ | Research territory; no runtime in this report offers it as a mainstream feature. Not recommended for now |

## 3. The competitors

### Durable execution engines (closest to Stepwise)

| | Temporal | Restate | Inngest | DBOS |
| --- | --- | --- | --- | --- |
| What it is | General durable workflow platform, the established leader | Lightweight durable runtime for services, workflows and agents | Event-driven durable functions, strong in TypeScript | Durable workflows as a library on Postgres |
| How you build agents | Code, with determinism rules; integrations with the OpenAI Agents SDK and Vercel AI SDK; new "Agent Harness" with tool-call approvals | Code; integrations with the OpenAI Agents SDK, Pydantic AI, Vercel AI SDK | Code; AgentKit for multi-agent | Code (decorators) |
| Self-host | Yes (open source); server is several services plus a database | Yes, **single binary** | Hosted first; self-host under BSL (not open source) | Yes (MIT), just Postgres |
| Human approvals, long waits | Yes (durable waits) | Yes ("minutes or months") | Yes (wait for event) | Yes |
| LLM-aware features (token limits, budgets, tool limits) | Build it yourself | Build it yourself | Some (step-level) | Build it yourself |
| Main criticism | Steep learning curve and heavier operations ("most teams will fail the learning curve": opinion piece) | Smaller, younger ecosystem ($20M Series A, Sept 2026) | Per-step pricing adds up for long agent runs; a reported 1,000-steps-per-function limit (third-party claim) | Doesn't address the probabilistic steps of agents natively (claim from a competitor's FAQ) |

Also in this group: **Trigger.dev** (open source, TypeScript, praised self-hosting) and
**Hatchet** (MIT, Postgres-based, fine-grained concurrency; scale ceiling vs Temporal).

### Cloud agent platforms (managed)

| | AWS Bedrock AgentCore | Google Vertex AI Agent Engine | Microsoft Foundry Agent Service | Claude Managed Agents |
| --- | --- | --- | --- | --- |
| What it is | Modular services to host agents built with any framework | Hosting for agents (especially Google's ADK) | Microsoft's agent hosting and services | Anthropic runs the agent loop *and* a sandbox |
| Runtime | Serverless, isolated per session, auto-scaling | Managed | **Hosted agents** (preview April 2026): isolated sandbox per session, scale to zero | Per-session container (bash, files, code) |
| Sessions / multi-turn | Yes | Yes (Sessions API) | Yes (threads) | Yes (sessions, event stream) |
| Memory | Short- and long-term | **Memory Bank** (long-term, extracted by Gemini) | Procedural, user and session memory | Memory stores |
| Tools and identity | **Gateway** (MCP), **Identity** (auth to tools), VPC access | Via ADK | Toolboxes (preview) | MCP, skills, credential **vaults** |
| Human approvals | Build with Policy | Build yourself | Build yourself | Permission policies (always allow / ask / auto) |
| Evaluation | **Evaluations** (GA March 2026) | — | — | **Outcomes** (a grader iterates against a rubric) |
| Other | Policy (GA), Code Interpreter, Browser, OpenTelemetry | — | Routines (preview), Voice Live | Scheduled deployments, multi-agent workflows |
| Lock-in | AWS | Google Cloud | Azure | Claude models only |
| Main criticism | Mostly announcements, few independent reviews; closed and AWS-only | Tied to Gemini and ADK for the smoothest path | Many features in preview | Claude only; your data runs on Anthropic's side |

### Frameworks with hosting, and memory platforms

| | LangGraph + LangSmith Deployment | Letta |
| --- | --- | --- |
| What it is | Graph-based agent framework; hosting sold as metered "Deployment" inside LangSmith | Stateful agents with long-term memory |
| Strengths | Checkpoints, interrupts for human input, large ecosystem, strong tracing and evals (LangSmith) | Tiered memory (core, archival, recall), agents edit their own memory, open source (Apache 2.0), model-agnostic |
| Main criticism | Forum reports of deployments breaking after upgrades, version mismatches in self-hosted setups, Redis and Postgres connection issues; pricing restructured mid-2026 | Memory-focused; not a durable-execution or approvals system |
| Pricing | Free developer tier; Plus about $39/seat/month (third-party); metered compute | Free tier; Pro $20/month; enterprise custom |

## 4. What competitors offer that Stepwise doesn't (yet)

| Feature | Who has it | Stepwise plan |
| --- | --- | --- |
| **Multi-turn sessions** | All four cloud platforms, LangGraph, Letta | Phase 3, first item (Conversations) |
| **Long-term memory across runs** | AgentCore, Vertex Memory Bank, Foundry, Claude Managed Agents, Letta | Not planned; candidate for "Later" (or memory as an MCP tool) |
| **Scheduled agents** | Claude Managed Agents, Foundry Routines | Phase 3 (schedules) |
| **Evaluations** | AgentCore, LangSmith, Claude Managed Agents (outcomes) | "Later": replay saved runs against a new prompt or model |
| **Code / browser sandbox** | AgentCore, Foundry hosted agents, Claude Managed Agents | Not our target (no coding agents) |
| **SDKs in Python and TypeScript** | All | Not planned yet; agents are JSON over HTTP |
| **Credential vaults / identity for tools** | AgentCore Identity, Claude Managed Agents vaults | Partly: `${STEPWISE_SECRET_*}`; per-org vault later |

## 5. Where Stepwise is different or ahead

1. **Agent features built into the runtime.** Durable engines (Temporal, Restate, Inngest,
   DBOS) give you durability primitives, and you write budgets, token-aware rate limits,
   exactly-once tool keys, approvals with expiry and per-tool limits yourself. Stepwise has
   them as configuration.
2. **Agents are data, not code.** No determinism rules, no SDK, and no redeploy to change an
   agent. That's simpler for a team that just wants agents to run, at the cost of flexibility
   for complex custom logic.
3. **Self-hosted and model-agnostic.** The cloud platforms each tie you to one cloud (or, for
   Claude Managed Agents, one model family) and keep your data on their side. Stepwise is one
   binary plus Postgres, with any model, on the customer's servers.
4. **Proof.** A crash-and-fault chaos test with published results is unusual in this space.

Where we are weaker: no multi-turn sessions or memory yet, no SDKs, no evals, a tiny community,
and none of the big vendors' integrations.

## 6. How to try them yourself

The quickest hands-on paths, from each vendor's own docs:

- **Restate** is a single binary; its "AI agents" tour shows durable agents with approvals:
  [docs.restate.dev/tour/ai-agents](https://docs.restate.dev/tour/ai-agents)
- **Temporal** has a local dev server (`temporal server start-dev` with the Temporal CLI) and
  AI agent samples on its site.
- **Inngest** runs a local dev server through its CLI; see its quick start.
- **LangGraph** has a local dev server through the LangGraph CLI and a free LangSmith tier.
- **Letta** runs locally or on its free hosted tier.
- **Claude Managed Agents** needs only a Claude API key (public beta).
- **AWS AgentCore**, **Vertex AI Agent Engine** and **Foundry** need a cloud account; each has
  a quick-start in its docs.

A useful comparison exercise: build the same refund agent (look up order, check policy, refund
with approval) on two or three of them, then kill the process mid-run and see what happens.

## Sources

- [8 best agent orchestration tools in 2026 (Arize)](https://arize.com/resources/best-agent-orchestration-tools/)
- [Best AI agent platforms to run agents in production (Ability)](https://www.ability.ai/blog/best-ai-agent-platforms-to-run-agents-in-production)
- [Best AI Agent Orchestration Frameworks: 11 compared (Aissist)](https://aissist.io/insights/best-ai-agent-orchestration-frameworks)
- [9 best Temporal alternatives (ZenML)](https://www.zenml.io/blog/temporal-alternatives)
- [8 Inngest alternatives for durable AI agents (ZenML)](https://www.zenml.io/blog/inngest-alternatives)
- [Best AI Agent Runtime Platforms 2026 (Agentspan)](https://agentspan.ai/blogs/best-ai-agent-runtime-platforms-2026/)
- [Durable execution for AI agents: Temporal vs Restate vs Inngest vs DBOS vs Vercel Workflow vs LangGraph (Cadenya)](https://cadenya.com/other-content/durable-execution-for-ai-agents)
- [Durable Execution for AI Agents: the 2026 guide (FrenchyDigital)](https://frenchydigital.com/blog/ai-agent-durable-execution-2026)
- [Which durable execution engine for AI agents (Startupik)](https://startupik.com/durable-execution-engine-ai-agents/)
- [Temporal raised $300M, admits most teams will fail the learning curve (UC Strategies)](https://ucstrategies.com/news/temporal-raised-300m-at-5b-but-admits-most-teams-will-fail-the-learning-curve/)
- [Temporal takes on the chaos behind enterprise AI agents (SiliconANGLE)](https://siliconangle.com/2026/07/21/temporal-agentic-ai-durable-execution-awsmarketplaceseries/)
- [Temporal for durable agents (Rubinlake tech radar)](https://rubinlake.com/es/technology-radar/ai-and-data-engineering/temporal-durable-agents)
- [Restate: AI agents tour](https://docs.restate.dev/tour/ai-agents)
- [Restate: durable orchestration with the OpenAI SDK](https://restate.dev/blog/durable-orchestration-for-ai-agents-with-restate-and-openai-sdk)
- [Restate Series A analysis (Remio)](https://www.remio.ai/post/restate-series-a-pits-a-20m-bet-against-database-backed-durable-execution)
- [Hatchet vs Trigger.dev v3 vs Inngest (PkgPulse)](https://www.pkgpulse.com/guides/hatchet-vs-trigger-dev-v3-vs-inngest-durable-workflows-2026)
- [Hatchet review (AICoolies)](https://aicoolies.com/reviews/hatchet-review)
- [DBOS and Inngest compared (Diagrid, competitor FAQ)](https://www.diagrid.io/faq/alternatives-dbos-inngest/in-what-scenarios-is-dbos-a-more-suitable-choice-than-catalyst-or-inngest-for-durable-agent-execution)
- [AgentCore release notes (AWS)](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/release-notes.html)
- [Amazon Bedrock AgentCore explained for developers](https://medium.com/awsfullstack/amazon-bedrock-agentcore-explained-for-developers-cc7c78821583)
- [AgentCore preview: a solutions architect's first look (Rackspace)](https://www.rackspace.com/en-nl/blog/amazon-bedrock-agentcore-preview-agentic-ai-platform)
- [Vertex AI Agent Engine Sessions overview (Google)](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/sessions/overview?authuser=5)
- [Configure agent memory with Agent Engine Sessions and Memory Bank (OneUptime)](https://oneuptime.com/blog/post/2026-02-17-how-to-configure-agent-memory-with-vertex-ai-agent-engine-sessions-and-memory-bank/markdown)
- [AI agent memory systems comparison (Lyzr)](https://www.lyzr.ai/blog/ai-agent-memory-systems-comparison/)
- [Introducing hosted agents in Foundry Agent Service (Microsoft)](https://devblogs.microsoft.com/foundry/introducing-the-new-hosted-agents-in-foundry-agent-service-secure-scalable-compute-built-for-agents)
- [What's new in Microsoft Foundry, Build 2026 (Microsoft)](https://devblogs.microsoft.com/foundry/whats-new-in-microsoft-foundry-build-2026/)
- [Microsoft Foundry agents (InfoQ)](https://infoq.com/news/2026/06/microsoft-foundry-agents)
- [Threads, runs and messages (Microsoft Learn, classic)](https://learn.microsoft.com/en-my/Azure/foundry-classic/agents/concepts/threads-runs-messages)
- [LangGraph and LangSmith version compatibility in production (LangChain KB)](https://kb.langchain.com/articles/5102702588-resolving-langgraph-and-langsmith-version-compatibility-issues-in-production)
- [LangSmith Deployment forum (LangChain)](https://forum.langchain.com/c/help/langsmith-deployment/6?page=1)
- [LangGraph review (PromptQuorum)](https://www.promptquorum.com/power-local-llm/langgraph-review)
- [LangSmith pricing guide 2026 (MetaCTO)](https://www.metacto.com/blogs/the-true-cost-of-langsmith-a-comprehensive-pricing-integration-guide)
- [Letta research notes (Ry Walker)](https://rywalker.com/research/letta)
- [Letta (Enterprise DNA directory)](https://enterprisedna.co/directories/open-source/letta)
- [What are Claude Managed Agents (TrueFoundry)](https://www.truefoundry.com/blog/what-are-claude-managed-agents)
