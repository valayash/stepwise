# Agent catalog: what people build, and what Stepwise needs for it

Written October 2026 to decide which features to build next. Surveys and case studies are
mostly vendor-published, so percentages are directional. Coding agents are left out on purpose:
they need sandboxes and are not our target (see "Agents we run" in [PLAN.md](../PLAN.md)).

## What's deployed most

- **Data and document work** leads: data entry and extraction (47% of enterprises in Zapier's
  survey), document analysis and summarization (41%), report generation (36%).
- **Customer support** is the department most likely to run agents (49%), with **operations**
  close behind (47%). Support agents now take actions such as refunds and account updates, and
  escalate to humans.
- **IT helpdesk** is often the first agent an enterprise ships (ticket deflection of 45–80% is
  claimed).
- **Banking and insurance** lead production use. In India: HDFC Bank reports 5 agents in
  production and 14 in development; Kotak about 20–25; areas are loan origination, underwriting,
  onboarding/KYC, collections and internal reporting. RBI's draft guidance (June 2026) expects
  customer-facing AI to disclose itself and offer a human.

## The catalog

Legend for "how it starts": **API** = the caller's code starts a run; **Chat** = a person
converses with it; **Event** = another system's webhook; **Schedule** = runs on a timetable.

| # | Agent | How it starts | Tools and actions | Risky actions → approval | Inputs and outputs |
| --- | --- | --- | --- | --- | --- |
| 1 | **Support action bot** (refunds, order changes) | Chat | Orders, payments, CRM | Refunds above a threshold | Text in, text out; hand over to a human |
| 2 | **Ticket triage** (classify, route, draft reply) | Event (helpdesk) | Helpdesk API | None (drafts only) | Ticket text → JSON (category, priority, draft) |
| 3 | **IT helpdesk** (access, resets, provisioning) | Chat | Identity, ITSM | Access grants | Text |
| 4 | **HR assistant** (policies, onboarding tasks) | Chat | Knowledge search, HRIS | Changes to records | Text; answers from company documents |
| 5 | **Document extraction** (invoices, forms, contracts) | API, at volume | Usually none | None | **PDF/images** → JSON |
| 6 | **Accounts payable** (match invoice to PO, schedule payment) | Event or Schedule | ERP, payments | Payments | **PDF** → JSON + action |
| 7 | **Expense audit** | Event | Policy check, finance system | Exceptions | **Receipts (images)** → decision |
| 8 | **KYC / onboarding** | API | ID checks, bureau, sanctions lists | Final approval by a person | **ID documents** → decision + audit trail |
| 9 | **Credit / underwriting assistant** | API | Bureau, bank statements | Decision by a person | **PDFs** → JSON summary |
| 10 | **Collections and reminders** | Schedule, at volume | SMS/WhatsApp/email, payments | Settlement offers | Many small runs; per-customer limits |
| 11 | **Sales research / CRM update** | Event or Schedule | Web search, CRM, email | Sending emails | JSON + CRM writes |
| 12 | **Scheduled reports / analysis** | Schedule | SQL, BI, documents | None | Report out (text, file) |
| 13 | **Incident triage and remediation** | Event (alerts) | Logs, metrics, runbooks, deploy tools | Any change to production | Long-running; waits on humans |
| 14 | **Reconciliation** (finance) | Schedule, at volume | Ledgers, bank data | Write-offs | Many runs; exceptions to a human |
| 15 | **Research and decide** (vendor, risk, due diligence) | API | Web, internal data | Final decision | Fan-out to sub-agents → JSON |

## Capabilities they need vs. what Stepwise has

| Capability | Agents needing it | Stepwise today |
| --- | --- | --- |
| Tools over HTTP/MCP with auth | All | ✅ |
| Exactly-once actions, crash safety | 1, 3, 6, 8, 10, 13, 14 | ✅ |
| Approvals that can wait, with expiry | 1, 3, 6–11, 13, 14 | ✅ |
| Budgets, per-tool limits, spend limit | All | ✅ |
| JSON results (`response_schema`) | 2, 5–9, 11, 15 | ✅ |
| Sub-agents in parallel | 15, 13 | ✅ |
| Webhooks out, audit trail, retention | All | ✅ |
| **Conversations (multi-turn)** | 1, 3, 4 | ❌ planned |
| **Hand over to a human** (and back) | 1, 3, 4 | ❌ needs conversations |
| **Documents and images as input** | 5–9 | ❌ input is text only |
| **Schedules** (cron) | 6, 10, 11, 12, 14 | ❌ |
| **Saved agents** (+ `{{variables}}`) | Needed by schedules, events and chat | ❌ moved to "Not now"; schedules and event triggers depend on it |
| **Event triggers** (inbound webhook → run) | 2, 6, 7, 11, 13 | ⚠️ callers can `POST /v1/runs`; a mapped inbound webhook needs saved agents |
| **Bulk runs** (many runs per request) and concurrency caps | 5, 10, 14 | ⚠️ one run per request; no per-agent cap |
| Live token streaming | 1, 3, 4 (chat feel) | ❌ in "Not now" |
| Knowledge search (RAG) | 4, 1 | ➖ a tool, not runtime: plug in any search/vector DB over MCP |
| PII redaction, data controls | 1, 5, 8, 9 (BFSI) | ❌ later |
| Evals: replay saved runs against a new prompt or model | All, before changes | ❌ later; saved steps make this a natural differentiator |
| Voice | 10 (calls) | ➖ out of scope: voice platforms can call Stepwise as the action layer |

## What this means for the roadmap

Ranked by how many catalog agents each unlocks, and by dependency:

1. **Conversations**: unlocks the chat agents (1, 3, 4), the most-deployed category.
2. **Documents and images as input**: unlocks the document agents (5–9), the most-deployed
   *kind of work*, and core to BFSI.
3. **Saved agents with variables**: required by schedules and event triggers, and makes chat
   simpler. It was moved to "Not now" as a convenience; this analysis shows it's a dependency.
4. **Schedules**: unlocks 6, 10, 11, 12, 14.
5. **Hand over to a human**: completes the support and IT bots (and the RBI expectation of
   human access).
6. **Bulk runs and per-agent concurrency caps**: volume work (5, 10, 14).
7. Later: event-trigger mapping, live streaming, evals by replay, PII controls.

## Sources

- [State of agentic AI adoption survey 2026 (Zapier)](https://zapier.com/blog/ai-agents-survey/)
- [The 2026 State of AI Agents report, part II (Claude)](https://claude.com/resources/guides/the-2026-state-of-ai-agents-report/research-insights-part-ii-going-deeper)
- [The State of AI in the Enterprise 2026 (Deloitte)](https://www.deloitte.com/us/en/what-we-do/capabilities/applied-artificial-intelligence/content/state-of-ai-in-the-enterprise.html)
- [150+ AI Agent Statistics 2026 (Master of Code)](https://masterofcode.com/blog/ai-agent-statistics)
- [12 Enterprise AI Agents Use Cases (HyScaler)](https://hyscaler.com/insights/enterprise-ai-agents-use-cases/)
- [10 AI Agent Use Cases (Sema4.ai)](https://sema4.ai/blog/ai-agent-use-cases/)
- [7 enterprise AI agent use cases in production (eCorpIT)](https://ecorpit.com/enterprise-ai-agents-production-use-cases-2026/)
- [AI Agent Use Cases That Work in Production (AlphaCorp)](https://alphacorp.ai/blog/9-ai-agent-use-cases-that-actually-work-in-production-2026)
- [Best AI Agents for Enterprise 2026: 14 agents across 7 functions (Rework)](https://resources.rework.com/tools/ai-agents/best-ai-agents-for-enterprise-2026)
- [Agentic AI use cases (EICTA, IIT Kanpur)](https://www.eicta.iitk.ac.in/knowledge-hub/artificial-intelligence/agentic-ai-use-cases)
- [AI agent use cases (Atomicwork)](https://www.atomicwork.com/blog/ai-agent-use-cases)
- [Agentic AI in BFSI: Indian banks (Analytics Insight)](https://www.analyticsinsight.net/banking/agentic-ai-in-bfsi-how-indian-banks-are-moving-from-chatbots-to-autonomous-decision-systems)
- [How the BFSI industry is leveraging agentic AI in India (Analytics Insight)](https://www.analyticsinsight.net/bfsi/how-the-bfsi-industry-is-leveraging-agentic-ai-in-india)
- [GFF 2026: agentic AI in financial services](https://book.st-hakky.com/en/news/gff-2026-agentic-ai-financial-services)
- [Agentic AI examples for banks, NBFCs and insurers (Ampcome, vendor)](https://www.ampcome.com/post/agentic-ai-examples-banking-compliance-risk)
- [Complete Guide to AI Agents 2026: Production Playbook](https://perplexityaimagazine.com/ai-tools/complete-guide-to-ai-agents-2026-production-playbook/)
