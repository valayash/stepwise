#!/usr/bin/env bash
# Level 3 of docs/TESTING.md: checks Stepwise against a real model.
# Start the server with a key first, e.g.
#   ANTHROPIC_API_KEY=... go run ./cmd/stepwise dev
#   MODEL=claude-opus-5-5 ./examples/level3.sh      # default model; any claude-* or gpt-* works
# Costs a few cents of tokens. Needs curl and python3.
set -uo pipefail
BASE=${STEPWISE_URL:-http://localhost:8080}
SHOP=${STEPWISE_SHOP_URL:-$BASE/demo/mcp}
MODEL=${MODEL:-claude-opus-5-5}
FAILED=0

json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
pass() { echo "  PASS  $*"; }
fail() { echo "  FAIL  $*"; FAILED=1; }
start() { # agent-json input → run id (prints the API error on failure)
  local body out
  body=$(python3 -c "import json,sys; print(json.dumps({'agent': json.loads(sys.argv[1]), 'input': sys.argv[2], 'budget_tokens': 200000}))" "$1" "$2")
  out=$(curl -s "$BASE/v1/runs" -H 'Content-Type: application/json' -d "$body")
  echo "$out" | json "d.get('id') or ''" 2>/dev/null
  [ -n "$(echo "$out" | json "d.get('id') or ''" 2>/dev/null)" ] || echo "  start failed: $out" >&2
}
wait_for() { # run-id status...
  for _ in $(seq 1 300); do
    local s; s=$(curl -s "$BASE/v1/runs/$1" | json "d['status']")
    for want in "${@:2}"; do [ "$s" = "$want" ] && { echo "$s"; return 0; }; done
    case "$s" in completed|failed|cancelled) echo "$s"; return 1;; esac
    sleep 1
  done
  echo timeout; return 1
}
refund_agent() {
  python3 -c "import json,sys; print(json.dumps({'model': sys.argv[1], 'tools': [{'type': 'mcp', 'url': sys.argv[2]}], 'require_approval': ['issue_refund'],
  'system': 'You are a refund agent. Look up the order, check the refund policy, and call issue_refund only if the policy says the order is eligible. Then explain your decision in one or two sentences.'}))" "$MODEL" "$SHOP"
}

echo "Model: $MODEL   Server: $BASE"
curl -sf "$BASE/v1/info" | json "'  models: ' + ', '.join(d['models'])" || { echo "server not reachable"; exit 1; }

echo "1. Eligible refund: several tool calls, approval, then completion"
RUN=$(start "$(refund_agent)" "My earbuds arrived broken, please refund order ORD-1001 (49.99 USD).")
if [ -n "$RUN" ] && [ "$(wait_for "$RUN" waiting)" = waiting ]; then
  STEP=$(curl -s "$BASE/v1/approvals" | json "[a['step_id'] for a in d['approvals'] if a['run_id']=='$RUN'][0]")
  curl -s -X POST "$BASE/v1/runs/$RUN/steps/$STEP/approve" -d '{"decided_by":"level3"}' >/dev/null
  if [ "$(wait_for "$RUN" completed)" = completed ]; then
    R=$(curl -s "$BASE/v1/runs/$RUN")
    echo "$R" | json "any('RF-' in s.get('result','') for s in d['steps'])" | grep -q True && pass "refund issued after approval" || fail "no refund in tool results"
    echo "    answer: $(echo "$R" | json "d['output'][:200]")"
  else fail "did not complete after approval: $(curl -s "$BASE/v1/runs/$RUN" | json "d.get('error','')")"; fi
else fail "never asked for approval: $(curl -s "$BASE/v1/runs/$RUN" | json "d.get('error','')" 2>/dev/null)"; fi

echo "2. Ineligible refund (delivered 45 days ago): the model must not ask to refund"
RUN=$(start "$(refund_agent)" "Please refund order ORD-1003, the phone case broke.")
ST=$([ -n "$RUN" ] && wait_for "$RUN" completed waiting)
if [ "$ST" = completed ]; then
  pass "declined without calling issue_refund"
  echo "    answer: $(curl -s "$BASE/v1/runs/$RUN" | json "d['output'][:200]")"
else fail "status $ST (it asked to refund an ineligible order, or failed)"; fi

echo "3. Structured output: the answer is JSON matching the schema"
AGENT=$(python3 -c "import json,sys; a=json.loads(sys.argv[1]); a['require_approval']=[]; a['tools'][0]['include']=['get_order','check_refund_policy']
a['response_schema']={'type':'object','properties':{'order_id':{'type':'string'},'eligible':{'type':'boolean'},'reason':{'type':'string'}},'required':['order_id','eligible','reason'],'additionalProperties':False}
print(json.dumps(a))" "$(refund_agent)")
RUN=$(start "$AGENT" "Is order ORD-1003 eligible for a refund? Don't refund it, just tell me.")
if [ -n "$RUN" ] && [ "$(wait_for "$RUN" completed)" = completed ]; then
  OUT=$(curl -s "$BASE/v1/runs/$RUN" | json "d['output']")
  echo "$OUT" | python3 -c "import json,sys; o=json.load(sys.stdin); assert o['order_id']=='ORD-1003' and o['eligible'] is False" 2>/dev/null \
    && pass "valid JSON: $OUT" || fail "unexpected output: $OUT"
else fail "run failed: $(curl -s "$BASE/v1/runs/$RUN" | json "d.get('error','')" 2>/dev/null)"; fi

echo "4. Prompt caching: later turns read the conversation from cache"
CACHED=$(curl -s "$BASE/metrics" | python3 -c "
import sys; print(int(sum(float(l.split()[-1]) for l in sys.stdin if l.startswith('stepwise_llm_tokens_total') and 'cached_input' in l)))")
[ "${CACHED:-0}" -gt 0 ] && pass "$CACHED input tokens served from cache" \
  || fail "no cached tokens (expected after multi-turn runs on Anthropic; prompts under the model's minimum don't cache)"

[ "$FAILED" = 0 ] && echo "All level 3 checks passed." || { echo "Some checks failed; details above. Open $BASE to inspect the runs."; exit 1; }
