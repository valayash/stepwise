#!/usr/bin/env bash
# Walks through the refund agent: start a run, see it pause for approval,
# approve it, and watch it finish. Needs `stepwise dev` running on :8080.
set -euo pipefail
BASE=${STEPWISE_URL:-http://localhost:8080}
DIR=$(cd "$(dirname "$0")" && pwd)

field() { sed -n "s/.*\"$1\":\"\{0,1\}\([^\",}]*\).*/\1/p" | head -1; }

echo "1. Starting the refund agent..."
RUN=$(curl -sf "$BASE/v1/runs" -H 'Content-Type: application/json' -d @"$DIR/refund-agent.json" | field id)
echo "   run $RUN"

echo "2. Waiting for it to ask for approval..."
for _ in $(seq 1 50); do
  STATUS=$(curl -sf "$BASE/v1/runs/$RUN" | field status)
  [ "$STATUS" = waiting ] && break
  sleep 0.2
done
INBOX=$(curl -sf "$BASE/v1/approvals")
STEP=$(echo "$INBOX" | field step_id)
echo "   it wants to call: $(echo "$INBOX" | field tool_name) (step $STEP)"

echo "3. Approving..."
curl -sf -X POST "$BASE/v1/runs/$RUN/steps/$STEP/approve" -d '{"decided_by":"demo"}' >/dev/null

for _ in $(seq 1 50); do
  STATUS=$(curl -sf "$BASE/v1/runs/$RUN" | field status)
  [ "$STATUS" = completed ] && break
  sleep 0.2
done
echo "4. Run $STATUS. Refunds in the shop:"
curl -sf "$BASE/demo/refunds"
echo
echo "Open $BASE to see the step timeline."
