#!/usr/bin/env bash
# Walks through the refund agent: start a run, see it pause for approval,
# approve it, and watch it finish.
#
#   ./examples/demo.sh                      # against `stepwise dev` on :8080
#   STEPWISE_SHOP_URL=http://shop:8090/mcp ./examples/demo.sh   # against docker compose
set -euo pipefail
BASE=${STEPWISE_URL:-http://localhost:8080}
SHOP=${STEPWISE_SHOP_URL:-$BASE/demo/mcp}
DIR=$(cd "$(dirname "$0")" && pwd)

# First occurrence of a JSON field (responses are one line; the run's own
# fields come before its steps). No jq needed.
field() { grep -o "\"$1\":\"\{0,1\}[^\",}]*" | head -1 | sed "s/^\"$1\":\"\{0,1\}//"; }
wait_for() { # run status
  for _ in $(seq 1 100); do
    [ "$(curl -sf "$BASE/v1/runs/$1" | field status)" = "$2" ] && return 0
    sleep 0.3
  done
  echo "run $1 never reached $2" >&2
  return 1
}

echo "1. Starting the refund agent..."
BODY=$(sed "s#http://localhost:8080/demo/mcp#$SHOP#" "$DIR/refund-agent.json")
RUN=$(curl -sf "$BASE/v1/runs" -H 'Content-Type: application/json' -d "$BODY" | field id)
echo "   run $RUN"

echo "2. Waiting for it to ask for approval..."
wait_for "$RUN" waiting
INBOX=$(curl -sf "$BASE/v1/approvals")
STEP=$(echo "$INBOX" | field step_id)
echo "   it wants to call: $(echo "$INBOX" | field tool_name) (step $STEP)"

echo "3. Approving..."
curl -sf -X POST "$BASE/v1/runs/$RUN/steps/$STEP/approve" -d '{"decided_by":"demo"}' >/dev/null

wait_for "$RUN" completed
echo "4. Run completed:"
curl -sf "$BASE/v1/runs/$RUN" | field output
echo
echo "Open $BASE to see the step timeline."
echo "$RUN" > "${TMPDIR:-/tmp}/stepwise-demo-run"
