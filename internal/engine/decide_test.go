package engine

import (
	"encoding/json"
	"testing"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/provider"
)

func llm(seq int, status core.StepStatus, content string, calls ...string) core.Step {
	s := core.Step{Seq: seq, Kind: core.StepLLM, Status: status}
	if status == core.StepCompleted {
		r := &provider.Response{Content: content}
		for _, c := range calls {
			r.ToolCalls = append(r.ToolCalls, provider.ToolCall{ID: c, Name: "t_" + c, Input: json.RawMessage(`{}`)})
		}
		s.LLM = r
	}
	return s
}

func tool(seq int, callID string, status core.StepStatus) core.Step {
	return core.Step{Seq: seq, Kind: core.StepTool, Status: status, ToolCallID: callID, ToolName: "t_" + callID, Result: "r_" + callID}
}

func baseRun() core.Run {
	return core.Run{Input: "hi", BudgetTokens: 1000, Spec: core.AgentSpec{Model: "m", MaxTurns: 5, System: "sys"}}
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name  string
		run   func(*core.Run)
		steps []core.Step
		want  actionKind
		seq   int
		call  string
	}{
		{name: "fresh run calls the model", want: actCallLLM, seq: 1},
		{name: "interrupted llm step is re-run in place", steps: []core.Step{llm(1, core.StepRunning, "")}, want: actCallLLM, seq: 1},
		{name: "final answer completes", steps: []core.Step{llm(1, core.StepCompleted, "done")}, want: actComplete},
		{name: "tool requested → start it", steps: []core.Step{llm(1, core.StepCompleted, "", "a", "b")}, want: actStartTool, seq: 2, call: "a"},
		{name: "second tool started after first", steps: []core.Step{llm(1, core.StepCompleted, "", "a", "b"), tool(2, "a", core.StepCompleted)}, want: actStartTool, seq: 3, call: "b"},
		{name: "interrupted tool resumes", steps: []core.Step{llm(1, core.StepCompleted, "", "a"), tool(2, "a", core.StepRunning)}, want: actExecTool},
		{name: "approved tool executes", steps: []core.Step{llm(1, core.StepCompleted, "", "a"), tool(2, "a", core.StepApproved)}, want: actExecTool},
		{name: "pending approval waits", steps: []core.Step{llm(1, core.StepCompleted, "", "a"), tool(2, "a", core.StepPendingApproval)}, want: actWait},
		{name: "waiting child waits", steps: []core.Step{llm(1, core.StepCompleted, "", "a", "b"), tool(2, "a", core.StepWaitingChild), tool(3, "b", core.StepCompleted)}, want: actWait},
		{name: "all tools done → next turn", steps: []core.Step{llm(1, core.StepCompleted, "", "a"), tool(2, "a", core.StepCompleted)}, want: actCallLLM, seq: 3},
		{
			name: "max turns", run: func(r *core.Run) { r.Spec.MaxTurns = 1 },
			steps: []core.Step{llm(1, core.StepCompleted, "", "a"), tool(2, "a", core.StepCompleted)}, want: actFail,
		},
		{name: "budget", run: func(r *core.Run) { r.TokensUsed = 1000 }, want: actFail},
		{
			name: "budget does not block finishing", run: func(r *core.Run) { r.TokensUsed = 5000 },
			steps: []core.Step{llm(1, core.StepCompleted, "done")}, want: actComplete,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := baseRun()
			if c.run != nil {
				c.run(&r)
			}
			got := decide(r, c.steps)
			if got.kind != c.want {
				t.Fatalf("kind = %v, want %v (%+v)", got.kind, c.want, got)
			}
			if c.seq != 0 && got.seq != c.seq {
				t.Errorf("seq = %d, want %d", got.seq, c.seq)
			}
			if c.call != "" && (got.call == nil || got.call.ID != c.call) {
				t.Errorf("call = %+v, want %s", got.call, c.call)
			}
		})
	}
}

func TestConversationRebuild(t *testing.T) {
	steps := []core.Step{
		tool(3, "b", core.StepPendingApproval), // not completed: skipped
		llm(1, core.StepCompleted, "checking", "a", "b"),
		tool(2, "a", core.StepCompleted),
	}
	steps[2].IsError = true
	msgs := conversation(baseRun(), steps)
	roles := ""
	for _, m := range msgs {
		roles += m.Role + ","
	}
	if roles != "system,user,assistant,tool," {
		t.Fatalf("roles = %s", roles)
	}
	if len(msgs[2].ToolCalls) != 2 || msgs[3].ToolCallID != "a" || !msgs[3].IsError || msgs[3].Content != "r_a" {
		t.Errorf("messages = %+v", msgs)
	}
}
