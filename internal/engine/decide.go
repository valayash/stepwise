package engine

import (
	"sort"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/provider"
)

// actionKind is what the worker should do next for a run.
type actionKind int

const (
	actCallLLM   actionKind = iota // ask the model for the next turn (new step, or re-run an interrupted one)
	actStartTool                   // the model asked for a tool that has no step yet
	actExecTool                    // execute an existing tool step (interrupted, or approved)
	actComplete                    // the model gave its final answer
	actFail                        // a limit was hit
	actWait                        // everything left is blocked on a human or a sub-agent
)

type action struct {
	kind   actionKind
	seq    int                // step position for actCallLLM / actStartTool
	step   *core.Step         // actExecTool, or an interrupted llm step for actCallLLM
	call   *provider.ToolCall // actStartTool
	output string             // actComplete
	fail   core.FailureKind   // actFail
	reason string             // actFail
}

// decide looks only at saved state and returns the next action. Because it is
// a pure function of what the store holds, a worker that picks a run up after
// a crash continues exactly where the previous worker stopped.
func decide(run core.Run, steps []core.Step) action {
	steps = sortedBySeq(steps)
	next := 1
	if len(steps) > 0 {
		next = steps[len(steps)-1].Seq + 1
	}

	// 1. Finish anything that was interrupted mid-execution (a crash, a lost
	//    lease, a transient failure that was put back in the queue).
	for i := range steps {
		s := &steps[i]
		if s.Status != core.StepRunning {
			continue
		}
		if s.Kind == core.StepLLM {
			return action{kind: actCallLLM, seq: s.Seq, step: s}
		}
		return action{kind: actExecTool, step: s}
	}

	// 2. Execute tool calls a human approved.
	for i := range steps {
		if steps[i].Status == core.StepApproved {
			return action{kind: actExecTool, step: &steps[i]}
		}
	}

	// 3. Look at the model's latest turn, and at the latest user message
	//    (conversations). max_turns counts model calls per user message.
	var last *core.Step
	llmTurns, lastUser := 0, 0
	for i := range steps {
		switch {
		case steps[i].Kind == core.StepUser:
			lastUser, llmTurns = steps[i].Seq, 0
		case steps[i].Kind == core.StepLLM && steps[i].Status == core.StepCompleted:
			last = &steps[i]
			llmTurns++
		}
	}
	callLLM := func() action {
		if llmTurns >= run.Spec.MaxTurns {
			return action{kind: actFail, fail: core.FailMaxTurns, reason: "agent reached max_turns without a final answer"}
		}
		if run.TokensUsed >= run.BudgetTokens {
			return action{kind: actFail, fail: core.FailBudget, reason: "token budget used up"}
		}
		return action{kind: actCallLLM, seq: next}
	}
	if last == nil || last.LLM == nil || lastUser > last.Seq {
		return callLLM() // nothing answered yet, or a new user message to answer
	}
	if len(last.LLM.ToolCalls) == 0 {
		return action{kind: actComplete, output: last.LLM.Content}
	}

	// 4. Start every requested tool call that has no step yet, in order.
	byCall := map[string]core.Step{}
	for _, s := range steps {
		if s.Kind == core.StepTool && s.Seq > last.Seq {
			byCall[s.ToolCallID] = s
		}
	}
	for i := range last.LLM.ToolCalls {
		tc := last.LLM.ToolCalls[i]
		if _, ok := byCall[tc.ID]; !ok {
			return action{kind: actStartTool, seq: next, call: &tc}
		}
	}

	// 5. All calls have steps. Wait for blocked ones, then give the model the results.
	for _, s := range byCall {
		if s.Status == core.StepPendingApproval || s.Status == core.StepWaitingChild {
			return action{kind: actWait}
		}
	}
	return callLLM()
}

// conversation rebuilds the model's message history from saved steps.
func conversation(run core.Run, steps []core.Step) []provider.Message {
	msgs := make([]provider.Message, 0, len(steps)+2)
	if run.Spec.System != "" {
		msgs = append(msgs, provider.Message{Role: "system", Content: run.Spec.System})
	}
	msgs = append(msgs, provider.Message{Role: "user", Content: run.Input})
	for _, s := range sortedBySeq(steps) {
		if s.Status != core.StepCompleted {
			continue
		}
		switch s.Kind {
		case core.StepLLM:
			if s.LLM != nil {
				msgs = append(msgs, provider.Message{Role: "assistant", Content: s.LLM.Content, ToolCalls: s.LLM.ToolCalls})
			}
		case core.StepTool:
			msgs = append(msgs, provider.Message{Role: "tool", ToolCallID: s.ToolCallID, Content: s.Result, IsError: s.IsError})
		case core.StepUser:
			msgs = append(msgs, provider.Message{Role: "user", Content: s.Result})
		}
	}
	return msgs
}

func sortedBySeq(steps []core.Step) []core.Step {
	out := append([]core.Step(nil), steps...)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}
