package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/tools"
)

// TriageConfig controls the self-healing triage agent.
type TriageConfig struct {
	Enabled bool
	Model   string // model for the triage agent; defaults to "mock"
	// AutoRetry lets the triage agent retry a run without a human approving.
	AutoRetry bool
	// MaxActive caps unfinished triage runs (default 10); past it, failures
	// get a rule-based diagnosis instead, so a burst can't start hundreds.
	MaxActive int
}

const triageSystemPrompt = `You are an SRE assistant. An AI agent run has failed. Diagnose it.

1. Call get_failed_run to read the run's error and its last steps.
2. Decide a category:
   - transient: provider outage, rate limit or timeout. Safe to retry unchanged.
   - bad_input: the request, model name or tool arguments are wrong. Retrying will fail again.
   - tool_bug: a tool keeps failing or returns errors.
   - agent_loop: the agent ran out of turns without finishing.
   - budget: the token budget was too small for the task.
   - other: anything else.
3. Call record_diagnosis with the run_id, the category and a one-sentence summary.
4. Only if the category is transient, call retry_run.
Finish with a short report for the on-call engineer.`

// Built-in tool names.
const (
	toolGetFailedRun    = "get_failed_run"
	toolRecordDiagnosis = "record_diagnosis"
	toolRetryRun        = "retry_run"
)

// triageOf returns the run a triage run is diagnosing, if any.
func triageOf(r core.Run) string {
	var meta struct {
		TriageOf string `json:"triage_of"`
	}
	_ = json.Unmarshal(r.Metadata, &meta)
	return meta.TriageOf
}

// maybeTriage starts a triage run for a failed top-level run.
func (e *Engine) maybeTriage(ctx context.Context, failed core.Run, o store.Outcome) {
	if !e.cfg.Triage.Enabled || failed.ParentRunID != "" || triageOf(failed) != "" {
		return
	}
	// Outages fail many runs at once and the rules already recognise them, so
	// they get no LLM call; any other burst is capped by MaxActive.
	cat := classify(o)
	if cat == "transient" {
		e.diagnoseByRules(ctx, failed, cat, "Provider or network failure: "+oneLine(o.Error)+". Retry once it recovers.")
		return
	}
	if n := e.activeTriageRuns(ctx); n >= e.cfg.Triage.MaxActive {
		e.diagnoseByRules(ctx, failed, cat, fmt.Sprintf("%s (rule-based: %d triage runs already active)", oneLine(o.Error), n))
		return
	}
	model := e.cfg.Triage.Model
	if model == "" {
		model = "mock"
	}
	runID := `{"type":"object","properties":{"run_id":{"type":"string"}},"required":["run_id"]}`
	spec := core.AgentSpec{
		Model:  model,
		System: triageSystemPrompt,
		Tools: []core.ToolSpec{
			{Type: core.ToolBuiltin, Name: toolGetFailedRun, Description: "Read a failed run: its error, failure kind and last steps.", InputSchema: json.RawMessage(runID)},
			{Type: core.ToolBuiltin, Name: toolRecordDiagnosis, Description: "Save your diagnosis on the failed run.", InputSchema: json.RawMessage(
				`{"type":"object","properties":{"run_id":{"type":"string"},"category":{"type":"string","enum":["transient","bad_input","tool_bug","agent_loop","budget","other"]},"summary":{"type":"string"}},"required":["run_id","category","summary"]}`)},
			{Type: core.ToolBuiltin, Name: toolRetryRun, Description: "Put the failed run back in the queue. Only for transient failures.", InputSchema: json.RawMessage(runID)},
		},
		MaxTurns: 8,
	}
	if !e.cfg.Triage.AutoRetry {
		spec.RequireApproval = []string{toolRetryRun}
	}
	meta, _ := json.Marshal(map[string]string{"triage_of": failed.ID})
	input := fmt.Sprintf("Diagnose this failed run.\nrun_id: %s\nfailure_kind: %s\nerror: %s\ncategory: %s\n(The category above was suggested by simple rules. Confirm or correct it.)\nsummary: %s",
		failed.ID, o.FailureKind, oneLine(o.Error), cat, oneLine(o.Error))

	tr, _, err := e.store.CreateRun(ctx, core.NewRun{
		Spec: spec.WithDefaults(), Input: input, BudgetTokens: 50_000, Metadata: meta,
		WebhookURL:     failed.WebhookURL,
		IdempotencyKey: "triage:" + failed.ID + ":" + fmt.Sprint(failed.Claims),
	})
	if err != nil {
		e.log.Warn("could not start triage run", "run_id", failed.ID, "err", err)
		return
	}
	e.m.triaged.Inc()
	e.log.Info("triage run started", "run_id", failed.ID, "triage_run_id", tr.ID)
}

// diagnoseByRules records a diagnosis without an LLM call. With AutoRetry, a
// transient failure is retried once (never again if the retry fails too).
func (e *Engine) diagnoseByRules(ctx context.Context, failed core.Run, category, summary string) {
	d, _ := json.Marshal(map[string]string{"category": category, "summary": summary, "triaged_by": "rules"})
	if err := e.store.SetTriage(ctx, failed.ID, d); err != nil {
		e.log.Warn("could not record diagnosis", "run_id", failed.ID, "err", err)
		return
	}
	if category == "transient" && e.cfg.Triage.AutoRetry && failed.Triage == nil {
		_ = e.store.Retry(ctx, failed.ID)
	}
}

// activeTriageRuns counts unfinished triage runs among the newest runs.
func (e *Engine) activeTriageRuns(ctx context.Context) int {
	n := 0
	for _, st := range []core.RunStatus{core.RunQueued, core.RunRunning, core.RunWaiting} {
		runs, _ := e.store.ListRuns(ctx, store.ListFilter{Status: st, Limit: 1000})
		for _, r := range runs {
			if triageOf(r) != "" {
				n++
			}
		}
	}
	return n
}

// classify is a rule-based first guess the triage agent can confirm or override.
func classify(o store.Outcome) string {
	msg := strings.ToLower(o.Error)
	switch {
	case o.FailureKind == core.FailBudget:
		return "budget"
	case o.FailureKind == core.FailMaxTurns:
		return "agent_loop"
	case o.FailureKind == core.FailExhausted && containsAny(msg, "429", "503", "502", "529", "overloaded", "timeout", "deadline", "connection", "circuit", "outage"):
		return "transient"
	case o.FailureKind == core.FailFatal && containsAny(msg, "400", "unknown model", "invalid"):
		return "bad_input"
	case containsAny(msg, "tool"):
		return "tool_bug"
	}
	return "other"
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// oneLine flattens an error onto one line without commas or semicolons, so
// the prompt's "key: value" lines stay unambiguous.
func oneLine(s string) string {
	s = strings.NewReplacer(",", " ", ";", " ").Replace(s)
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}

// registerTriageTools installs the triage agent's built-in tools.
func (e *Engine) registerTriageTools() {
	argRunID := func(c tools.Call) (string, error) {
		var in struct {
			RunID string `json:"run_id"`
		}
		_ = json.Unmarshal(c.Input, &in)
		if in.RunID == "" {
			return "", fmt.Errorf("run_id is required")
		}
		return in.RunID, nil
	}
	// A triage run may only act on the run it was created for.
	guard := func(ctx context.Context, c tools.Call) (string, *tools.Result) {
		id, err := argRunID(c)
		if err != nil {
			return "", &tools.Result{Text: err.Error(), IsError: true}
		}
		self, err := e.store.GetRun(ctx, c.RunID)
		if err != nil || triageOf(self) != id {
			return "", &tools.Result{Text: "this triage run may only inspect run " + triageOf(self), IsError: true}
		}
		return id, nil
	}

	e.tools.Builtins[toolGetFailedRun] = func(ctx context.Context, c tools.Call) (tools.Result, error) {
		id, bad := guard(ctx, c)
		if bad != nil {
			return *bad, nil
		}
		r, err := e.store.GetRun(ctx, id)
		if err != nil {
			return tools.Result{}, err
		}
		steps, err := e.store.ListSteps(ctx, id)
		if err != nil {
			return tools.Result{}, err
		}
		if len(steps) > 6 {
			steps = steps[len(steps)-6:]
		}
		type stepView struct {
			Seq      int    `json:"seq"`
			Kind     string `json:"kind"`
			Status   string `json:"status"`
			Tool     string `json:"tool,omitempty"`
			Attempts int    `json:"attempts"`
			Result   string `json:"result,omitempty"`
			IsError  bool   `json:"is_error,omitempty"`
		}
		views := make([]stepView, 0, len(steps))
		for _, s := range steps {
			res := s.Result
			if s.LLM != nil {
				res = s.LLM.Content
			}
			if len(res) > 300 {
				res = res[:300] + "..."
			}
			views = append(views, stepView{s.Seq, string(s.Kind), string(s.Status), s.ToolName, s.Attempts, res, s.IsError})
		}
		out, _ := json.MarshalIndent(map[string]any{
			"run_id": r.ID, "status": r.Status, "model": r.Spec.Model, "failure_kind": r.FailureKind,
			"error": r.Error, "tokens_used": r.TokensUsed, "budget_tokens": r.BudgetTokens,
			"claims": r.Claims, "last_steps": views,
		}, "", "  ")
		return tools.Result{Text: string(out)}, nil
	}

	e.tools.Builtins[toolRecordDiagnosis] = func(ctx context.Context, c tools.Call) (tools.Result, error) {
		id, bad := guard(ctx, c)
		if bad != nil {
			return *bad, nil
		}
		var in struct {
			Category string `json:"category"`
			Summary  string `json:"summary"`
		}
		_ = json.Unmarshal(c.Input, &in)
		if in.Category == "" {
			return tools.Result{Text: "category is required", IsError: true}, nil
		}
		d, _ := json.Marshal(map[string]string{"category": in.Category, "summary": in.Summary, "triage_run_id": c.RunID})
		if err := e.store.SetTriage(ctx, id, d); err != nil {
			return tools.Result{}, err
		}
		return tools.Result{Text: "Diagnosis recorded on run " + id + "."}, nil
	}

	e.tools.Builtins[toolRetryRun] = func(ctx context.Context, c tools.Call) (tools.Result, error) {
		id, bad := guard(ctx, c)
		if bad != nil {
			return *bad, nil
		}
		if err := e.store.Retry(ctx, id); err != nil {
			return tools.Result{Text: "Could not retry: " + err.Error(), IsError: true}, nil
		}
		return tools.Result{Text: "Run " + id + " is queued again."}, nil
	}
}
