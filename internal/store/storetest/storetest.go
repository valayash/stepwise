// Package storetest is a conformance suite every store.Store must pass.
// The in-memory and Postgres stores run the same tests, so the engine can rely
// on identical semantics from both.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/store"
)

// Factory returns a fresh, empty store for one test.
type Factory func(t *testing.T) store.Store

// Run executes the whole suite.
func Run(t *testing.T, newStore Factory) {
	tests := map[string]func(*testing.T, store.Store){
		"CreateIsIdempotent":            testCreateIdempotent,
		"ClaimIsExclusive":              testClaimExclusive,
		"ExpiredLeaseIsReclaimedFenced": testLeaseExpiryAndFencing,
		"BeginStepResumesSameSeq":       testBeginStepResume,
		"CompletingLLMStepCountsTokens": testCompleteCountsTokens,
		"YieldDelaysNextClaim":          testYieldDelay,
		"ApprovalFlow":                  testApprovalFlow,
		"RejectionBecomesToolError":     testRejection,
		"WaitingWithoutBlockersQueues":  testWaitingWithoutBlockers,
		"SubAgentResolvesParent":        testSubAgent,
		"CancelCascadesAndFences":       testCancel,
		"RetryFailedRun":                testRetry,
		"WebhookOutbox":                 testOutbox,
		"ConcurrentClaimsNeverOverlap":  testConcurrentClaims,
		"ListRunsFilters":               testListRuns,
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) { fn(t, newStore(t)) })
	}
}

var ctx = context.Background()

func spec() core.AgentSpec {
	return core.AgentSpec{Model: "mock", Tools: []core.ToolSpec{{Type: core.ToolHTTP, Name: "lookup", URL: "http://x"}}}
}

func mustCreate(t *testing.T, s store.Store, nr core.NewRun) core.Run {
	t.Helper()
	if nr.Spec.Model == "" {
		nr.Spec = spec()
	}
	r, _, err := s.CreateRun(ctx, nr)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return r
}

func claimOne(t *testing.T, s store.Store, owner string, lease time.Duration) core.Run {
	t.Helper()
	runs, err := s.ClaimRuns(ctx, owner, 1, lease)
	if err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("claimed %d runs, want 1", len(runs))
	}
	return runs[0]
}

func fence(r core.Run) core.Fence { return core.Fence{RunID: r.ID, Token: r.LeaseToken} }

func testCreateIdempotent(t *testing.T, s store.Store) {
	a, created, err := s.CreateRun(ctx, core.NewRun{Spec: spec(), Input: "hi", IdempotencyKey: "k1"})
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	b, created, err := s.CreateRun(ctx, core.NewRun{Spec: spec(), Input: "different", IdempotencyKey: "k1"})
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("second create should return the first run: created=%v id=%s/%s err=%v", created, b.ID, a.ID, err)
	}
	if a.Status != core.RunQueued || a.BudgetTokens != core.DefaultBudgetTokens || a.Spec.MaxTurns != core.DefaultMaxTurns {
		t.Errorf("defaults not applied: %+v", a)
	}
	got, err := s.GetRun(ctx, a.ID)
	if err != nil || got.Input != "hi" {
		t.Errorf("GetRun = %+v, %v", got, err)
	}
	if _, err := s.GetRun(ctx, "nope"); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("missing run err = %v", err)
	}
}

func testClaimExclusive(t *testing.T, s store.Store) {
	r := mustCreate(t, s, core.NewRun{Input: "x"})
	c := claimOne(t, s, "w1", time.Minute)
	if c.ID != r.ID || c.Status != core.RunRunning || c.LeaseOwner != "w1" || c.LeaseToken == 0 {
		t.Fatalf("claim = %+v", c)
	}
	more, _ := s.ClaimRuns(ctx, "w2", 10, time.Minute)
	if len(more) != 0 {
		t.Fatalf("a leased run was claimed twice")
	}
	if err := s.Heartbeat(ctx, fence(c), time.Minute); err != nil {
		t.Errorf("heartbeat: %v", err)
	}
}

func testLeaseExpiryAndFencing(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "x"})
	old := claimOne(t, s, "w1", 50*time.Millisecond)
	time.Sleep(120 * time.Millisecond) // w1 "dies"

	fresh := claimOne(t, s, "w2", time.Minute)
	if fresh.LeaseToken <= old.LeaseToken {
		t.Fatalf("new lease token %d must exceed old %d", fresh.LeaseToken, old.LeaseToken)
	}
	if fresh.Claims != 2 {
		t.Errorf("claims = %d, want 2", fresh.Claims)
	}

	// Every write from the zombie worker must be rejected.
	f := fence(old)
	if err := s.Heartbeat(ctx, f, time.Minute); !errors.Is(err, core.ErrFenced) {
		t.Errorf("Heartbeat with stale token: %v", err)
	}
	if _, err := s.BeginStep(ctx, f, core.Step{Seq: 1, Kind: core.StepLLM}); !errors.Is(err, core.ErrFenced) {
		t.Errorf("BeginStep with stale token: %v", err)
	}
	if err := s.Yield(ctx, f, store.Yield{Status: core.RunQueued}); !errors.Is(err, core.ErrFenced) {
		t.Errorf("Yield with stale token: %v", err)
	}
	if err := s.Finish(ctx, f, store.Outcome{Status: core.RunCompleted}); !errors.Is(err, core.ErrFenced) {
		t.Errorf("Finish with stale token: %v", err)
	}
	// The new owner is unaffected.
	if err := s.Heartbeat(ctx, fence(fresh), time.Minute); err != nil {
		t.Errorf("new owner heartbeat: %v", err)
	}
}

func testBeginStepResume(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "x"})
	r := claimOne(t, s, "w1", time.Minute)
	st, err := s.BeginStep(ctx, fence(r), core.Step{Seq: 1, Kind: core.StepLLM})
	if err != nil || st.ID == 0 || st.Attempts != 1 || st.Status != core.StepRunning {
		t.Fatalf("BeginStep = %+v, %v", st, err)
	}
	// Crash before saving: the next attempt reuses the same step.
	again, err := s.BeginStep(ctx, fence(r), core.Step{Seq: 1, Kind: core.StepLLM})
	if err != nil || again.ID != st.ID || again.Attempts != 2 {
		t.Fatalf("resumed BeginStep = %+v, %v", again, err)
	}
	// A completed step can never be begun again.
	again.Status = core.StepCompleted
	again.LLM = &provider.Response{Content: "done"}
	if err := s.SaveStep(ctx, fence(r), again); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginStep(ctx, fence(r), core.Step{Seq: 1, Kind: core.StepLLM}); !errors.Is(err, core.ErrConflict) {
		t.Errorf("re-beginning a completed step: %v", err)
	}
}

func testCompleteCountsTokens(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "x"})
	r := claimOne(t, s, "w1", time.Minute)
	if err := s.Yield(ctx, fence(r), store.Yield{Status: core.RunQueued, Attempts: 2, Error: "boom"}); err != nil {
		t.Fatal(err)
	}
	r = claimOne(t, s, "w1", time.Minute)
	if r.Attempts != 2 || r.Error != "boom" {
		t.Fatalf("yield did not keep attempts/error: %+v", r)
	}
	st, _ := s.BeginStep(ctx, fence(r), core.Step{Seq: 1, Kind: core.StepLLM})
	st.Status = core.StepCompleted
	st.LLM = &provider.Response{Content: "ok", InputTokens: 30, OutputTokens: 12}
	if err := s.SaveStep(ctx, fence(r), st); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.TokensUsed != 42 || got.Attempts != 0 || got.Error != "" {
		t.Errorf("after completing a step: tokens=%d attempts=%d err=%q", got.TokensUsed, got.Attempts, got.Error)
	}
	steps, _ := s.ListSteps(ctx, r.ID)
	if len(steps) != 1 || steps[0].LLM == nil || steps[0].LLM.Content != "ok" || steps[0].CompletedAt == nil {
		t.Errorf("steps = %+v", steps)
	}
	// Saving the same completion twice must not double-count tokens.
	_ = s.SaveStep(ctx, fence(r), st)
	got, _ = s.GetRun(ctx, r.ID)
	if got.TokensUsed != 42 {
		t.Errorf("tokens double-counted: %d", got.TokensUsed)
	}
}

func testYieldDelay(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "x"})
	r := claimOne(t, s, "w1", time.Minute)
	if err := s.Yield(ctx, fence(r), store.Yield{Status: core.RunQueued, AvailableAt: time.Now().Add(150 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if runs, _ := s.ClaimRuns(ctx, "w1", 1, time.Minute); len(runs) != 0 {
		t.Fatal("claimed a run before its available_at")
	}
	time.Sleep(200 * time.Millisecond)
	claimOne(t, s, "w1", time.Minute)
}

func pendingApproval(t *testing.T, s store.Store) (core.Run, core.Step) {
	t.Helper()
	mustCreate(t, s, core.NewRun{Input: "x", WebhookURL: "http://hook"})
	r := claimOne(t, s, "w1", time.Minute)
	st, err := s.BeginStep(ctx, fence(r), core.Step{
		Seq: 2, Kind: core.StepTool, Status: core.StepPendingApproval,
		ToolCallID: "c1", ToolName: "issue_refund", ToolInput: json.RawMessage(`{"amount":5}`),
	})
	if err != nil || st.Status != core.StepPendingApproval || st.Attempts != 0 {
		t.Fatalf("pending step = %+v, %v", st, err)
	}
	if err := s.Yield(ctx, fence(r), store.Yield{Status: core.RunWaiting}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.Status != core.RunWaiting {
		t.Fatalf("status = %s, want waiting", got.Status)
	}
	if runs, _ := s.ClaimRuns(ctx, "w1", 1, time.Minute); len(runs) != 0 {
		t.Fatal("a waiting run was claimed")
	}
	return r, st
}

func testApprovalFlow(t *testing.T, s store.Store) {
	r, st := pendingApproval(t, s)
	ds, _ := s.ListDeliveries(ctx, r.ID)
	if len(ds) != 1 || ds[0].Event != core.EventApprovalRequested {
		t.Errorf("approval webhook not queued: %+v", ds)
	}
	if err := s.DecideApproval(ctx, r.ID, st.ID, core.Approval{Approved: true, DecidedBy: "ops"}); err != nil {
		t.Fatal(err)
	}
	if err := s.DecideApproval(ctx, r.ID, st.ID, core.Approval{Approved: true}); !errors.Is(err, core.ErrNotRunnable) {
		t.Errorf("double decision: %v", err)
	}
	r2 := claimOne(t, s, "w2", time.Minute)
	steps, _ := s.ListSteps(ctx, r2.ID)
	if steps[0].Status != core.StepApproved || steps[0].Approval == nil || steps[0].Approval.DecidedBy != "ops" {
		t.Fatalf("step = %+v", steps[0])
	}
	// The approved step is executed under the same seq.
	exec, err := s.BeginStep(ctx, fence(r2), core.Step{Seq: 2, Kind: core.StepTool})
	if err != nil || exec.ID != st.ID || exec.Status != core.StepRunning || exec.Attempts != 1 {
		t.Errorf("executing approved step = %+v, %v", exec, err)
	}
}

func testRejection(t *testing.T, s store.Store) {
	r, st := pendingApproval(t, s)
	if err := s.DecideApproval(ctx, r.ID, st.ID, core.Approval{Approved: false, Note: "too large"}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.Status != core.RunQueued {
		t.Errorf("run should be queued to let the model react; got %s", got.Status)
	}
	steps, _ := s.ListSteps(ctx, r.ID)
	if steps[0].Status != core.StepCompleted || !steps[0].IsError || steps[0].Result == "" {
		t.Errorf("rejected step = %+v", steps[0])
	}
}

func testWaitingWithoutBlockers(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "x"})
	r := claimOne(t, s, "w1", time.Minute)
	if err := s.Yield(ctx, fence(r), store.Yield{Status: core.RunWaiting}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetRun(ctx, r.ID)
	if got.Status != core.RunQueued {
		t.Errorf("waiting with nothing to wait for must requeue; got %s", got.Status)
	}
}

func testSubAgent(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "parent"})
	p := claimOne(t, s, "w1", time.Minute)
	ps, _ := s.BeginStep(ctx, fence(p), core.Step{Seq: 2, Kind: core.StepTool, ToolCallID: "c1", ToolName: "research"})

	step, child, err := s.SpawnChild(ctx, fence(p), ps, core.NewRun{Spec: spec(), Input: "child task"})
	if err != nil || step.Status != core.StepWaitingChild || step.ChildRunID != child.ID || child.ParentRunID != p.ID {
		t.Fatalf("SpawnChild = %+v / %+v, %v", step, child, err)
	}
	// Spawning again (after a crash) must reuse the same child.
	_, again, _ := s.SpawnChild(ctx, fence(p), ps, core.NewRun{Spec: spec(), Input: "child task"})
	if again.ID != child.ID {
		t.Fatal("SpawnChild is not idempotent per step")
	}
	if err := s.Yield(ctx, fence(p), store.Yield{Status: core.RunWaiting}); err != nil {
		t.Fatal(err)
	}

	c := claimOne(t, s, "w2", time.Minute)
	if c.ID != child.ID {
		t.Fatalf("claimed %s, want the child", c.ID)
	}
	cs, _ := s.BeginStep(ctx, fence(c), core.Step{Seq: 1, Kind: core.StepLLM})
	cs.Status = core.StepCompleted
	cs.LLM = &provider.Response{Content: "found it", InputTokens: 10, OutputTokens: 5}
	_ = s.SaveStep(ctx, fence(c), cs)
	if err := s.Finish(ctx, fence(c), store.Outcome{Status: core.RunCompleted, Output: "found it"}); err != nil {
		t.Fatal(err)
	}

	parent, _ := s.GetRun(ctx, p.ID)
	if parent.Status != core.RunQueued {
		t.Errorf("parent status = %s, want queued", parent.Status)
	}
	if parent.TokensUsed != 15 {
		t.Errorf("child tokens not rolled up: %d", parent.TokensUsed)
	}
	steps, _ := s.ListSteps(ctx, p.ID)
	if steps[0].Status != core.StepCompleted || steps[0].Result != "found it" || steps[0].IsError {
		t.Errorf("parent step = %+v", steps[0])
	}
	kids, _ := s.ListRuns(ctx, store.ListFilter{ParentRunID: p.ID})
	if len(kids) != 1 {
		t.Errorf("children listed = %d", len(kids))
	}
}

func testCancel(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "parent", WebhookURL: "http://hook"})
	p := claimOne(t, s, "w1", time.Minute)
	ps, _ := s.BeginStep(ctx, fence(p), core.Step{Seq: 2, Kind: core.StepTool, ToolCallID: "c", ToolName: "sub"})
	_, child, _ := s.SpawnChild(ctx, fence(p), ps, core.NewRun{Spec: spec(), Input: "c"})

	if err := s.Cancel(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(ctx, fence(p), time.Minute); !errors.Is(err, core.ErrFenced) {
		t.Errorf("worker of a cancelled run must be fenced: %v", err)
	}
	got, _ := s.GetRun(ctx, p.ID)
	kid, _ := s.GetRun(ctx, child.ID)
	if got.Status != core.RunCancelled || kid.Status != core.RunCancelled {
		t.Errorf("statuses = %s / %s", got.Status, kid.Status)
	}
	if err := s.Cancel(ctx, p.ID); !errors.Is(err, core.ErrNotRunnable) {
		t.Errorf("cancel twice: %v", err)
	}
	ds, _ := s.ListDeliveries(ctx, p.ID)
	if len(ds) != 1 || ds[0].Event != core.EventRunCancelled {
		t.Errorf("deliveries = %+v", ds)
	}
}

func testRetry(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "x"})
	r := claimOne(t, s, "w1", time.Minute)
	if err := s.Retry(ctx, r.ID); !errors.Is(err, core.ErrNotRunnable) {
		t.Errorf("retrying a running run: %v", err)
	}
	_ = s.Finish(ctx, fence(r), store.Outcome{Status: core.RunFailed, Error: "x", FailureKind: core.FailExhausted})
	if err := s.Retry(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	got := claimOne(t, s, "w1", time.Minute)
	if got.Error != "" || got.FailureKind != "" || got.Attempts != 0 || got.CompletedAt != nil {
		t.Errorf("retried run not reset: %+v", got)
	}
	if err := s.SetTriage(ctx, r.ID, json.RawMessage(`{"category":"transient"}`)); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetRun(ctx, r.ID)
	if string(got.Triage) == "" {
		t.Error("triage not stored")
	}
}

func testOutbox(t *testing.T, s store.Store) {
	mustCreate(t, s, core.NewRun{Input: "x", WebhookURL: "http://hook", Metadata: json.RawMessage(`{"order":"ORD-1"}`)})
	r := claimOne(t, s, "w1", time.Minute)
	if err := s.Finish(ctx, fence(r), store.Outcome{Status: core.RunCompleted, Output: "refunded"}); err != nil {
		t.Fatal(err)
	}
	ds, err := s.ClaimDeliveries(ctx, 10, time.Minute)
	if err != nil || len(ds) != 1 {
		t.Fatalf("ClaimDeliveries = %+v, %v", ds, err)
	}
	d := ds[0]
	var payload struct {
		Type string `json:"type"`
		Data struct {
			RunID    string          `json:"run_id"`
			Output   string          `json:"output"`
			Metadata json.RawMessage `json:"metadata"`
		} `json:"data"`
	}
	_ = json.Unmarshal(d.Payload, &payload)
	if d.Event != core.EventRunCompleted || payload.Data.RunID != r.ID || payload.Data.Output != "refunded" || len(payload.Data.Metadata) == 0 {
		t.Errorf("delivery = %+v payload=%+v", d, payload)
	}
	if again, _ := s.ClaimDeliveries(ctx, 10, time.Minute); len(again) != 0 {
		t.Error("a leased delivery was claimed twice")
	}
	if err := s.FailDelivery(ctx, d.ID, time.Now().Add(-time.Second), "500", false); err != nil {
		t.Fatal(err)
	}
	ds, _ = s.ClaimDeliveries(ctx, 10, time.Minute)
	if len(ds) != 1 || ds[0].Attempts != 2 {
		t.Fatalf("retry claim = %+v", ds)
	}
	if err := s.CompleteDelivery(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	all, _ := s.ListDeliveries(ctx, r.ID)
	if all[0].Status != store.DeliveryDelivered {
		t.Errorf("status = %s", all[0].Status)
	}
}

func testConcurrentClaims(t *testing.T, s store.Store) {
	const runs, workers = 40, 8
	for i := 0; i < runs; i++ {
		mustCreate(t, s, core.NewRun{Input: "x"})
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := s.ClaimRuns(ctx, "w", 3, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, r := range got {
					seen[r.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != runs {
		t.Errorf("claimed %d distinct runs, want %d", len(seen), runs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("run %s claimed %d times", id, n)
		}
	}
}

func testListRuns(t *testing.T, s store.Store) {
	for i := 0; i < 3; i++ {
		mustCreate(t, s, core.NewRun{Input: "x"})
	}
	claimOne(t, s, "w", time.Minute)
	all, _ := s.ListRuns(ctx, store.ListFilter{})
	queued, _ := s.ListRuns(ctx, store.ListFilter{Status: core.RunQueued})
	limited, _ := s.ListRuns(ctx, store.ListFilter{Limit: 2})
	if len(all) != 3 || len(queued) != 2 || len(limited) != 2 {
		t.Errorf("all=%d queued=%d limited=%d", len(all), len(queued), len(limited))
	}
}
