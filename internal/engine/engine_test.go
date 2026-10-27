package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/demo"
	"github.com/valayash/stepwise/internal/mcp"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/store/memory"
	"github.com/valayash/stepwise/internal/tools"
)

// ---- harness ----

type harness struct {
	t      *testing.T
	store  *memory.Store
	shop   *demo.Shop
	shopMC string // MCP URL
	runner *tools.Runner
	mock   *provider.Mock
	engine *Engine
	cfg    Config
	deps   Deps
	stop   context.CancelFunc
	done   chan struct{}
}

func newHarness(t *testing.T, extra ...provider.Route) *harness {
	t.Helper()
	shop := demo.NewShop(time.Now())
	srv := httptest.NewServer(shop.Handler())
	t.Cleanup(srv.Close)

	h := &harness{t: t, store: memory.New(nil), shop: shop, shopMC: srv.URL + "/mcp", runner: tools.NewRunner(), mock: provider.NewMock()}
	routes := append(extra, provider.Route{Prefix: "mock", Provider: h.mock})
	h.cfg = Config{
		WorkerID: "test", Workers: 4, Lease: 400 * time.Millisecond, PollInterval: 20 * time.Millisecond,
		MaxAttempts: 4, BackoffBase: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
	}
	h.deps = Deps{
		Store: h.store, Router: provider.NewRouter(routes...), Tools: h.runner,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return h
}

func (h *harness) start() {
	h.engine = New(h.cfg, h.deps)
	ctx, cancel := context.WithCancel(context.Background())
	h.stop, h.done = cancel, make(chan struct{})
	go func() { h.engine.Run(ctx); close(h.done) }()
	h.t.Cleanup(func() { h.shutdown() })
}

func (h *harness) shutdown() {
	if h.stop != nil {
		h.stop()
		<-h.done
		h.stop = nil
	}
}

func (h *harness) create(spec core.AgentSpec, input string, budget int) core.Run {
	h.t.Helper()
	spec, err := h.runner.Prepare(context.Background(), spec)
	if err != nil {
		h.t.Fatalf("Prepare: %v", err)
	}
	r, _, err := h.store.CreateRun(context.Background(), core.NewRun{Spec: spec, Input: input, BudgetTokens: budget})
	if err != nil {
		h.t.Fatal(err)
	}
	return r
}

func (h *harness) waitStatus(id string, want core.RunStatus) core.Run {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, _ := h.store.GetRun(context.Background(), id)
		if r.Status == want {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	r, _ := h.store.GetRun(context.Background(), id)
	steps, _ := h.store.ListSteps(context.Background(), id)
	h.t.Fatalf("run %s: status %s (error %q), want %s; steps: %s", id, r.Status, r.Error, want, describe(steps))
	return r
}

func (h *harness) steps(id string) []core.Step {
	s, _ := h.store.ListSteps(context.Background(), id)
	return s
}

func describe(steps []core.Step) string {
	var parts []string
	for _, s := range steps {
		d := string(s.Kind) + ":" + string(s.Status)
		if s.ToolName != "" {
			d += "(" + s.ToolName + ")"
		}
		parts = append(parts, d)
	}
	return strings.Join(parts, " → ")
}

func shopSpec(url string, approve ...string) core.AgentSpec {
	return core.AgentSpec{
		Model:           "mock",
		System:          "You are a refund agent.",
		Tools:           []core.ToolSpec{{Type: core.ToolMCP, URL: url}},
		RequireApproval: approve,
	}
}

const refundPrompt = "Customer asks for a refund. order_id: ORD-1001, amount: 49.99, reason: damaged"

// ---- tests ----

func TestRefundAgentWithHumanApproval(t *testing.T) {
	h := newHarness(t)
	h.start()
	run := h.create(shopSpec(h.shopMC, "issue_refund"), refundPrompt, 0)

	h.waitStatus(run.ID, core.RunWaiting)
	var pending core.Step
	for _, s := range h.steps(run.ID) {
		if s.Status == core.StepPendingApproval {
			pending = s
		}
	}
	if pending.ToolName != "issue_refund" || !strings.Contains(string(pending.ToolInput), "ORD-1001") {
		t.Fatalf("pending step = %+v", pending)
	}
	if h.shop.Stats().RefundsIssued != 0 {
		t.Fatal("refund issued before approval")
	}

	if err := h.store.DecideApproval(context.Background(), run.ID, pending.ID, core.Approval{Approved: true, DecidedBy: "lead"}); err != nil {
		t.Fatal(err)
	}
	done := h.waitStatus(run.ID, core.RunCompleted)

	if got := describe(h.steps(run.ID)); got != "llm:completed → tool:completed(get_order) → llm:completed → tool:completed(check_refund_policy) → llm:completed → tool:completed(issue_refund) → llm:completed" {
		t.Errorf("steps = %s", got)
	}
	if !strings.Contains(done.Output, "RF-00001") {
		t.Errorf("output = %q", done.Output)
	}
	if st := h.shop.Stats(); st.RefundsIssued != 1 || st.Calls["issue_refund"] != 1 {
		t.Errorf("shop stats = %+v", st)
	}
	if done.TokensUsed == 0 {
		t.Error("tokens not tracked")
	}
}

func TestRejectedApprovalIsReportedToTheModel(t *testing.T) {
	h := newHarness(t)
	h.start()
	run := h.create(shopSpec(h.shopMC, "issue_refund"), refundPrompt, 0)
	h.waitStatus(run.ID, core.RunWaiting)
	for _, s := range h.steps(run.ID) {
		if s.Status == core.StepPendingApproval {
			_ = h.store.DecideApproval(context.Background(), run.ID, s.ID, core.Approval{Approved: false, Note: "fraud risk"})
		}
	}
	done := h.waitStatus(run.ID, core.RunCompleted)
	if h.shop.Stats().RefundsIssued != 0 {
		t.Error("a rejected refund was issued")
	}
	if !strings.Contains(done.Output, "fraud risk") {
		t.Errorf("model should see the rejection; output = %q", done.Output)
	}
}

// A worker issues the refund and dies before saving the result. The next
// worker re-executes the step with the same idempotency key, and the shop
// returns the original refund instead of paying twice.
func TestUnansweredApprovalExpires(t *testing.T) {
	h := newHarness(t)
	h.start()
	spec := shopSpec(h.shopMC, "issue_refund")
	spec.ApprovalTimeout = "10ms"
	run := h.create(spec, refundPrompt, 0)
	done := h.waitStatus(run.ID, core.RunCompleted) // nobody decides; the sweep rejects it
	if h.shop.Stats().RefundsIssued != 0 || !strings.Contains(done.Output, "rejected automatically") {
		t.Errorf("refunds %d, output %q", h.shop.Stats().RefundsIssued, done.Output)
	}
}

func TestRetentionDeletesFinishedRuns(t *testing.T) {
	h := newHarness(t)
	h.cfg.Retention = time.Millisecond
	h.start()
	run := h.create(core.AgentSpec{Model: "mock"}, "hi", 0)
	h.waitStatus(run.ID, core.RunCompleted)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if _, err := h.store.GetRun(context.Background(), run.ID); errors.Is(err, core.ErrNotFound) {
			return
		}
	}
	t.Error("finished run was not deleted")
}

func TestConversationContinuesARun(t *testing.T) {
	h := newHarness(t)
	h.start()
	run := h.create(core.AgentSpec{Model: "mock", MaxTurns: 1}, "hi", 0) // max_turns counts per message
	if out := h.waitStatus(run.ID, core.RunCompleted).Output; out != "mock: hi" {
		t.Fatalf("first reply = %q", out)
	}
	if _, err := h.store.AddMessage(context.Background(), run.ID, "and the second thing?"); err != nil {
		t.Fatal(err)
	}
	if out := h.waitStatus(run.ID, core.RunCompleted).Output; out != "mock: and the second thing?" {
		t.Errorf("second reply = %q", out)
	}
	var kinds []string
	for _, s := range h.steps(run.ID) {
		kinds = append(kinds, string(s.Kind))
	}
	if strings.Join(kinds, ",") != "llm,user,llm" {
		t.Errorf("steps = %v", kinds)
	}
}

func TestKillResumesFromSavedSteps(t *testing.T) {
	h := newHarness(t)
	h.mock.Latency = 150 * time.Millisecond // time to kill mid-call
	h.start()
	run := h.create(shopSpec(h.shopMC), refundPrompt, 0)
	var killedSeq int
	for deadline := time.Now().Add(5 * time.Second); killedSeq == 0 && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		for _, s := range h.steps(run.ID) {
			if s.Kind == core.StepLLM && s.Status == core.StepRunning && s.Seq > 1 && h.engine.Kill(run.ID) {
				killedSeq = s.Seq // killed during a later model call, after earlier steps were saved
			}
		}
	}
	if killedSeq == 0 {
		t.Fatal("never caught the run mid-step")
	}
	done := h.waitStatus(run.ID, core.RunCompleted)
	if done.Claims < 2 || h.shop.Stats().RefundsIssued != 1 {
		t.Errorf("claims %d, refunds %d; want a second claim and exactly one refund", done.Claims, h.shop.Stats().RefundsIssued)
	}
	for _, s := range h.steps(run.ID) {
		if want := 1 + btoi(s.Seq == killedSeq); s.Attempts != want {
			t.Errorf("step %d ran %d times, want %d (only the interrupted step runs again)", s.Seq, s.Attempts, want)
		}
	}
	if h.engine.Kill(run.ID) {
		t.Error("killing a finished run should report false")
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestCrashAfterSideEffectIsExactlyOnce(t *testing.T) {
	h := newHarness(t)
	spec, _ := h.runner.Prepare(context.Background(), core.AgentSpec{Model: "mock", Tools: []core.ToolSpec{{Type: core.ToolMCP, URL: h.shopMC, Include: []string{"issue_refund"}}}})
	ctx := context.Background()
	run, _, _ := h.store.CreateRun(ctx, core.NewRun{Spec: spec, Input: refundPrompt})

	// The doomed worker.
	claimed, _ := h.store.ClaimRuns(ctx, "doomed", 1, 100*time.Millisecond)
	f := core.Fence{RunID: run.ID, Token: claimed[0].LeaseToken}
	llmStep, _ := h.store.BeginStep(ctx, f, core.Step{Seq: 1, Kind: core.StepLLM})
	llmStep.Status = core.StepCompleted
	llmStep.LLM = &provider.Response{FinishReason: "tool_calls", ToolCalls: []provider.ToolCall{{
		ID: "call_1", Name: "issue_refund", Input: json.RawMessage(`{"order_id":"ORD-1001","amount":49.99}`),
	}}}
	_ = h.store.SaveStep(ctx, f, llmStep)
	toolStep, _ := h.store.BeginStep(ctx, f, core.Step{Seq: 2, Kind: core.StepTool, ToolCallID: "call_1", ToolName: "issue_refund", ToolInput: llmStep.LLM.ToolCalls[0].Input})
	res, err := h.runner.MCP.CallTool(ctx, h.shopMC, "issue_refund", toolStep.ToolInput, toolStep.IdempotencyKey())
	if err != nil || res.IsError {
		t.Fatalf("doomed worker's refund failed: %+v %v", res, err)
	}
	// ...and the process dies here: the result is never saved.

	h.start()
	h.waitStatus(run.ID, core.RunCompleted)

	steps := h.steps(run.ID)
	if steps[1].Attempts != 2 {
		t.Errorf("tool step attempts = %d, want 2 (re-executed once)", steps[1].Attempts)
	}
	st := h.shop.Stats()
	if st.RefundsIssued != 1 || st.DuplicatesSeen != 1 || st.Calls["issue_refund"] != 2 {
		t.Errorf("want 2 calls, 1 refund, 1 duplicate absorbed; got %+v", st)
	}
	got, _ := h.store.GetRun(ctx, run.ID)
	if got.Claims != 2 {
		t.Errorf("claims = %d, want 2", got.Claims)
	}
}

// flaky fails the first n calls with a retryable 503.
type flaky struct {
	*provider.Mock
	failFirst int32
	calls     atomic.Int32
}

func (f *flaky) Name() string { return "flaky" }
func (f *flaky) Complete(ctx context.Context, r provider.Request) (provider.Response, error) {
	if f.calls.Add(1) <= f.failFirst {
		return provider.Response{}, &provider.UpstreamError{Provider: "flaky", Status: 503, Body: "overloaded"}
	}
	return f.Mock.Complete(ctx, r)
}

func TestTransientFailuresAreRetried(t *testing.T) {
	fl := &flaky{Mock: provider.NewMock(), failFirst: 2}
	h := newHarness(t, provider.Route{Prefix: "flaky", Provider: fl})
	h.start()
	run := h.create(core.AgentSpec{Model: "flaky"}, "hello", 0)
	done := h.waitStatus(run.ID, core.RunCompleted)
	if done.Output != "mock: hello" || done.Attempts != 0 {
		t.Errorf("run = %+v", done)
	}
	steps := h.steps(run.ID)
	if len(steps) != 1 || steps[0].Attempts != 3 {
		t.Errorf("llm step attempts = %d, want 3 (two failures, one success)", steps[0].Attempts)
	}
	if v := h.engine.m.retries.Value("transient"); v != 2 {
		t.Errorf("retries metric = %v", v)
	}
}

func TestExhaustedRunIsTriagedAndRetriedAfterApproval(t *testing.T) {
	fl := &flaky{Mock: provider.NewMock(), failFirst: 4} // MaxAttempts is 4
	h := newHarness(t, provider.Route{Prefix: "flaky", Provider: fl})
	h.cfg.Triage = TriageConfig{Enabled: true, Model: "mock"}
	h.start()

	run := h.create(core.AgentSpec{Model: "flaky"}, "hello", 0)
	failed := h.waitStatus(run.ID, core.RunFailed)
	if failed.FailureKind != core.FailExhausted || !strings.Contains(failed.Error, "503") {
		t.Fatalf("failed run = %+v", failed)
	}

	// The triage agent reads the failure, records a diagnosis, then asks to retry.
	var triage core.Run
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && triage.ID == "" {
		runs, _ := h.store.ListRuns(context.Background(), store.ListFilter{Status: core.RunWaiting})
		for _, r := range runs {
			if triageOf(r) == run.ID {
				triage = r
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if triage.ID == "" {
		t.Fatal("no triage run waiting for approval")
	}
	got, _ := h.store.GetRun(context.Background(), run.ID)
	var diag map[string]string
	_ = json.Unmarshal(got.Triage, &diag)
	if diag["category"] != "transient" || diag["triage_run_id"] != triage.ID {
		t.Errorf("diagnosis = %v", diag)
	}

	for _, s := range h.steps(triage.ID) {
		if s.Status == core.StepPendingApproval && s.ToolName == toolRetryRun {
			_ = h.store.DecideApproval(context.Background(), triage.ID, s.ID, core.Approval{Approved: true})
		}
	}
	h.waitStatus(triage.ID, core.RunCompleted)
	retried := h.waitStatus(run.ID, core.RunCompleted) // the provider has recovered by now
	if retried.Output != "mock: hello" {
		t.Errorf("retried run output = %q", retried.Output)
	}
}

func TestFatalErrorFailsImmediately(t *testing.T) {
	h := newHarness(t)
	h.start()
	r, _, _ := h.store.CreateRun(context.Background(), core.NewRun{Spec: core.AgentSpec{Model: "no-such-model"}, Input: "x"})
	got := h.waitStatus(r.ID, core.RunFailed)
	if got.FailureKind != core.FailFatal || got.Claims != 1 {
		t.Errorf("run = %+v", got)
	}
}

func TestSubAgentsRunAndReport(t *testing.T) {
	h := newHarness(t)
	h.start()
	run := h.create(core.AgentSpec{
		Model: "mock",
		Tools: []core.ToolSpec{{Type: core.ToolAgent, Name: "researcher", Agent: &core.AgentSpec{Model: "mock", System: "Research things."}}},
	}, "Coordinate this. task: find-late-orders", 0)

	done := h.waitStatus(run.ID, core.RunCompleted)
	kids, _ := h.store.ListRuns(context.Background(), store.ListFilter{ParentRunID: run.ID})
	if len(kids) != 1 || kids[0].Status != core.RunCompleted || kids[0].Input != "find-late-orders" {
		t.Fatalf("children = %+v", kids)
	}
	steps := h.steps(run.ID)
	if steps[1].ChildRunID != kids[0].ID || steps[1].Result != "mock: find-late-orders" {
		t.Errorf("parent tool step = %+v", steps[1])
	}
	if !strings.Contains(done.Output, "mock: find-late-orders") {
		t.Errorf("parent output = %q", done.Output)
	}
	if done.TokensUsed <= kids[0].TokensUsed {
		t.Errorf("child tokens not rolled into parent: parent %d, child %d", done.TokensUsed, kids[0].TokensUsed)
	}
}

func TestBudgetStopsTheAgent(t *testing.T) {
	h := newHarness(t)
	h.start()
	run := h.create(shopSpec(h.shopMC), refundPrompt, 5)
	got := h.waitStatus(run.ID, core.RunFailed)
	if got.FailureKind != core.FailBudget {
		t.Errorf("failure kind = %s", got.FailureKind)
	}
}

func TestCancelStopsTheWorker(t *testing.T) {
	h := newHarness(t)
	h.mock.Latency = 300 * time.Millisecond
	h.start()
	run := h.create(core.AgentSpec{Model: "mock"}, "slow", 0)
	h.waitStatus(run.ID, core.RunRunning)
	if err := h.store.Cancel(context.Background(), run.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond) // the in-flight call ends; its writes must be fenced off
	got, _ := h.store.GetRun(context.Background(), run.ID)
	if got.Status != core.RunCancelled {
		t.Fatalf("a cancelled run was overwritten: %s", got.Status)
	}
	if steps := h.steps(run.ID); len(steps) > 0 && steps[0].Status == core.StepCompleted {
		t.Error("the fenced worker saved a step after cancellation")
	}
}

// stubLimiter defers the first reservation.
type stubLimiter struct {
	mu       sync.Mutex
	deferred bool
	settled  int
}

func (s *stubLimiter) Reserve(context.Context, string, int) (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.deferred {
		s.deferred = true
		return 50 * time.Millisecond, nil
	}
	return 0, nil
}
func (s *stubLimiter) Settle(_ context.Context, _ string, d int) error {
	s.mu.Lock()
	s.settled += d
	s.mu.Unlock()
	return nil
}

func TestRateLimiterDefersWithoutBlockingWorkers(t *testing.T) {
	h := newHarness(t)
	lim := &stubLimiter{}
	h.deps.Limiter = lim
	h.start()
	run := h.create(core.AgentSpec{Model: "mock"}, "hi", 0)
	done := h.waitStatus(run.ID, core.RunCompleted)
	if done.Claims != 2 {
		t.Errorf("claims = %d, want 2 (deferred once)", done.Claims)
	}
	if v := h.engine.m.rateLimited.Value("mock"); v != 1 {
		t.Errorf("rate_limited metric = %v", v)
	}
	if steps := h.steps(run.ID); steps[0].Attempts != 1 {
		t.Errorf("a deferred call must not count as an attempt: %d", steps[0].Attempts)
	}
}

func TestShutdownHandsRunsBack(t *testing.T) {
	h := newHarness(t)
	h.mock.Latency = 2 * time.Second
	h.start()
	run := h.create(core.AgentSpec{Model: "mock"}, "slow", 0)
	h.waitStatus(run.ID, core.RunRunning)
	h.shutdown()
	got, _ := h.store.GetRun(context.Background(), run.ID)
	if got.Status != core.RunQueued || got.LeaseOwner != "" {
		t.Errorf("after shutdown: status %s owner %q, want queued and unowned", got.Status, got.LeaseOwner)
	}
}

// inventor makes the first-turn tool calls (default: a tool that does not
// exist), then answers with the last tool result.
type inventor struct {
	calls atomic.Int32
	first []provider.ToolCall
}

func (i *inventor) Name() string { return "inventor" }
func (i *inventor) Stream(context.Context, provider.Request, provider.DeltaFunc) (provider.Response, error) {
	return provider.Response{}, nil
}
func (i *inventor) Complete(_ context.Context, r provider.Request) (provider.Response, error) {
	if i.calls.Add(1) == 1 {
		if i.first == nil {
			i.first = []provider.ToolCall{{ID: "x", Name: "launch_rockets", Input: json.RawMessage(`{}`)}}
		}
		return provider.Response{FinishReason: "tool_calls", ToolCalls: i.first}, nil
	}
	last := r.Messages[len(r.Messages)-1]
	return provider.Response{FinishReason: "stop", Content: "recovered: " + last.Content}, nil
}

func TestModelInventsATool(t *testing.T) {
	h := newHarness(t, provider.Route{Prefix: "inventor", Provider: &inventor{}})
	h.start()
	run := h.create(core.AgentSpec{Model: "inventor", Tools: []core.ToolSpec{{Type: core.ToolMCP, URL: h.shopMC, Include: []string{"get_order"}}}}, "x", 0)
	done := h.waitStatus(run.ID, core.RunCompleted)
	if !strings.Contains(done.Output, "Unknown tool") || !strings.Contains(done.Output, "get_order") {
		t.Errorf("output = %q", done.Output)
	}
}

func TestToolCallLimit(t *testing.T) {
	order := json.RawMessage(`{"order_id":"ORD-1001"}`)
	h := newHarness(t, provider.Route{Prefix: "inventor", Provider: &inventor{first: []provider.ToolCall{
		{ID: "a", Name: "get_order", Input: order}, {ID: "b", Name: "get_order", Input: order},
	}}})
	h.start()
	run := h.create(core.AgentSpec{Model: "inventor", Limits: map[string]int{"get_order": 1},
		Tools: []core.ToolSpec{{Type: core.ToolMCP, URL: h.shopMC, Include: []string{"get_order"}}}}, "x", 0)
	done := h.waitStatus(run.ID, core.RunCompleted)
	if !strings.Contains(done.Output, "at most 1 time") {
		t.Errorf("output = %q", done.Output)
	}
	steps, _ := h.store.ListSteps(context.Background(), run.ID)
	var made, refused int
	for _, s := range steps {
		if s.Kind == core.StepTool && s.IsError {
			refused++
		} else if s.Kind == core.StepTool {
			made++
		}
	}
	if made != 1 || refused != 1 {
		t.Errorf("tool steps: made %d, refused %d; want 1 and 1", made, refused)
	}
}

func TestResponseSchema(t *testing.T) {
	h := newHarness(t, provider.Route{Prefix: "inventor", Provider: &inventor{}})
	h.start()
	schema := json.RawMessage(`{"type":"object","properties":{"order_id":{"type":"string"}}}`)
	ok := h.create(core.AgentSpec{Model: "mock", ResponseSchema: schema}, "order_id: ORD-7", 0)
	if out := h.waitStatus(ok.ID, core.RunCompleted).Output; out != `{"order_id":"ORD-7"}` {
		t.Errorf("output = %q", out)
	}
	// inventor ignores the schema and answers in prose: callers must not get it as "JSON".
	bad := h.create(core.AgentSpec{Model: "inventor", ResponseSchema: schema}, "x", 0)
	if r := h.waitStatus(bad.ID, core.RunFailed); !strings.Contains(r.Error, "not valid JSON") {
		t.Errorf("error = %q", r.Error)
	}
}

func TestTriageToolsOnlyTouchTheirOwnRun(t *testing.T) {
	h := newHarness(t)
	e := New(h.cfg, h.deps)
	ctx := context.Background()
	victim, _, _ := h.store.CreateRun(ctx, core.NewRun{Spec: core.AgentSpec{Model: "mock"}, Input: "x"})
	meta, _ := json.Marshal(map[string]string{"triage_of": "some-other-run"})
	tr, _, _ := h.store.CreateRun(ctx, core.NewRun{Spec: core.AgentSpec{Model: "mock"}, Input: "x", Metadata: meta})

	res, err := e.tools.Builtins[toolRetryRun](ctx, tools.Call{RunID: tr.ID, Input: json.RawMessage(`{"run_id":"` + victim.ID + `"}`)})
	if err != nil || !res.IsError {
		t.Errorf("a triage run acted on an unrelated run: %+v %v", res, err)
	}
	_ = mcp.ProtocolVersion
}
