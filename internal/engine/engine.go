// Package engine runs agents durably. Workers claim runs from the store,
// replay saved steps to rebuild state, and execute one step at a time:
// model calls, tool calls, approvals and sub-agents. Every step is saved
// before the next one starts, so a crash loses at most the step in flight.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valayash/stepwise/internal/breaker"
	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/metrics"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/ratelimit"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/telemetry"
	"github.com/valayash/stepwise/internal/tools"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Config tunes the engine.
type Config struct {
	WorkerID      string        // unique per process; defaults to host-pid-random
	Workers       int           // concurrent runs per process (default 8)
	Lease         time.Duration // how long a claim lasts without a heartbeat (default 30s)
	PollInterval  time.Duration // idle polling, on top of change notifications (default 1s)
	LLMTimeout    time.Duration // one model call (default 2m)
	MaxAttempts   int           // tries per step before giving up (default 6)
	BackoffBase   time.Duration // first retry delay (default 1s)
	BackoffMax    time.Duration // retry delay cap (default 2m)
	StepsPerClaim int           // steps before yielding to other runs (default 100)

	Triage TriageConfig
}

func (c *Config) defaults() {
	if c.WorkerID == "" {
		host, _ := os.Hostname()
		c.WorkerID = fmt.Sprintf("%s-%d-%04x", host, os.Getpid(), rand.IntN(1<<16))
	}
	orDefault(&c.Workers, 8)
	orDefault(&c.Lease, 30*time.Second)
	orDefault(&c.PollInterval, time.Second)
	orDefault(&c.LLMTimeout, 2*time.Minute)
	orDefault(&c.MaxAttempts, 6)
	orDefault(&c.BackoffBase, time.Second)
	orDefault(&c.BackoffMax, 2*time.Minute)
	orDefault(&c.StepsPerClaim, 100)
}

// orDefault sets *v to def when it is unset (zero or negative).
func orDefault[T int | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

// Engine executes runs.
type Engine struct {
	cfg      Config
	store    store.Store
	router   *provider.Router
	tools    *tools.Runner
	limiter  ratelimit.Limiter
	breakers *breaker.Set
	log      *slog.Logger
	m        *Metrics
	now      func() time.Time
}

// Deps are the engine's collaborators. Limiter, Breakers, Logger and Metrics are optional.
type Deps struct {
	Store    store.Store
	Router   *provider.Router
	Tools    *tools.Runner
	Limiter  ratelimit.Limiter
	Breakers *breaker.Set
	Logger   *slog.Logger
	Metrics  *Metrics
}

// New builds an engine and registers its built-in tools on deps.Tools.
func New(cfg Config, d Deps) *Engine {
	cfg.defaults()
	if d.Limiter == nil {
		d.Limiter = ratelimit.Unlimited{}
	}
	if d.Breakers == nil {
		d.Breakers = breaker.NewSet(5, 30*time.Second, nil)
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Metrics == nil {
		d.Metrics = NewMetrics(metrics.NewRegistry(), d.Breakers)
	}
	if d.Tools == nil {
		d.Tools = tools.NewRunner()
	}
	e := &Engine{
		cfg: cfg, store: d.Store, router: d.Router, tools: d.Tools, limiter: d.Limiter,
		breakers: d.Breakers, log: d.Logger.With("worker", cfg.WorkerID), m: d.Metrics, now: time.Now,
	}
	e.registerTriageTools()
	return e
}

// WorkerID returns this engine's lease owner name.
func (e *Engine) WorkerID() string { return e.cfg.WorkerID }

// Run starts the workers and blocks until ctx is cancelled. Runs in flight
// at shutdown are handed back to the queue so another worker can continue.
func (e *Engine) Run(ctx context.Context) {
	events, stop := e.store.Subscribe()
	defer stop()

	wake := make(chan struct{}, e.cfg.Workers)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				if ev.Type == store.EventRunsReady {
					select {
					case wake <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	go func() {
		t := time.NewTicker(30 * e.cfg.PollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				e.expireApprovals(ctx)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < e.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.workerLoop(ctx, wake)
		}()
	}
	e.log.Info("engine started", "workers", e.cfg.Workers, "lease", e.cfg.Lease)
	wg.Wait()
	e.log.Info("engine stopped")
}

// expireApprovals rejects tool calls that waited longer than their agent's
// approval_timeout. Every worker process sweeps; a call already decided by
// another one is simply skipped.
func (e *Engine) expireApprovals(ctx context.Context) {
	steps, err := e.store.PendingApprovals(ctx)
	if err != nil {
		return
	}
	ttl := map[string]time.Duration{}
	for _, st := range steps {
		d, ok := ttl[st.RunID]
		if !ok {
			if run, err := e.store.GetRun(ctx, st.RunID); err == nil {
				d = run.Spec.ApprovalTTL()
			}
			ttl[st.RunID] = d
		}
		if d <= 0 || e.now().Sub(st.CreatedAt) < d {
			continue
		}
		note := fmt.Sprintf("No decision within %s; rejected automatically.", d)
		if e.store.DecideApproval(ctx, st.RunID, st.ID, core.Approval{Note: note, DecidedBy: "stepwise"}) == nil {
			e.m.approvals.Inc("expired")
		}
	}
}

func (e *Engine) workerLoop(ctx context.Context, wake <-chan struct{}) {
	for ctx.Err() == nil {
		runs, err := e.store.ClaimRuns(ctx, e.cfg.WorkerID, 1, e.cfg.Lease)
		if err != nil {
			if ctx.Err() == nil {
				e.log.Warn("claim failed", "err", err)
			}
			sleep(ctx, e.cfg.PollInterval)
			continue
		}
		if len(runs) == 0 {
			jitter := time.Duration(rand.Int64N(int64(e.cfg.PollInterval)/2 + 1))
			select {
			case <-ctx.Done():
			case <-wake:
			case <-time.After(e.cfg.PollInterval + jitter):
			}
			continue
		}
		e.m.claims.Inc()
		e.Process(ctx, runs[0])
	}
}

// errStop ends processing of the current claim: the run was handed back,
// finished, or is no longer ours.
var errStop = errors.New("stop processing")

// Process drives one claimed run until it finishes, waits, or is handed back.
// Exported so tests and the chaos harness can drive runs step by step.
func (e *Engine) Process(ctx context.Context, run core.Run) {
	f := core.Fence{RunID: run.ID, Token: run.LeaseToken}
	// One span per claim, inside the run's trace (trace ID = run ID).
	ctx, span := telemetry.Tracer().Start(telemetry.RunContext(ctx, run.ID), "run.claim", trace.WithAttributes(
		attribute.String("stepwise.run.id", run.ID),
		attribute.Int("stepwise.run.claim", run.Claims),
		attribute.String("stepwise.worker.id", e.cfg.WorkerID),
		attribute.String("gen_ai.request.model", run.Spec.Model),
	))
	defer span.End()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	log := e.log.With("run_id", run.ID, "claim", run.Claims)

	// Heartbeat: keep the lease alive; stop everything if it is lost.
	var lost bool
	var lostMu sync.Mutex
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(e.cfg.Lease / 3)
		defer t.Stop()
		failingSince := time.Time{}
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				err := e.store.Heartbeat(runCtx, f, e.cfg.Lease)
				switch {
				case err == nil:
					failingSince = time.Time{}
				case errors.Is(err, core.ErrFenced) || errors.Is(err, core.ErrNotFound):
					lostMu.Lock()
					lost = true
					lostMu.Unlock()
					log.Warn("lease lost; abandoning run")
					cancel()
					return
				default:
					if failingSince.IsZero() {
						failingSince = e.now()
					} else if e.now().Sub(failingSince) > e.cfg.Lease {
						log.Error("heartbeat failing for a full lease; abandoning run", "err", err)
						cancel()
						return
					}
				}
			}
		}
	}()

	x := &execution{e: e, run: run, f: f, log: log}
	err := x.loop(runCtx)
	cancel()
	<-hbDone

	lostMu.Lock()
	wasLost := lost
	lostMu.Unlock()
	switch {
	case errors.Is(err, core.ErrFenced) || wasLost:
		e.m.leaseLost.Inc()
		span.SetAttributes(attribute.String("stepwise.claim.outcome", "lease_lost"))
	case ctx.Err() != nil && !errors.Is(err, errStop):
		span.SetAttributes(attribute.String("stepwise.claim.outcome", "shutdown"))
		// Shutting down mid-run: hand it back so another worker resumes it now.
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		if yerr := e.store.Yield(rctx, f, store.Yield{Status: core.RunQueued, AvailableAt: e.now(), Attempts: x.run.Attempts}); yerr != nil && !errors.Is(yerr, core.ErrFenced) {
			log.Warn("could not release run on shutdown", "err", yerr)
		}
	case err != nil && !errors.Is(err, errStop):
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Error("run processing error", "err", err)
		e.m.retries.Inc("store_error")
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		_ = e.store.Yield(rctx, f, store.Yield{
			Status: core.RunQueued, AvailableAt: e.now().Add(breaker.Backoff(x.run.Attempts+1, e.cfg.BackoffBase, e.cfg.BackoffMax)),
			Attempts: x.run.Attempts, Error: err.Error(),
		})
	}
}

// execution is one claim of one run.
type execution struct {
	e     *Engine
	run   core.Run
	f     core.Fence
	steps []core.Step
	log   *slog.Logger
}

func (x *execution) loop(ctx context.Context) error {
	for i := 0; i < x.e.cfg.StepsPerClaim; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		steps, err := x.e.store.ListSteps(ctx, x.run.ID)
		if err != nil {
			return err
		}
		x.steps = steps
		a := decide(x.run, steps)
		if err := x.do(ctx, a); err != nil {
			return err
		}
	}
	// Fairness: let other runs have a worker; this one continues right away.
	return x.yield(ctx, store.Yield{Status: core.RunQueued, AvailableAt: x.e.now(), Attempts: x.run.Attempts})
}

func (x *execution) do(ctx context.Context, a action) error {
	switch a.kind {
	case actCallLLM:
		return x.callLLM(ctx, a.seq)
	case actStartTool:
		return x.startTool(ctx, a.seq, *a.call)
	case actExecTool:
		return x.execTool(ctx, *a.step)
	case actComplete:
		if len(x.run.Spec.ResponseSchema) > 0 && !json.Valid([]byte(a.output)) {
			// A refusal or a max_tokens cut can still break the schema; callers rely on JSON.
			return x.finish(ctx, store.Outcome{Status: core.RunFailed, FailureKind: core.FailFatal, Error: "final answer is not valid JSON for response_schema"})
		}
		return x.finish(ctx, store.Outcome{Status: core.RunCompleted, Output: a.output})
	case actFail:
		return x.finish(ctx, store.Outcome{Status: core.RunFailed, Error: a.reason, FailureKind: a.fail})
	case actWait:
		return x.yield(ctx, store.Yield{Status: core.RunWaiting})
	}
	return fmt.Errorf("unknown action %d", a.kind)
}

// ---- model calls ----

func (x *execution) callLLM(ctx context.Context, seq int) error {
	spec, model := x.run.Spec, x.run.Spec.Model
	p, err := x.e.router.For(model)
	if err != nil {
		return x.finish(ctx, store.Outcome{Status: core.RunFailed, Error: err.Error(), FailureKind: core.FailFatal})
	}

	remaining := x.run.BudgetTokens - x.run.TokensUsed
	maxTokens := min(spec.MaxTokensPerCall, remaining)
	msgs := conversation(x.run, x.steps)
	toolDefs := spec.ToolDefs()

	br := x.e.breakers.Get(p.Name())
	estimate, err := x.admit(ctx, br, p.Name(), model, ratelimit.EstimateTokens(promptChars(msgs, toolDefs), maxTokens))
	if err != nil {
		return err
	}

	step, err := x.e.store.BeginStep(ctx, x.f, core.Step{Seq: seq, Kind: core.StepLLM})
	if err != nil {
		br.Abort()
		_ = x.e.limiter.Settle(context.WithoutCancel(ctx), model, -estimate)
		return err
	}
	if step.Attempts > 1 {
		x.e.m.replayed.Inc("llm")
	}

	spanCtx, span := telemetry.Tracer().Start(ctx, "chat "+model, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String("gen_ai.system", p.Name()),
		attribute.String("gen_ai.request.model", model),
		attribute.Int("gen_ai.request.max_tokens", maxTokens),
		attribute.Int("stepwise.step.seq", step.Seq),
		attribute.Int("stepwise.step.attempt", step.Attempts),
	))
	callCtx, cancel := context.WithTimeout(spanCtx, x.e.cfg.LLMTimeout)
	start := x.e.now()
	resp, err := p.Complete(callCtx, provider.Request{
		Model: model, Messages: msgs, Tools: toolDefs, MaxTokens: maxTokens, Temperature: spec.Temperature,
		ResponseSchema: spec.ResponseSchema,
	})
	cancel()
	elapsed := x.e.now().Sub(start)
	x.e.m.llmSeconds.Observe(elapsed.Seconds(), p.Name())
	endSpan(span, err,
		attribute.String("gen_ai.response.id", resp.ID),
		attribute.StringSlice("gen_ai.response.finish_reasons", []string{resp.FinishReason}),
		attribute.Int("gen_ai.usage.input_tokens", resp.InputTokens),
		attribute.Int("gen_ai.usage.cache_read_input_tokens", resp.CachedTokens),
		attribute.Int("gen_ai.usage.output_tokens", resp.OutputTokens),
		attribute.Int("stepwise.tool_calls", len(resp.ToolCalls)),
	)

	if err != nil {
		_ = x.e.limiter.Settle(context.WithoutCancel(ctx), model, -estimate)
		if ctx.Err() != nil {
			br.Abort()
			return ctx.Err() // lease lost or shutting down: someone else will redo this step
		}
		if !provider.Retryable(err) {
			br.Success() // the provider answered; it is up
			x.e.m.llmCalls.Inc(p.Name(), "error")
			return x.finish(ctx, store.Outcome{Status: core.RunFailed, Error: err.Error(), FailureKind: core.FailFatal})
		}
		br.Failure()
		x.e.m.llmCalls.Inc(p.Name(), "retry")
		return x.retryLater(ctx, err, retryAfter(err))
	}

	br.Success()
	used := resp.InputTokens + resp.OutputTokens
	_ = x.e.limiter.Settle(context.WithoutCancel(ctx), model, used-estimate)
	x.e.m.llmCalls.Inc(p.Name(), "ok")
	x.e.m.tokens.Add(float64(resp.InputTokens), model, "input")
	x.e.m.tokens.Add(float64(resp.OutputTokens), model, "output")
	x.e.m.tokens.Add(float64(resp.CachedTokens), model, "cached_input") // part of input; watch it to confirm caching works

	step.Status = core.StepCompleted
	step.LLM = &resp
	step.DurationMs = elapsed.Milliseconds()
	if err := x.e.store.SaveStep(ctx, x.f, step); err != nil {
		return err
	}
	x.run.TokensUsed += used
	x.run.Attempts = 0
	x.e.m.steps.Inc(string(core.StepLLM))
	return nil
}

// admit asks the circuit breaker (cheap, local), then the shared rate
// limiter, for permission to call the model. If either says "not now", the
// run goes back to the queue and admit returns errStop. It returns the token
// estimate actually reserved, which the caller settles after the call.
func (x *execution) admit(ctx context.Context, br *breaker.Breaker, system, model string, estimate int) (int, error) {
	if ok, wait := br.Allow(); !ok {
		x.e.m.breakerRejected.Inc(system)
		trace.SpanFromContext(ctx).AddEvent("circuit_open", trace.WithAttributes(attribute.String("gen_ai.system", system)))
		return 0, x.yield(ctx, store.Yield{Status: core.RunQueued, AvailableAt: x.e.now().Add(wait), Attempts: x.run.Attempts, Error: system + " circuit open"})
	}
	wait, err := x.e.limiter.Reserve(ctx, model, estimate)
	if err != nil {
		// Fail open: a limiter outage should not stop every agent. Providers
		// still enforce their own limits, which we handle as retries.
		x.log.Warn("rate limiter unavailable; proceeding", "err", err)
		return 0, nil
	}
	if wait > 0 {
		br.Abort()
		x.e.m.rateLimited.Inc(model)
		trace.SpanFromContext(ctx).AddEvent("rate_limited", trace.WithAttributes(
			attribute.String("gen_ai.request.model", model), attribute.Int64("stepwise.wait_ms", wait.Milliseconds())))
		return 0, x.yield(ctx, store.Yield{Status: core.RunQueued, AvailableAt: x.e.now().Add(wait), Attempts: x.run.Attempts})
	}
	return estimate, nil
}

// retryLater hands the run back with exponential backoff, or fails it once
// MaxAttempts is reached.
func (x *execution) retryLater(ctx context.Context, cause error, hint time.Duration) error {
	attempts := x.run.Attempts + 1
	if attempts >= x.e.cfg.MaxAttempts {
		x.e.m.retries.Inc("exhausted")
		return x.finish(ctx, store.Outcome{
			Status: core.RunFailed, FailureKind: core.FailExhausted,
			Error: fmt.Sprintf("gave up after %d attempts: %v", attempts, cause),
		})
	}
	delay := breaker.Backoff(attempts, x.e.cfg.BackoffBase, x.e.cfg.BackoffMax)
	if hint > delay {
		delay = hint
	}
	x.e.m.retries.Inc("transient")
	x.log.Info("transient failure; retrying later", "attempt", attempts, "delay", delay, "err", cause)
	return x.yield(ctx, store.Yield{
		Status: core.RunQueued, AvailableAt: x.e.now().Add(delay), Attempts: attempts, Error: cause.Error(),
	})
}

// ---- tool calls ----

func (x *execution) startTool(ctx context.Context, seq int, tc provider.ToolCall) error {
	base := core.Step{Seq: seq, Kind: core.StepTool, ToolCallID: tc.ID, ToolName: tc.Name, ToolInput: tc.Input}

	spec, ok := x.run.Spec.Tool(tc.Name)
	if !ok {
		// The model invented a tool. Tell it, and let it recover.
		return x.refuseTool(ctx, base, fmt.Sprintf("Unknown tool %q. Available tools: %s.", tc.Name, toolNames(x.run.Spec)))
	}
	if limit, ok := x.run.Spec.Limits[tc.Name]; ok && x.toolCalls(tc.Name) >= limit {
		x.e.m.toolCalls.Inc(string(spec.Type), "limited")
		return x.refuseTool(ctx, base, fmt.Sprintf("Tool %s may be called at most %d time(s) per run; this call was not made.", tc.Name, limit))
	}

	if x.run.Spec.NeedsApproval(tc.Name) {
		base.Status = core.StepPendingApproval
		if _, err := x.e.store.BeginStep(ctx, x.f, base); err != nil {
			return err
		}
		x.e.m.approvals.Inc("requested")
		x.log.Info("tool call needs approval", "tool", tc.Name)
		return nil // decide() starts any other calls, then waits
	}

	st, err := x.e.store.BeginStep(ctx, x.f, base)
	if err != nil {
		return err
	}
	return x.runTool(ctx, st, spec)
}

// refuseTool records a tool call that was not made, as an error the model sees.
func (x *execution) refuseTool(ctx context.Context, base core.Step, msg string) error {
	st, err := x.e.store.BeginStep(ctx, x.f, base)
	if err != nil {
		return err
	}
	st.Status, st.IsError, st.Result = core.StepCompleted, true, msg
	return x.e.store.SaveStep(ctx, x.f, st)
}

// toolCalls counts this run's earlier calls of a tool (made, pending or refused).
func (x *execution) toolCalls(name string) int {
	n := 0
	for _, s := range x.steps {
		if s.Kind == core.StepTool && s.ToolName == name {
			n++
		}
	}
	return n
}

func (x *execution) execTool(ctx context.Context, st core.Step) error {
	spec, ok := x.run.Spec.Tool(st.ToolName)
	if !ok {
		return fmt.Errorf("step %d references unknown tool %q", st.Seq, st.ToolName)
	}
	if st.Status == core.StepApproved {
		x.e.m.approvals.Inc("approved")
	}
	// Re-begin: approved → running, or count another attempt of an interrupted step.
	st, err := x.e.store.BeginStep(ctx, x.f, core.Step{Seq: st.Seq, Kind: core.StepTool})
	if err != nil {
		return err
	}
	if st.Attempts > 1 {
		x.e.m.replayed.Inc("tool")
	}
	return x.runTool(ctx, st, spec)
}

func (x *execution) runTool(ctx context.Context, st core.Step, spec core.ToolSpec) error {
	if spec.Type == core.ToolAgent {
		return x.spawnChild(ctx, st, spec)
	}

	spanCtx, span := telemetry.Tracer().Start(ctx, "execute_tool "+spec.Name, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "execute_tool"),
		attribute.String("gen_ai.tool.name", spec.Name),
		attribute.String("gen_ai.tool.call.id", st.ToolCallID),
		attribute.String("stepwise.tool.type", string(spec.Type)),
		attribute.Int("stepwise.step.seq", st.Seq),
		attribute.Int("stepwise.step.attempt", st.Attempts),
	))
	start := x.e.now()
	res, err := x.e.tools.Run(spanCtx, tools.Call{
		RunID: x.run.ID, Spec: spec, Input: st.ToolInput, IdempotencyKey: st.IdempotencyKey(),
	})
	elapsed := x.e.now().Sub(start)
	x.e.m.toolSeconds.Observe(elapsed.Seconds(), string(spec.Type))
	endSpan(span, err, attribute.Bool("stepwise.tool.is_error", res.IsError))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		x.e.m.toolCalls.Inc(string(spec.Type), "retry")
		if st.Attempts >= x.e.cfg.MaxAttempts {
			// The tool stays broken. Tell the model instead of failing the
			// whole run: agents can often work around a missing tool.
			res = tools.Result{IsError: true, Text: fmt.Sprintf("Tool %s failed after %d attempts: %v", spec.Name, st.Attempts, err)}
		} else {
			return x.retryLater(ctx, err, 0)
		}
	}

	outcome := "ok"
	if res.IsError {
		outcome = "tool_error"
	}
	x.e.m.toolCalls.Inc(string(spec.Type), outcome)
	st.Status = core.StepCompleted
	st.Result, st.IsError = res.Text, res.IsError
	st.DurationMs = elapsed.Milliseconds()
	if err := x.e.store.SaveStep(ctx, x.f, st); err != nil {
		return err
	}
	x.run.Attempts = 0
	x.e.m.steps.Inc(string(core.StepTool))
	return nil
}

func (x *execution) spawnChild(ctx context.Context, st core.Step, spec core.ToolSpec) error {
	var in struct {
		Task string `json:"task"`
	}
	_ = json.Unmarshal(st.ToolInput, &in)
	task := in.Task
	if task == "" {
		task = string(st.ToolInput)
	}
	budget := x.run.BudgetTokens - x.run.TokensUsed
	if budget <= 0 {
		budget = 1
	}
	meta, _ := json.Marshal(map[string]string{"parent_tool": spec.Name})
	_, child, err := x.e.store.SpawnChild(ctx, x.f, st, core.NewRun{
		Spec: *spec.Agent, Input: task, BudgetTokens: budget, Metadata: meta,
	})
	if err != nil {
		return err
	}
	x.e.m.children.Inc()
	x.log.Info("sub-agent started", "tool", spec.Name, "child_run_id", child.ID)
	return nil
}

// ---- endings ----

func (x *execution) yield(ctx context.Context, y store.Yield) error {
	if err := x.e.store.Yield(ctx, x.f, y); err != nil {
		return err
	}
	return errStop
}

func (x *execution) finish(ctx context.Context, o store.Outcome) error {
	if err := x.e.store.Finish(ctx, x.f, o); err != nil {
		return err
	}
	x.e.m.finished.Inc(string(o.Status))
	x.log.Info("run finished", "status", o.Status, "failure_kind", o.FailureKind, "tokens", x.run.TokensUsed)
	if o.Status == core.RunFailed {
		x.e.maybeTriage(context.WithoutCancel(ctx), x.run, o)
	}
	return errStop
}

// ---- helpers ----

// endSpan records err on span, or the success attributes when there is none.
func endSpan(span trace.Span, err error, ok ...attribute.KeyValue) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(ok...)
	}
	span.End()
}

func promptChars(msgs []provider.Message, defs []provider.ToolDef) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Content)
		for _, tc := range m.ToolCalls {
			n += len(tc.Name) + len(tc.Input)
		}
	}
	for _, d := range defs {
		n += len(d.Name) + len(d.Description) + len(d.InputSchema)
	}
	return n
}

// retryAfter reads a provider's Retry-After hint (seconds form).
func retryAfter(err error) time.Duration {
	var ue *provider.UpstreamError
	if errors.As(err, &ue) && ue.RetryAfter != "" {
		if s, perr := strconv.ParseFloat(strings.TrimSpace(ue.RetryAfter), 64); perr == nil && s > 0 {
			return time.Duration(s * float64(time.Second))
		}
	}
	return 0
}

func toolNames(spec core.AgentSpec) string {
	names := make([]string, len(spec.Tools))
	for i, t := range spec.Tools {
		names[i] = t.Name
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
