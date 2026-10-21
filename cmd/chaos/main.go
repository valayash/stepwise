// Command chaos hammers Stepwise with crashes and failures, then proves that
// no run was lost and no side effect happened twice.
//
// It starts several worker "processes" sharing one store, runs many refund
// agents through them (each with a human-approval step), injects tool and
// model failures, and repeatedly kills workers at random. A killed worker
// gets no graceful shutdown: from the instant of the kill every store call it
// makes fails, its in-flight calls are cut off, and its leases must expire
// before anyone else can continue its runs — the same as `kill -9`.
//
//	go run ./cmd/chaos -runs 500 -workers 6 -kill-every 250ms
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/demo"
	"github.com/valayash/stepwise/internal/engine"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/store/memory"
	"github.com/valayash/stepwise/internal/tools"
)

func main() {
	var (
		runs       = flag.Int("runs", 300, "agent runs to execute")
		workers    = flag.Int("workers", 6, "worker processes alive at any time")
		perWorker  = flag.Int("concurrency", 4, "concurrent runs per worker process")
		killEvery  = flag.Duration("kill-every", 250*time.Millisecond, "average time between kills")
		lease      = flag.Duration("lease", 400*time.Millisecond, "lease length (short, so takeovers happen fast)")
		toolFaults = flag.Float64("tool-fault-rate", 0.05, "fraction of tool calls failing with HTTP 503")
		llmFaults  = flag.Float64("llm-fault-rate", 0.05, "fraction of model calls failing with 503")
		latency    = flag.Duration("latency", 15*time.Millisecond, "model and tool latency")
		timeout    = flag.Duration("timeout", 5*time.Minute, "give up after")
		out        = flag.String("out", "", "also write the report to this file")
	)
	flag.Parse()

	rep, err := run(config{*runs, *workers, *perWorker, *killEvery, *lease, *toolFaults, *llmFaults, *latency, *timeout})
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos:", err)
	}
	text := rep.markdown()
	fmt.Println(text)
	if *out != "" {
		_ = os.WriteFile(*out, []byte(text), 0o644)
	}
	if err != nil || !rep.passed() {
		os.Exit(1)
	}
}

type config struct {
	runs, workers, perWorker int
	killEvery, lease         time.Duration
	toolFaults, llmFaults    float64
	latency, timeout         time.Duration
}

// ---- a store connection that dies like a killed process ----

var errKilled = errors.New("worker process killed")

type killable struct {
	store.Store
	dead atomic.Bool
}

func (k *killable) check() error {
	if k.dead.Load() {
		return errKilled
	}
	return nil
}

func (k *killable) ClaimRuns(ctx context.Context, owner string, n int, lease time.Duration) ([]core.Run, error) {
	if err := k.check(); err != nil {
		return nil, err
	}
	runs, err := k.Store.ClaimRuns(ctx, owner, n, lease)
	if k.dead.Load() {
		return nil, errKilled // claimed at the moment of death: the lease simply expires
	}
	return runs, err
}
func (k *killable) Heartbeat(ctx context.Context, f core.Fence, l time.Duration) error {
	if err := k.check(); err != nil {
		return err
	}
	return k.Store.Heartbeat(ctx, f, l)
}
func (k *killable) BeginStep(ctx context.Context, f core.Fence, s core.Step) (core.Step, error) {
	if err := k.check(); err != nil {
		return core.Step{}, err
	}
	return k.Store.BeginStep(ctx, f, s)
}
func (k *killable) SaveStep(ctx context.Context, f core.Fence, s core.Step) error {
	if err := k.check(); err != nil {
		return err
	}
	return k.Store.SaveStep(ctx, f, s)
}
func (k *killable) SpawnChild(ctx context.Context, f core.Fence, s core.Step, n core.NewRun) (core.Step, core.Run, error) {
	if err := k.check(); err != nil {
		return core.Step{}, core.Run{}, err
	}
	return k.Store.SpawnChild(ctx, f, s, n)
}
func (k *killable) Yield(ctx context.Context, f core.Fence, y store.Yield) error {
	if err := k.check(); err != nil {
		return err
	}
	return k.Store.Yield(ctx, f, y)
}
func (k *killable) Finish(ctx context.Context, f core.Fence, o store.Outcome) error {
	if err := k.check(); err != nil {
		return err
	}
	return k.Store.Finish(ctx, f, o)
}
func (k *killable) ListSteps(ctx context.Context, id string) ([]core.Step, error) {
	if err := k.check(); err != nil {
		return nil, err
	}
	return k.Store.ListSteps(ctx, id)
}
func (k *killable) GetRun(ctx context.Context, id string) (core.Run, error) {
	if err := k.check(); err != nil {
		return core.Run{}, err
	}
	return k.Store.GetRun(ctx, id)
}

// ---- report ----

type report struct {
	cfg                  config
	duration             time.Duration
	kills, spawned       int
	completed, failed    int
	other                map[core.RunStatus]int
	claims, takeovers    int
	steps                int
	llmReexec, toolReexc int
	refunds, duplicates  int
	ordersRefundedTwice  int
	toolFaults           int
	gaps                 []string
	failures             []string
}

func (r *report) passed() bool {
	return r.completed == r.cfg.runs && r.ordersRefundedTwice == 0 && r.refunds == r.cfg.runs && len(r.gaps) == 0
}

func (r *report) markdown() string {
	verdict := "PASS"
	if !r.passed() {
		verdict = "FAIL"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "## Chaos test: %s\n\n", verdict)
	fmt.Fprintf(&sb, "%d refund-agent runs (7 steps each, one human approval) on %d worker processes × %d concurrent runs, "+
		"%.0f%% injected tool failures, %.0f%% injected model failures, a worker killed every ~%s with no graceful shutdown.\n\n",
		r.cfg.runs, r.cfg.workers, r.cfg.perWorker, r.cfg.toolFaults*100, r.cfg.llmFaults*100, r.cfg.killEvery)
	fmt.Fprintf(&sb, "| Check | Result |\n| --- | --- |\n")
	fmt.Fprintf(&sb, "| Runs completed | **%d / %d** |\n", r.completed, r.cfg.runs)
	fmt.Fprintf(&sb, "| Runs lost or stuck | **%d** |\n", r.cfg.runs-r.completed-r.failed)
	fmt.Fprintf(&sb, "| Runs failed | %d |\n", r.failed)
	fmt.Fprintf(&sb, "| Refunds issued | **%d** (one per run) |\n", r.refunds)
	fmt.Fprintf(&sb, "| Orders refunded twice | **%d** |\n", r.ordersRefundedTwice)
	fmt.Fprintf(&sb, "| Repeated refund calls absorbed by idempotency keys | %d |\n", r.duplicates)
	fmt.Fprintf(&sb, "| Step histories with gaps or duplicates | **%d** |\n", len(r.gaps))
	fmt.Fprintf(&sb, "| Workers killed | %d |\n", r.kills)
	fmt.Fprintf(&sb, "| Runs taken over from a dead or yielding worker (claims beyond the first) | %d |\n", r.takeovers)
	fmt.Fprintf(&sb, "| Steps executed in total | %d |\n", r.steps)
	fmt.Fprintf(&sb, "| Model calls re-executed after a crash or transient failure | %d (%.1f%% of model steps) |\n",
		r.llmReexec, pct(r.llmReexec, r.cfg.runs*4))
	fmt.Fprintf(&sb, "| Tool calls re-executed after a crash or transient failure | %d |\n", r.toolReexc)
	fmt.Fprintf(&sb, "| Injected tool faults | %d |\n", r.toolFaults)
	fmt.Fprintf(&sb, "| Wall time | %s (%.0f steps/s) |\n", r.duration.Round(time.Millisecond), float64(r.steps)/r.duration.Seconds())
	if len(r.failures) > 0 {
		fmt.Fprintf(&sb, "\nFailures:\n")
		for _, f := range r.failures[:min(5, len(r.failures))] {
			fmt.Fprintf(&sb, "- %s\n", f)
		}
	}
	if len(r.gaps) > 0 {
		fmt.Fprintf(&sb, "\nGaps:\n")
		for _, g := range r.gaps[:min(5, len(r.gaps))] {
			fmt.Fprintf(&sb, "- %s\n", g)
		}
	}
	fmt.Fprintf(&sb, "\nRe-executed model calls are the cost of a crash: a call whose answer was not saved yet runs again. "+
		"Completed steps are never repeated, and side effects stay exactly-once because tools receive a stable idempotency key.\n")
	return sb.String()
}

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

// ---- the experiment ----

func run(cfg config) (*report, error) {
	rep := &report{cfg: cfg, other: map[core.RunStatus]int{}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	// Shared infrastructure: the store, the shop, the model.
	base := memory.New(nil)
	shop := demo.NewShop(time.Now())
	shop.Latency = cfg.latency
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return rep, err
	}
	srv := &http.Server{Handler: shop.Handler()}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	shopURL := "http://" + ln.Addr().String() + "/mcp"

	mock := &provider.Mock{Latency: cfg.latency, FailRate: cfg.llmFaults}
	router := provider.NewRouter(provider.Route{Prefix: "mock", Provider: mock})

	// Runs: each refunds its own order, after approval.
	prep := tools.NewRunner()
	spec, err := prep.Prepare(ctx, core.AgentSpec{
		Model: "mock", System: "Refund agent.",
		Tools:           []core.ToolSpec{{Type: core.ToolMCP, URL: shopURL}},
		RequireApproval: []string{"issue_refund"},
	})
	if err != nil {
		return rep, fmt.Errorf("prepare spec (shop reachable?): %w", err)
	}
	shop.SetFailRate(cfg.toolFaults) // faults start once setup is done
	day := time.Now().Add(-48 * time.Hour).Format(time.DateOnly)
	ids := make([]string, cfg.runs)
	orderOf := map[string]string{}
	for i := 0; i < cfg.runs; i++ {
		order := fmt.Sprintf("ORD-C%05d", i)
		shop.AddOrder(demo.Order{ID: order, Customer: "c", Status: "delivered", Amount: 20, Currency: "USD", DeliveredAt: day})
		r, _, err := base.CreateRun(ctx, core.NewRun{Spec: spec, Input: fmt.Sprintf("order_id: %s, amount: 20, reason: damaged", order)})
		if err != nil {
			return rep, err
		}
		ids[i] = r.ID
		orderOf[r.ID] = order
	}

	// A human who approves everything, a little later.
	go func() {
		for ctx.Err() == nil {
			waiting, _ := base.ListRuns(ctx, store.ListFilter{Status: core.RunWaiting, Limit: 1000})
			for _, r := range waiting {
				steps, _ := base.ListSteps(ctx, r.ID)
				for _, s := range steps {
					if s.Status == core.StepPendingApproval {
						_ = base.DecideApproval(ctx, r.ID, s.ID, core.Approval{Approved: true, DecidedBy: "chaos"})
					}
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Worker processes.
	type proc struct {
		conn   *killable
		cancel context.CancelFunc
		done   chan struct{}
	}
	var (
		mu    sync.Mutex
		procs []*proc
		seq   int
	)
	spawn := func() {
		mu.Lock()
		seq++
		id := seq
		mu.Unlock()
		conn := &killable{Store: base}
		runner := tools.NewRunner()
		eng := engine.New(engine.Config{
			WorkerID: fmt.Sprintf("proc-%d", id), Workers: cfg.perWorker, Lease: cfg.lease,
			PollInterval: 30 * time.Millisecond, MaxAttempts: 10,
			BackoffBase: 5 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		}, engine.Deps{Store: conn, Router: router, Tools: runner, Logger: quiet})
		pctx, pcancel := context.WithCancel(ctx)
		p := &proc{conn: conn, cancel: pcancel, done: make(chan struct{})}
		go func() { eng.Run(pctx); close(p.done) }()
		mu.Lock()
		procs = append(procs, p)
		rep.spawned++
		mu.Unlock()
	}
	kill := func() {
		mu.Lock()
		if len(procs) == 0 {
			mu.Unlock()
			return
		}
		i := rand.IntN(len(procs))
		p := procs[i]
		procs = append(procs[:i], procs[i+1:]...)
		rep.kills++
		mu.Unlock()
		p.conn.dead.Store(true) // from now on, nothing this process does reaches the database
		p.cancel()              // and its in-flight calls are cut off
	}
	for i := 0; i < cfg.workers; i++ {
		spawn()
	}

	start := time.Now()
	killerDone := make(chan struct{})
	allDone := make(chan struct{})
	go func() {
		defer close(killerDone)
		for {
			wait := time.Duration(float64(cfg.killEvery) * (0.5 + rand.Float64()))
			select {
			case <-allDone:
				return
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			kill()
			time.Sleep(20 * time.Millisecond) // a replacement process boots
			spawn()
		}
	}()

	// Wait until every run is terminal.
	for ctx.Err() == nil {
		done := 0
		for _, id := range ids {
			if r, _ := base.GetRun(ctx, id); r.Status.Terminal() {
				done++
			}
		}
		if done == len(ids) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	rep.duration = time.Since(start)
	close(allDone)
	<-killerDone
	mu.Lock()
	for _, p := range procs {
		p.cancel()
	}
	alive := append([]*proc(nil), procs...)
	mu.Unlock()
	for _, p := range alive {
		<-p.done
	}

	// ---- verify ----
	bg := context.Background()
	for _, id := range ids {
		r, _ := base.GetRun(bg, id)
		switch r.Status {
		case core.RunCompleted:
			rep.completed++
		case core.RunFailed:
			rep.failed++
			rep.failures = append(rep.failures, fmt.Sprintf("%s: %s %s", id[:8], r.FailureKind, r.Error))
		default:
			rep.other[r.Status]++
		}
		rep.claims += r.Claims
		if r.Claims > 1 {
			rep.takeovers += r.Claims - 1
		}
		steps, _ := base.ListSteps(bg, id)
		rep.steps += len(steps)
		for i, s := range steps {
			if s.Seq != i+1 {
				rep.gaps = append(rep.gaps, fmt.Sprintf("run %s: step %d has seq %d", id[:8], i+1, s.Seq))
				break
			}
			if s.Attempts > 1 {
				if s.Kind == core.StepLLM {
					rep.llmReexec += s.Attempts - 1
				} else {
					rep.toolReexc += s.Attempts - 1
				}
			}
		}
		if r.Status == core.RunCompleted && len(steps) != 7 {
			rep.gaps = append(rep.gaps, fmt.Sprintf("run %s: %d steps, want 7", id[:8], len(steps)))
		}
	}

	perOrder := map[string]int{}
	for _, rf := range shop.Refunds() {
		perOrder[rf.OrderID]++
	}
	for _, n := range perOrder {
		if n > 1 {
			rep.ordersRefundedTwice++
		}
	}
	st := shop.Stats()
	rep.refunds, rep.duplicates, rep.toolFaults = st.RefundsIssued, st.DuplicatesSeen, st.InjectedFaults
	sort.Strings(rep.failures)
	if ctx.Err() != nil {
		b, _ := json.Marshal(rep.other)
		return rep, fmt.Errorf("timed out with runs unfinished: %s", b)
	}
	return rep, nil
}
