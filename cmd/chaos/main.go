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
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/demo"
	"github.com/valayash/stepwise/internal/engine"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/store/memory"
	"github.com/valayash/stepwise/internal/store/postgres"
	"github.com/valayash/stepwise/internal/store/postgres/pgxdriver"
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
		db         = flag.String("db", "", "Postgres URL: run workers as separate OS processes and kill them with SIGKILL")
		bin        = flag.String("bin", "", "stepwise binary for -db mode (built automatically if empty)")
	)
	flag.Parse()
	if *db != "" && *latency <= 0 {
		// Worker processes read it as STEPWISE_MOCK_LATENCY, which must be positive;
		// with 0 they exit at startup and the run would just time out.
		fmt.Fprintln(os.Stderr, "chaos: -latency must be positive with -db (use 1ms for none)")
		os.Exit(2)
	}

	rep, err := run(config{*runs, *workers, *perWorker, *killEvery, *lease, *toolFaults, *llmFaults, *latency, *timeout, *db, *bin})
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
	db, bin                  string
}

// ---- worker fleets ----

// fleet starts and kills worker processes.
type fleet interface {
	spawn() error
	kill() bool // false when nothing was running
	stopAll()
}

// goroutineFleet runs workers inside this process and simulates kill -9 by
// cutting a worker's store connection at the instant of the kill.
type goroutineFleet struct {
	ctx    context.Context
	base   store.Store
	router *provider.Router
	log    *slog.Logger
	cfg    config

	mu    sync.Mutex
	procs []*goroutineProc
	seq   int
}

type goroutineProc struct {
	conn   *killable
	cancel context.CancelFunc
	done   chan struct{}
}

func (f *goroutineFleet) spawn() error {
	f.mu.Lock()
	f.seq++
	id := f.seq
	f.mu.Unlock()
	conn := &killable{Store: f.base}
	eng := engine.New(engine.Config{
		WorkerID: fmt.Sprintf("proc-%d", id), Workers: f.cfg.perWorker, Lease: f.cfg.lease,
		PollInterval: 30 * time.Millisecond, MaxAttempts: 10,
		BackoffBase: 5 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
	}, engine.Deps{Store: conn, Router: f.router, Tools: tools.NewRunner(), Logger: f.log})
	pctx, pcancel := context.WithCancel(f.ctx)
	p := &goroutineProc{conn: conn, cancel: pcancel, done: make(chan struct{})}
	go func() { eng.Run(pctx); close(p.done) }()
	f.mu.Lock()
	f.procs = append(f.procs, p)
	f.mu.Unlock()
	return nil
}

func (f *goroutineFleet) kill() bool {
	f.mu.Lock()
	if len(f.procs) == 0 {
		f.mu.Unlock()
		return false
	}
	i := rand.IntN(len(f.procs))
	p := f.procs[i]
	f.procs = append(f.procs[:i], f.procs[i+1:]...)
	f.mu.Unlock()
	p.conn.dead.Store(true) // from now on, nothing this process does reaches the database
	p.cancel()              // and its in-flight calls are cut off
	return true
}

func (f *goroutineFleet) stopAll() {
	f.mu.Lock()
	procs := append([]*goroutineProc(nil), f.procs...)
	f.procs = nil
	f.mu.Unlock()
	for _, p := range procs {
		p.cancel()
		<-p.done
	}
}

// osFleet runs each worker as a real `stepwise worker` OS process and kills
// it with SIGKILL: no deferred cleanup, no graceful release, open
// transactions rolled back by Postgres when the connection drops.
type osFleet struct {
	bin string
	env []string

	mu    sync.Mutex
	procs []*exec.Cmd
}

func (f *osFleet) spawn() error {
	cmd := exec.Command(f.bin, "worker")
	cmd.Env = append(os.Environ(), f.env...)
	cmd.Stdout, cmd.Stderr = io.Discard, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	f.mu.Lock()
	f.procs = append(f.procs, cmd)
	f.mu.Unlock()
	return nil
}

func (f *osFleet) kill() bool {
	f.mu.Lock()
	if len(f.procs) == 0 {
		f.mu.Unlock()
		return false
	}
	i := rand.IntN(len(f.procs))
	cmd := f.procs[i]
	f.procs = append(f.procs[:i], f.procs[i+1:]...)
	f.mu.Unlock()
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_ = cmd.Wait()
	return true
}

func (f *osFleet) stopAll() {
	f.mu.Lock()
	procs := f.procs
	f.procs = nil
	f.mu.Unlock()
	for _, cmd := range procs {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}
}

// openPostgres creates a fresh schema for this experiment and returns a
// store on it, plus the URL worker processes should use.
func openPostgres(ctx context.Context, base string) (store.Store, string, func(), error) {
	admin, _, err := pgxdriver.Open(base)
	if err != nil {
		return nil, "", nil, err
	}
	var b [5]byte
	_, _ = crand.Read(b[:])
	schema := "chaos_" + hex.EncodeToString(b[:])
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		return nil, "", nil, err
	}
	u, err := url.Parse(base)
	if err != nil {
		admin.Close()
		return nil, "", nil, err
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	dbURL := u.String()

	db, listener, err := pgxdriver.Open(dbURL)
	if err != nil {
		admin.Close()
		return nil, "", nil, err
	}
	if err := postgres.Migrate(ctx, db); err != nil {
		db.Close()
		admin.Close()
		return nil, "", nil, err
	}
	st, err := postgres.New(db, listener)
	if err != nil {
		db.Close()
		admin.Close()
		return nil, "", nil, err
	}
	cleanup := func() {
		_ = st.Close()
		db.Close()
		_, _ = admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		admin.Close()
	}
	return st, dbURL, cleanup, nil
}

// buildBinary compiles cmd/stepwise into a temporary directory.
func buildBinary() (string, error) {
	dir, err := os.MkdirTemp("", "stepwise-chaos-")
	if err != nil {
		return "", err
	}
	bin := filepath.Join(dir, "stepwise")
	out, err := exec.Command("go", "build", "-o", bin, "./cmd/stepwise").CombinedOutput()
	if err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("build stepwise: %v\n%s", err, out)
	}
	return bin, nil
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
	how := "in-process workers, killed by cutting their store connection (simulated kill -9), in-memory store"
	if r.cfg.db != "" {
		how = "separate `stepwise worker` OS processes killed with SIGKILL, Postgres store"
	}
	fmt.Fprintf(&sb, "%d refund-agent runs (7 steps each, one human approval) on %d worker processes × %d concurrent runs, "+
		"%.0f%% injected tool failures, %.0f%% injected model failures, a worker killed every ~%s with no graceful shutdown.\n\n"+
		"Mode: %s.\n\n",
		r.cfg.runs, r.cfg.workers, r.cfg.perWorker, r.cfg.toolFaults*100, r.cfg.llmFaults*100, r.cfg.killEvery, how)
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
	var (
		base  store.Store
		dbURL string
	)
	if cfg.db != "" {
		var cleanup func()
		var err error
		base, dbURL, cleanup, err = openPostgres(ctx, cfg.db)
		if err != nil {
			return rep, err
		}
		defer cleanup()
		if cfg.bin == "" {
			bin, err := buildBinary()
			if err != nil {
				return rep, err
			}
			defer os.RemoveAll(filepath.Dir(bin))
			cfg.bin = bin
		}
	} else {
		base = memory.New(nil)
	}
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
	var fl fleet
	if cfg.db != "" {
		fl = &osFleet{bin: cfg.bin, env: []string{
			"STEPWISE_DATABASE_URL=" + dbURL,
			fmt.Sprintf("STEPWISE_WORKERS=%d", cfg.perWorker),
			"STEPWISE_LEASE=" + cfg.lease.String(),
			"STEPWISE_MAX_ATTEMPTS=10",
			"STEPWISE_POLL_INTERVAL=30ms",
			"STEPWISE_BACKOFF_BASE=5ms",
			"STEPWISE_BACKOFF_MAX=50ms",
			"STEPWISE_MOCK_LATENCY=" + cfg.latency.String(),
			fmt.Sprintf("STEPWISE_MOCK_FAIL_RATE=%g", cfg.llmFaults),
			"STEPWISE_TRIAGE=false",
			"STEPWISE_LOG_LEVEL=error",
		}}
	} else {
		fl = &goroutineFleet{ctx: ctx, base: base, router: router, log: quiet, cfg: cfg}
	}
	spawn := func() {
		if err := fl.spawn(); err != nil {
			fmt.Fprintln(os.Stderr, "chaos: spawn:", err)
			return
		}
		rep.spawned++
	}
	kill := func() {
		if fl.kill() {
			rep.kills++
		}
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
	fl.stopAll()

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
