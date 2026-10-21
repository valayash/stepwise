// Package memory is an in-memory store.Store for tests and `stepwise dev`.
// It implements the same semantics as the Postgres store (leases, fencing,
// outbox, parent resolution) but nothing survives a restart.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/store"
)

// Store is a mutex-guarded in-memory store.
type Store struct {
	mu         sync.Mutex
	now        func() time.Time
	runs       map[string]*core.Run
	steps      map[string][]*core.Step
	idem       map[string]string
	deliveries []*store.Delivery
	deliveryTo map[int64]time.Time // delivery lease
	nextStep   int64
	nextDeliv  int64

	subMu sync.Mutex
	subs  map[int]chan store.Event
	subID int
}

var _ store.Store = (*Store)(nil)

// New returns an empty store. now may be nil (time.Now).
func New(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{
		now:        now,
		runs:       map[string]*core.Run{},
		steps:      map[string][]*core.Step{},
		idem:       map[string]string{},
		deliveryTo: map[int64]time.Time{},
		subs:       map[int]chan store.Event{},
	}
}

// Close implements store.Store.
func (m *Store) Close() error { return nil }

func clone[T any](v *T) T { return *v }

// ---- runs ----

// CreateRun implements store.Store.
func (m *Store) CreateRun(_ context.Context, nr core.NewRun) (core.Run, bool, error) {
	m.mu.Lock()
	if nr.IdempotencyKey != "" {
		if id, ok := m.idem[nr.IdempotencyKey]; ok {
			r := clone(m.runs[id])
			m.mu.Unlock()
			return r, false, nil
		}
	}
	r := newRun(nr, m.now())
	stored := r // the map owns its own copy; r is returned without the lock
	m.runs[r.ID] = &stored
	if nr.IdempotencyKey != "" {
		m.idem[nr.IdempotencyKey] = r.ID
	}
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunsReady, RunID: r.ID})
	return r, true, nil
}

func newRun(nr core.NewRun, now time.Time) core.Run {
	budget := nr.BudgetTokens
	if budget <= 0 {
		budget = core.DefaultBudgetTokens
	}
	return core.Run{
		ID:             core.NewID(),
		Status:         core.RunQueued,
		Spec:           nr.Spec.WithDefaults(),
		Input:          nr.Input,
		BudgetTokens:   budget,
		AvailableAt:    now,
		ParentRunID:    nr.ParentRunID,
		ParentStepID:   nr.ParentStepID,
		WebhookURL:     nr.WebhookURL,
		Metadata:       nr.Metadata,
		IdempotencyKey: nr.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// GetRun implements store.Store.
func (m *Store) GetRun(_ context.Context, id string) (core.Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[id]
	if !ok {
		return core.Run{}, core.ErrNotFound
	}
	return clone(r), nil
}

// ListRuns implements store.Store. Newest first.
func (m *Store) ListRuns(_ context.Context, f store.ListFilter) ([]core.Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []core.Run
	for _, r := range m.runs {
		if f.Status != "" && r.Status != f.Status {
			continue
		}
		if f.ParentRunID != "" && r.ParentRunID != f.ParentRunID {
			continue
		}
		out = append(out, clone(r))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID > out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

// ListSteps implements store.Store. Ordered by seq.
func (m *Store) ListSteps(_ context.Context, runID string) ([]core.Step, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.runs[runID]; !ok {
		return nil, core.ErrNotFound
	}
	return m.stepsOf(runID), nil
}

func (m *Store) stepsOf(runID string) []core.Step {
	src := m.steps[runID]
	out := make([]core.Step, len(src))
	for i, s := range src {
		out[i] = clone(s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// ---- workers ----

// ClaimRuns implements store.Store.
func (m *Store) ClaimRuns(_ context.Context, owner string, limit int, lease time.Duration) ([]core.Run, error) {
	m.mu.Lock()
	now := m.now()
	var candidates []*core.Run
	for _, r := range m.runs {
		queued := r.Status == core.RunQueued && !r.AvailableAt.After(now)
		orphaned := r.Status == core.RunRunning && r.LeaseExpiresAt != nil && r.LeaseExpiresAt.Before(now)
		if queued || orphaned {
			candidates = append(candidates, r)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].AvailableAt.Before(candidates[j].AvailableAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	out := make([]core.Run, 0, len(candidates))
	exp := now.Add(lease)
	for _, r := range candidates {
		r.Status = core.RunRunning
		r.LeaseOwner = owner
		r.Claims++
		r.LeaseToken = int64(r.Claims)
		r.LeaseExpiresAt = &exp
		r.UpdatedAt = now
		out = append(out, clone(r))
	}
	m.mu.Unlock()
	for _, r := range out {
		m.publish(store.Event{Type: store.EventRunChanged, RunID: r.ID})
	}
	return out, nil
}

// fenced returns the run if the fence is valid. Caller holds m.mu.
func (m *Store) fenced(f core.Fence) (*core.Run, error) {
	r, ok := m.runs[f.RunID]
	if !ok {
		return nil, core.ErrNotFound
	}
	if r.Status != core.RunRunning || r.LeaseToken != f.Token {
		return nil, core.ErrFenced
	}
	return r, nil
}

// Heartbeat implements store.Store.
func (m *Store) Heartbeat(_ context.Context, f core.Fence, lease time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.fenced(f)
	if err != nil {
		return err
	}
	exp := m.now().Add(lease)
	r.LeaseExpiresAt = &exp
	return nil
}

// BeginStep implements store.Store.
func (m *Store) BeginStep(_ context.Context, f core.Fence, s core.Step) (core.Step, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.fenced(f)
	if err != nil {
		return core.Step{}, err
	}
	now := m.now()
	for _, ex := range m.steps[f.RunID] {
		if ex.Seq != s.Seq {
			continue
		}
		if ex.Status != core.StepRunning && ex.Status != core.StepApproved {
			return core.Step{}, fmt.Errorf("%w: step %d is %s", core.ErrConflict, s.Seq, ex.Status)
		}
		ex.Status = core.StepRunning
		ex.Attempts++
		r.UpdatedAt = now
		return clone(ex), nil
	}
	m.nextStep++
	s.ID = m.nextStep
	s.RunID = f.RunID
	if s.Status == "" {
		s.Status = core.StepRunning
	}
	if s.Status == core.StepRunning {
		s.Attempts = 1
	}
	s.CreatedAt = now
	m.steps[f.RunID] = append(m.steps[f.RunID], &s)
	r.UpdatedAt = now
	if s.Status == core.StepPendingApproval {
		m.outbox(r, core.EventApprovalRequested, &s, now)
	}
	defer m.publish(store.Event{Type: store.EventRunChanged, RunID: f.RunID})
	return s, nil
}

// SaveStep implements store.Store.
func (m *Store) SaveStep(_ context.Context, f core.Fence, s core.Step) error {
	m.mu.Lock()
	r, err := m.fenced(f)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	ex := m.findStep(f.RunID, s.ID)
	if ex == nil {
		m.mu.Unlock()
		return core.ErrNotFound
	}
	now := m.now()
	wasCompleted := ex.Status == core.StepCompleted
	attempts, created, id, seq := ex.Attempts, ex.CreatedAt, ex.ID, ex.Seq
	*ex = s
	ex.ID, ex.Seq, ex.RunID, ex.Attempts, ex.CreatedAt = id, seq, f.RunID, attempts, created
	if ex.Status == core.StepCompleted && !wasCompleted {
		ex.CompletedAt = &now
		r.TokensUsed += ex.Tokens()
		r.Attempts = 0
		r.Error = ""
	}
	if ex.Status == core.StepPendingApproval {
		m.outbox(r, core.EventApprovalRequested, ex, now)
	}
	r.UpdatedAt = now
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunChanged, RunID: f.RunID})
	return nil
}

func (m *Store) findStep(runID string, id int64) *core.Step {
	for _, s := range m.steps[runID] {
		if s.ID == id {
			return s
		}
	}
	return nil
}

// SpawnChild implements store.Store.
func (m *Store) SpawnChild(_ context.Context, f core.Fence, parent core.Step, nr core.NewRun) (core.Step, core.Run, error) {
	m.mu.Lock()
	r, err := m.fenced(f)
	if err != nil {
		m.mu.Unlock()
		return core.Step{}, core.Run{}, err
	}
	now := m.now()
	ps := m.findStep(f.RunID, parent.ID)
	if ps == nil {
		m.mu.Unlock()
		return core.Step{}, core.Run{}, core.ErrNotFound
	}
	if ps.ChildRunID != "" { // already spawned before a crash: reuse the child
		out, child := clone(ps), clone(m.runs[ps.ChildRunID])
		m.mu.Unlock()
		return out, child, nil
	}
	nr.ParentRunID, nr.ParentStepID = f.RunID, ps.ID
	child := newRun(nr, now)
	stored := child
	m.runs[child.ID] = &stored
	ps.Status = core.StepWaitingChild
	ps.ChildRunID = child.ID
	r.UpdatedAt = now
	out := clone(ps)
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunsReady, RunID: child.ID})
	m.publish(store.Event{Type: store.EventRunChanged, RunID: f.RunID})
	return out, child, nil
}

// Yield implements store.Store.
func (m *Store) Yield(_ context.Context, f core.Fence, y store.Yield) error {
	m.mu.Lock()
	r, err := m.fenced(f)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	now := m.now()
	switch y.Status {
	case core.RunQueued:
		r.AvailableAt = y.AvailableAt
		if r.AvailableAt.IsZero() {
			r.AvailableAt = now
		}
	case core.RunWaiting:
		// A sub-agent may have finished, or a decision arrived, while this
		// worker still held the run. If nothing blocks anymore, requeue
		// instead of waiting forever.
		steps := m.stepsOf(r.ID)
		if !store.Blocking(steps) || store.HasWork(steps) {
			y.Status = core.RunQueued
			r.AvailableAt = now
		}
	default:
		m.mu.Unlock()
		return fmt.Errorf("%w: yield status %q", core.ErrInvalid, y.Status)
	}
	r.Status = y.Status
	r.Attempts = y.Attempts
	r.Error = y.Error
	r.LeaseOwner, r.LeaseExpiresAt = "", nil
	r.UpdatedAt = now
	ready := r.Status == core.RunQueued
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunChanged, RunID: f.RunID})
	if ready {
		m.publish(store.Event{Type: store.EventRunsReady, RunID: f.RunID})
	}
	return nil
}

// Finish implements store.Store.
func (m *Store) Finish(_ context.Context, f core.Fence, o store.Outcome) error {
	m.mu.Lock()
	r, err := m.fenced(f)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if !o.Status.Terminal() {
		m.mu.Unlock()
		return fmt.Errorf("%w: finish status %q", core.ErrInvalid, o.Status)
	}
	now := m.now()
	m.finishLocked(r, o, now)
	parent := r.ParentRunID
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunChanged, RunID: f.RunID})
	if parent != "" {
		m.publish(store.Event{Type: store.EventRunChanged, RunID: parent})
		m.publish(store.Event{Type: store.EventRunsReady, RunID: parent})
	}
	return nil
}

// finishLocked ends a run, writes its webhook and resolves its parent step.
func (m *Store) finishLocked(r *core.Run, o store.Outcome, now time.Time) {
	r.Status = o.Status
	r.Output, r.Error, r.FailureKind = o.Output, o.Error, o.FailureKind
	r.LeaseOwner, r.LeaseExpiresAt = "", nil
	r.CompletedAt = &now
	r.UpdatedAt = now

	event := map[core.RunStatus]string{
		core.RunCompleted: core.EventRunCompleted,
		core.RunFailed:    core.EventRunFailed,
		core.RunCancelled: core.EventRunCancelled,
	}[o.Status]
	m.outbox(r, event, nil, now)

	if r.ParentRunID == "" {
		return
	}
	parent := m.runs[r.ParentRunID]
	ps := m.findStep(r.ParentRunID, r.ParentStepID)
	if parent == nil || ps == nil || ps.Status != core.StepWaitingChild {
		return
	}
	ps.Status = core.StepCompleted
	ps.CompletedAt = &now
	ps.DurationMs = now.Sub(ps.CreatedAt).Milliseconds()
	if o.Status == core.RunCompleted {
		ps.Result = o.Output
	} else {
		ps.Result = fmt.Sprintf("Sub-agent %s: %s", o.Status, o.Error)
		ps.IsError = true
	}
	// Sub-agent tokens count against the parent's budget too.
	parent.TokensUsed += r.TokensUsed
	if parent.Status == core.RunWaiting && !store.Blocking(m.stepsOf(parent.ID)) {
		parent.Status = core.RunQueued
		parent.AvailableAt = now
	}
	parent.UpdatedAt = now
}

// outbox queues a webhook if the run has a URL. Caller holds m.mu.
func (m *Store) outbox(r *core.Run, event string, step *core.Step, now time.Time) {
	if r.WebhookURL == "" || event == "" {
		return
	}
	m.nextDeliv++
	m.deliveries = append(m.deliveries, &store.Delivery{
		ID: m.nextDeliv, RunID: r.ID, URL: r.WebhookURL, Event: event,
		Payload: store.WebhookPayload(event, *r, step, now), Status: store.DeliveryPending,
		NextAttemptAt: now, CreatedAt: now,
	})
}

// ---- humans ----

// DecideApproval implements store.Store.
func (m *Store) DecideApproval(_ context.Context, runID string, stepID int64, a core.Approval) error {
	m.mu.Lock()
	r, ok := m.runs[runID]
	if !ok {
		m.mu.Unlock()
		return core.ErrNotFound
	}
	s := m.findStep(runID, stepID)
	if s == nil {
		m.mu.Unlock()
		return core.ErrNotFound
	}
	if s.Status != core.StepPendingApproval || r.Status.Terminal() {
		m.mu.Unlock()
		return fmt.Errorf("%w: step is %s", core.ErrNotRunnable, s.Status)
	}
	now := m.now()
	a.DecidedAt = now
	s.Approval = &a
	if a.Approved {
		s.Status = core.StepApproved
	} else {
		s.Status = core.StepCompleted
		s.CompletedAt = &now
		s.IsError = true
		s.Result = "The reviewer rejected this action."
		if a.Note != "" {
			s.Result += " Reviewer note: " + a.Note
		}
	}
	steps := m.stepsOf(runID)
	if r.Status == core.RunWaiting && (store.HasWork(steps) || !store.Blocking(steps)) {
		r.Status = core.RunQueued
		r.AvailableAt = now
	}
	r.UpdatedAt = now
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunChanged, RunID: runID})
	m.publish(store.Event{Type: store.EventRunsReady, RunID: runID})
	return nil
}

// Cancel implements store.Store. Children of a cancelled run are cancelled too.
func (m *Store) Cancel(_ context.Context, runID string) error {
	m.mu.Lock()
	r, ok := m.runs[runID]
	if !ok {
		m.mu.Unlock()
		return core.ErrNotFound
	}
	if r.Status.Terminal() {
		m.mu.Unlock()
		return fmt.Errorf("%w: run is %s", core.ErrNotRunnable, r.Status)
	}
	now := m.now()
	changed := []string{}
	var cancel func(r *core.Run)
	cancel = func(r *core.Run) {
		m.finishLocked(r, store.Outcome{Status: core.RunCancelled, Error: "cancelled"}, now)
		changed = append(changed, r.ID)
		for _, c := range m.runs {
			if c.ParentRunID == r.ID && !c.Status.Terminal() {
				cancel(c)
			}
		}
	}
	cancel(r)
	m.mu.Unlock()
	for _, id := range changed {
		m.publish(store.Event{Type: store.EventRunChanged, RunID: id})
	}
	return nil
}

// Retry implements store.Store.
func (m *Store) Retry(_ context.Context, runID string) error {
	m.mu.Lock()
	r, ok := m.runs[runID]
	if !ok {
		m.mu.Unlock()
		return core.ErrNotFound
	}
	if r.Status != core.RunFailed {
		m.mu.Unlock()
		return fmt.Errorf("%w: only failed runs can be retried (run is %s)", core.ErrNotRunnable, r.Status)
	}
	now := m.now()
	r.Status = core.RunQueued
	r.AvailableAt = now
	r.Attempts = 0
	r.Error, r.FailureKind, r.CompletedAt = "", "", nil
	r.UpdatedAt = now
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunChanged, RunID: runID})
	m.publish(store.Event{Type: store.EventRunsReady, RunID: runID})
	return nil
}

// SetTriage implements store.Store.
func (m *Store) SetTriage(_ context.Context, runID string, d json.RawMessage) error {
	m.mu.Lock()
	r, ok := m.runs[runID]
	if !ok {
		m.mu.Unlock()
		return core.ErrNotFound
	}
	r.Triage = d
	r.UpdatedAt = m.now()
	m.outbox(r, core.EventRunTriaged, nil, r.UpdatedAt)
	m.mu.Unlock()
	m.publish(store.Event{Type: store.EventRunChanged, RunID: runID})
	return nil
}

// ---- webhook outbox ----

// ClaimDeliveries implements store.Store.
func (m *Store) ClaimDeliveries(_ context.Context, limit int, lease time.Duration) ([]store.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var out []store.Delivery
	for _, d := range m.deliveries {
		if len(out) >= limit {
			break
		}
		if d.Status != store.DeliveryPending || d.NextAttemptAt.After(now) {
			continue
		}
		if until, ok := m.deliveryTo[d.ID]; ok && until.After(now) {
			continue
		}
		m.deliveryTo[d.ID] = now.Add(lease)
		d.Attempts++
		out = append(out, clone(d))
	}
	return out, nil
}

// CompleteDelivery implements store.Store.
func (m *Store) CompleteDelivery(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, d := range m.deliveries {
		if d.ID == id {
			d.Status = store.DeliveryDelivered
			d.LastError = ""
			delete(m.deliveryTo, id)
			return nil
		}
	}
	return core.ErrNotFound
}

// FailDelivery implements store.Store.
func (m *Store) FailDelivery(_ context.Context, id int64, next time.Time, errMsg string, dead bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, d := range m.deliveries {
		if d.ID == id {
			d.LastError = errMsg
			d.NextAttemptAt = next
			if dead {
				d.Status = store.DeliveryDead
			}
			delete(m.deliveryTo, id)
			return nil
		}
	}
	return core.ErrNotFound
}

// ListDeliveries implements store.Store.
func (m *Store) ListDeliveries(_ context.Context, runID string) ([]store.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Delivery
	for _, d := range m.deliveries {
		if runID == "" || d.RunID == runID {
			out = append(out, clone(d))
		}
	}
	return out, nil
}

// ---- notifications ----

// Subscribe implements store.Store.
func (m *Store) Subscribe() (<-chan store.Event, func()) {
	ch := make(chan store.Event, 64)
	m.subMu.Lock()
	m.subID++
	id := m.subID
	m.subs[id] = ch
	m.subMu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			m.subMu.Lock()
			delete(m.subs, id)
			m.subMu.Unlock()
		})
	}
}

func (m *Store) publish(e store.Event) {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	for _, ch := range m.subs {
		select {
		case ch <- e:
		default: // slow subscriber: drop, they also poll
		}
	}
}
