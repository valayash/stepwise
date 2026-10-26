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
	store.Hub

	mu         sync.Mutex
	now        func() time.Time
	runs       map[string]*core.Run
	steps      map[string][]*core.Step
	idem       map[string]string
	deliveries []*store.Delivery
	deliveryTo map[int64]time.Time // delivery lease
	nextStep   int64
	nextDeliv  int64
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
	}
}

// Close implements store.Store.
func (m *Store) Close() error { return nil }

func clone[T any](v *T) T { return *v }

// write runs fn under the lock, then publishes the events it returns. Every
// mutation goes through here, so no error path can forget to unlock.
func (m *Store) write(fn func(now time.Time) ([]store.Event, error)) error {
	m.mu.Lock()
	events, err := fn(m.now())
	m.mu.Unlock()
	m.Publish(events...)
	return err
}

// ---- runs ----

// CreateRun implements store.Store.
func (m *Store) CreateRun(_ context.Context, nr core.NewRun) (r core.Run, created bool, err error) {
	err = m.write(func(now time.Time) ([]store.Event, error) {
		if id, ok := m.idem[nr.IdempotencyKey]; ok && nr.IdempotencyKey != "" {
			r = clone(m.runs[id])
			return nil, nil
		}
		r, created = m.insertRun(nr, now), true
		if nr.IdempotencyKey != "" {
			m.idem[nr.IdempotencyKey] = r.ID
		}
		return []store.Event{store.Ready(r.ID)}, nil
	})
	return r, created, err
}

// insertRun stores a new queued run and returns a copy. Caller holds m.mu.
func (m *Store) insertRun(nr core.NewRun, now time.Time) core.Run {
	r := core.Run{
		ID:             core.NewID(),
		Status:         core.RunQueued,
		Spec:           nr.Spec.WithDefaults(),
		Input:          nr.Input,
		BudgetTokens:   nr.Budget(),
		AvailableAt:    now,
		ParentRunID:    nr.ParentRunID,
		ParentStepID:   nr.ParentStepID,
		WebhookURL:     nr.WebhookURL,
		Metadata:       nr.Metadata,
		IdempotencyKey: nr.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	stored := r // the map owns its own copy
	m.runs[r.ID] = &stored
	return r
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

// Usage implements store.Store.
func (m *Store) Usage(_ context.Context, since time.Time) (store.Usage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var u store.Usage
	for _, r := range m.runs {
		if r.ParentRunID == "" && !r.CreatedAt.Before(since) {
			u.Runs++
			u.Tokens += r.TokensUsed
		}
	}
	return u, nil
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
func (m *Store) ClaimRuns(_ context.Context, owner string, limit int, lease time.Duration) (out []core.Run, err error) {
	err = m.write(func(now time.Time) ([]store.Event, error) {
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
		var events []store.Event
		out = make([]core.Run, 0, len(candidates))
		exp := now.Add(lease)
		for _, r := range candidates {
			r.Status = core.RunRunning
			r.LeaseOwner = owner
			r.Claims++
			r.LeaseToken = int64(r.Claims)
			r.LeaseExpiresAt = &exp
			r.UpdatedAt = now
			out = append(out, clone(r))
			events = append(events, store.Changed(r.ID))
		}
		return events, nil
	})
	return out, err
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
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, err := m.fenced(f)
		if err != nil {
			return nil, err
		}
		exp := now.Add(lease)
		r.LeaseExpiresAt = &exp
		return nil, nil
	})
}

// BeginStep implements store.Store.
func (m *Store) BeginStep(_ context.Context, f core.Fence, s core.Step) (out core.Step, err error) {
	err = m.write(func(now time.Time) ([]store.Event, error) {
		r, err := m.fenced(f)
		if err != nil {
			return nil, err
		}
		r.UpdatedAt = now
		for _, ex := range m.steps[f.RunID] {
			if ex.Seq != s.Seq {
				continue
			}
			if ex.Status != core.StepRunning && ex.Status != core.StepApproved {
				return nil, fmt.Errorf("%w: step %d is %s", core.ErrConflict, s.Seq, ex.Status)
			}
			ex.Status = core.StepRunning
			ex.Attempts++
			out = clone(ex)
			return nil, nil
		}
		m.nextStep++
		s.ID, s.RunID, s.CreatedAt = m.nextStep, f.RunID, now
		if s.Status == "" {
			s.Status = core.StepRunning
		}
		if s.Status == core.StepRunning {
			s.Attempts = 1
		}
		m.steps[f.RunID] = append(m.steps[f.RunID], &s)
		if s.Status == core.StepPendingApproval {
			m.outbox(r, core.EventApprovalRequested, &s, now)
		}
		out = s
		return []store.Event{store.Changed(f.RunID)}, nil
	})
	return out, err
}

// SaveStep implements store.Store.
func (m *Store) SaveStep(_ context.Context, f core.Fence, s core.Step) error {
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, err := m.fenced(f)
		if err != nil {
			return nil, err
		}
		ex := m.findStep(f.RunID, s.ID)
		if ex == nil {
			return nil, core.ErrNotFound
		}
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
		return []store.Event{store.Changed(f.RunID)}, nil
	})
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
func (m *Store) SpawnChild(_ context.Context, f core.Fence, parent core.Step, nr core.NewRun) (out core.Step, child core.Run, err error) {
	err = m.write(func(now time.Time) ([]store.Event, error) {
		r, err := m.fenced(f)
		if err != nil {
			return nil, err
		}
		ps := m.findStep(f.RunID, parent.ID)
		if ps == nil {
			return nil, core.ErrNotFound
		}
		if ps.ChildRunID != "" { // already spawned before a crash: reuse the child
			out, child = clone(ps), clone(m.runs[ps.ChildRunID])
			return nil, nil
		}
		nr.ParentRunID, nr.ParentStepID = f.RunID, ps.ID
		child = m.insertRun(nr, now)
		ps.Status = core.StepWaitingChild
		ps.ChildRunID = child.ID
		r.UpdatedAt = now
		out = clone(ps)
		return []store.Event{store.Ready(child.ID), store.Changed(f.RunID)}, nil
	})
	return out, child, err
}

// Yield implements store.Store.
func (m *Store) Yield(_ context.Context, f core.Fence, y store.Yield) error {
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, err := m.fenced(f)
		if err != nil {
			return nil, err
		}
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
			return nil, fmt.Errorf("%w: yield status %q", core.ErrInvalid, y.Status)
		}
		r.Status = y.Status
		r.Attempts = y.Attempts
		r.Error = y.Error
		r.LeaseOwner, r.LeaseExpiresAt = "", nil
		r.UpdatedAt = now
		if r.Status == core.RunQueued {
			return []store.Event{store.Changed(f.RunID), store.Ready(f.RunID)}, nil
		}
		return []store.Event{store.Changed(f.RunID)}, nil
	})
}

// Finish implements store.Store.
func (m *Store) Finish(_ context.Context, f core.Fence, o store.Outcome) error {
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, err := m.fenced(f)
		if err != nil {
			return nil, err
		}
		if !o.Status.Terminal() {
			return nil, fmt.Errorf("%w: finish status %q", core.ErrInvalid, o.Status)
		}
		m.finishLocked(r, o, now)
		if r.ParentRunID != "" {
			return []store.Event{store.Changed(f.RunID), store.Changed(r.ParentRunID), store.Ready(r.ParentRunID)}, nil
		}
		return []store.Event{store.Changed(f.RunID)}, nil
	})
}

// finishLocked ends a run, writes its webhook and resolves its parent step.
func (m *Store) finishLocked(r *core.Run, o store.Outcome, now time.Time) {
	r.Status = o.Status
	r.Output, r.Error, r.FailureKind = o.Output, o.Error, o.FailureKind
	r.LeaseOwner, r.LeaseExpiresAt = "", nil
	r.CompletedAt = &now
	r.UpdatedAt = now
	m.outbox(r, store.FinishEvent(o.Status), nil, now)

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
	ps.Result, ps.IsError = store.ChildResult(o)
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

// PendingApprovals implements store.Store.
func (m *Store) PendingApprovals(_ context.Context) ([]core.Step, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []core.Step
	for _, steps := range m.steps {
		for _, s := range steps {
			if s.Status == core.StepPendingApproval {
				out = append(out, clone(s))
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// DecideApproval implements store.Store.
func (m *Store) DecideApproval(_ context.Context, runID string, stepID int64, a core.Approval) error {
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, ok := m.runs[runID]
		s := m.findStep(runID, stepID)
		if !ok || s == nil {
			return nil, core.ErrNotFound
		}
		if s.Status != core.StepPendingApproval || r.Status.Terminal() {
			return nil, fmt.Errorf("%w: step is %s", core.ErrNotRunnable, s.Status)
		}
		a.DecidedAt = now
		s.Approval = &a
		if a.Approved {
			s.Status = core.StepApproved
		} else {
			s.Status = core.StepCompleted
			s.CompletedAt = &now
			s.IsError = true
			s.Result = store.RejectionResult(a.Note)
		}
		steps := m.stepsOf(runID)
		if r.Status == core.RunWaiting && (store.HasWork(steps) || !store.Blocking(steps)) {
			r.Status = core.RunQueued
			r.AvailableAt = now
		}
		r.UpdatedAt = now
		return []store.Event{store.Changed(runID), store.Ready(runID)}, nil
	})
}

// Cancel implements store.Store. Children of a cancelled run are cancelled too.
func (m *Store) Cancel(_ context.Context, runID string) error {
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, ok := m.runs[runID]
		if !ok {
			return nil, core.ErrNotFound
		}
		if r.Status.Terminal() {
			return nil, fmt.Errorf("%w: run is %s", core.ErrNotRunnable, r.Status)
		}
		var events []store.Event
		var cancel func(r *core.Run)
		cancel = func(r *core.Run) {
			m.finishLocked(r, store.Outcome{Status: core.RunCancelled, Error: "cancelled"}, now)
			events = append(events, store.Changed(r.ID))
			for _, c := range m.runs {
				if c.ParentRunID == r.ID && !c.Status.Terminal() {
					cancel(c)
				}
			}
		}
		cancel(r)
		return events, nil
	})
}

// Retry implements store.Store.
func (m *Store) Retry(_ context.Context, runID string) error {
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, ok := m.runs[runID]
		if !ok {
			return nil, core.ErrNotFound
		}
		if r.Status != core.RunFailed {
			return nil, fmt.Errorf("%w: only failed runs can be retried (run is %s)", core.ErrNotRunnable, r.Status)
		}
		r.Status = core.RunQueued
		r.AvailableAt = now
		r.Attempts = 0
		r.Error, r.FailureKind, r.CompletedAt = "", "", nil
		r.UpdatedAt = now
		return []store.Event{store.Changed(runID), store.Ready(runID)}, nil
	})
}

// SetTriage implements store.Store.
func (m *Store) SetTriage(_ context.Context, runID string, d json.RawMessage) error {
	return m.write(func(now time.Time) ([]store.Event, error) {
		r, ok := m.runs[runID]
		if !ok {
			return nil, core.ErrNotFound
		}
		r.Triage = d
		r.UpdatedAt = now
		m.outbox(r, core.EventRunTriaged, nil, now)
		return []store.Event{store.Changed(runID)}, nil
	})
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
	return m.updateDelivery(id, func(d *store.Delivery) {
		d.Status = store.DeliveryDelivered
		d.LastError = ""
	})
}

// FailDelivery implements store.Store.
func (m *Store) FailDelivery(_ context.Context, id int64, next time.Time, errMsg string, dead bool) error {
	return m.updateDelivery(id, func(d *store.Delivery) {
		d.LastError = errMsg
		d.NextAttemptAt = next
		if dead {
			d.Status = store.DeliveryDead
		}
	})
}

// updateDelivery applies fn to a delivery and releases its lease.
func (m *Store) updateDelivery(id int64, fn func(*store.Delivery)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, d := range m.deliveries {
		if d.ID == id {
			fn(d)
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
