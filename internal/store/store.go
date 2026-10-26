// Package store defines how Stepwise persists runs, steps and webhook
// deliveries. Two implementations exist: an in-memory store (tests and
// `stepwise dev`) and a Postgres store (production, crash-safe).
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/valayash/stepwise/internal/core"
)

// Store is everything the engine, API and webhook dispatcher need.
//
// Worker writes take a core.Fence. Implementations must reject a fenced
// write with core.ErrFenced unless the run is still `running` and its lease
// token matches, which makes stale workers harmless.
type Store interface {
	// ---- runs (API) ----

	// CreateRun inserts a queued run. If IdempotencyKey matches an existing
	// run, that run is returned with created=false and nothing is inserted.
	CreateRun(ctx context.Context, nr core.NewRun) (run core.Run, created bool, err error)
	GetRun(ctx context.Context, id string) (core.Run, error)
	ListRuns(ctx context.Context, f ListFilter) ([]core.Run, error)
	ListSteps(ctx context.Context, runID string) ([]core.Step, error)
	// Usage totals top-level runs created since a time (sub-agent tokens
	// already roll up into their parents).
	Usage(ctx context.Context, since time.Time) (Usage, error)

	// ---- workers ----

	// ClaimRuns leases up to limit runnable runs: queued runs whose
	// available_at has passed, plus running runs whose lease expired (their
	// worker died). Each claim gets a new, higher lease token.
	ClaimRuns(ctx context.Context, owner string, limit int, lease time.Duration) ([]core.Run, error)
	// Heartbeat extends a lease. core.ErrFenced means the worker lost the run.
	Heartbeat(ctx context.Context, f core.Fence, lease time.Duration) error
	// BeginStep records that a step is about to execute. If a step already
	// exists at the same seq (a crash interrupted it), its attempt counter is
	// incremented and the existing step is returned.
	BeginStep(ctx context.Context, f core.Fence, s core.Step) (core.Step, error)
	// SaveStep stores a step's outcome. Completing an llm step adds its tokens
	// to the run; completing any step resets the run's retry counter.
	SaveStep(ctx context.Context, f core.Fence, s core.Step) error
	// SpawnChild creates a sub-agent run and marks the parent's step as waiting for it.
	SpawnChild(ctx context.Context, f core.Fence, parentStep core.Step, child core.NewRun) (core.Step, core.Run, error)
	// Yield releases the lease: the run goes back to queued (retry later) or
	// waiting (blocked on a human or sub-agents).
	Yield(ctx context.Context, f core.Fence, y Yield) error
	// Finish ends a run, queues its webhook, and resolves the parent step if
	// this was a sub-agent run.
	Finish(ctx context.Context, f core.Fence, o Outcome) error

	// ---- humans ----

	DecideApproval(ctx context.Context, runID string, stepID int64, a core.Approval) error
	// PendingApprovals lists every tool call waiting for a human, oldest first.
	PendingApprovals(ctx context.Context) ([]core.Step, error)
	Cancel(ctx context.Context, runID string) error
	// Retry puts a failed run back in the queue with a fresh retry counter.
	Retry(ctx context.Context, runID string) error
	SetTriage(ctx context.Context, runID string, diagnosis json.RawMessage) error

	// ---- webhook outbox ----

	ClaimDeliveries(ctx context.Context, limit int, lease time.Duration) ([]Delivery, error)
	CompleteDelivery(ctx context.Context, id int64) error
	FailDelivery(ctx context.Context, id int64, next time.Time, errMsg string, dead bool) error
	ListDeliveries(ctx context.Context, runID string) ([]Delivery, error)

	// ---- change notifications ----

	// Subscribe returns a channel of change events and a function to stop.
	// Delivery is best-effort: slow subscribers miss events, so consumers
	// must also poll (workers) or re-read state (live views).
	Subscribe() (<-chan Event, func())

	Close() error
}

// Usage is token spend over a period.
type Usage struct {
	Runs   int `json:"runs"`
	Tokens int `json:"tokens"`
}

// ListFilter narrows ListRuns.
type ListFilter struct {
	Status      core.RunStatus
	ParentRunID string
	Limit       int
}

// Yield is how a worker hands a run back.
type Yield struct {
	Status      core.RunStatus // core.RunQueued or core.RunWaiting
	AvailableAt time.Time      // queued only: earliest next claim
	Attempts    int            // retry counter to store
	Error       string         // last transient error, for visibility
}

// Outcome is how a run ends.
type Outcome struct {
	Status      core.RunStatus // core.RunCompleted or core.RunFailed
	Output      string
	Error       string
	FailureKind core.FailureKind
}

// Delivery statuses.
const (
	DeliveryPending   = "pending"
	DeliveryDelivered = "delivered"
	DeliveryDead      = "dead"
)

// Delivery is one webhook message in the transactional outbox.
type Delivery struct {
	ID            int64           `json:"id"`
	RunID         string          `json:"run_id"`
	URL           string          `json:"url"`
	Event         string          `json:"event"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"`
	Attempts      int             `json:"attempts"`
	NextAttemptAt time.Time       `json:"next_attempt_at"`
	LastError     string          `json:"last_error,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

// Event types on the Subscribe channel.
const (
	EventRunsReady  = "runs_ready"  // something became claimable
	EventRunChanged = "run_changed" // a run or its steps changed
)

// Event is a change notification.
type Event struct {
	Type  string `json:"type"`
	RunID string `json:"run_id,omitempty"`
}

// Changed and Ready build the two kinds of Event.
func Changed(runID string) Event { return Event{Type: EventRunChanged, RunID: runID} }
func Ready(runID string) Event   { return Event{Type: EventRunsReady, RunID: runID} }

// Hub fans events out to subscribers. Stores embed it to implement Subscribe.
// The zero value is ready to use.
type Hub struct {
	mu   sync.Mutex
	subs map[int]chan Event
	next int
}

// Subscribe implements Store.
func (h *Hub) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 64)
	h.mu.Lock()
	if h.subs == nil {
		h.subs = map[int]chan Event{}
	}
	h.next++
	id := h.next
	h.subs[id] = ch
	h.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs, id)
			h.mu.Unlock()
		})
	}
}

// Publish sends events to every subscriber, dropping them for slow ones:
// subscribers also poll, so a missed event only delays them.
func (h *Hub) Publish(events ...Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range events {
		for _, ch := range h.subs {
			select {
			case ch <- e:
			default:
			}
		}
	}
}

// FinishEvent is the webhook event sent when a run ends with status s.
func FinishEvent(s core.RunStatus) string {
	switch s {
	case core.RunCompleted:
		return core.EventRunCompleted
	case core.RunFailed:
		return core.EventRunFailed
	case core.RunCancelled:
		return core.EventRunCancelled
	}
	return ""
}

// RejectionResult is the tool result the model sees when a human rejects a call.
func RejectionResult(note string) string {
	msg := "The reviewer rejected this action."
	if note != "" {
		msg += " Reviewer note: " + note
	}
	return msg
}

// ChildResult is the parent step's result when its sub-agent run ends.
func ChildResult(o Outcome) (result string, isError bool) {
	if o.Status == core.RunCompleted {
		return o.Output, false
	}
	return fmt.Sprintf("Sub-agent %s: %s", o.Status, o.Error), true
}

// WebhookPayload builds the JSON body sent for a run event. It is built at
// the moment of the change and stored in the outbox, so retries resend
// exactly what happened, not a later state.
func WebhookPayload(event string, r core.Run, step *core.Step, now time.Time) json.RawMessage {
	data := map[string]any{
		"run_id":      r.ID,
		"status":      r.Status,
		"tokens_used": r.TokensUsed,
	}
	if r.Output != "" {
		data["output"] = r.Output
	}
	if r.Error != "" {
		data["error"] = r.Error
		data["failure_kind"] = r.FailureKind
	}
	if len(r.Metadata) > 0 {
		data["metadata"] = r.Metadata
	}
	if r.ParentRunID != "" {
		data["parent_run_id"] = r.ParentRunID
	}
	if step != nil {
		data["step"] = map[string]any{
			"id": step.ID, "seq": step.Seq, "tool_name": step.ToolName, "tool_input": step.ToolInput,
		}
	}
	b, _ := json.Marshal(map[string]any{"type": event, "created_at": now.UTC(), "data": data})
	return b
}

// Blocking reports whether any step still waits on a human or a sub-agent.
func Blocking(steps []core.Step) bool {
	for _, s := range steps {
		if s.Status == core.StepPendingApproval || s.Status == core.StepWaitingChild {
			return true
		}
	}
	return false
}

// HasWork reports whether a step is ready to execute (an approved tool call).
func HasWork(steps []core.Step) bool {
	for _, s := range steps {
		if s.Status == core.StepApproved {
			return true
		}
	}
	return false
}
