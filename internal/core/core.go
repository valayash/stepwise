// Package core holds Stepwise's domain types: runs, steps, agent specs and
// the errors shared by the store, the engine and the API.
package core

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/valayash/stepwise/internal/provider"
)

// RunStatus is where a run is in its lifecycle.
//
//	queued ──claim──▶ running ──▶ completed | failed
//	   ▲                 │
//	   └── retry/yield ──┤
//	                     ▼
//	                  waiting (approval or sub-agents) ──resolved──▶ queued
//
// cancelled can be reached from any non-terminal state.
type RunStatus string

const (
	RunQueued    RunStatus = "queued"
	RunRunning   RunStatus = "running"
	RunWaiting   RunStatus = "waiting"
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

// Terminal reports whether a run will never execute again on its own.
func (s RunStatus) Terminal() bool {
	return s == RunCompleted || s == RunFailed || s == RunCancelled
}

// FailureKind explains why a run failed.
type FailureKind string

const (
	FailExhausted FailureKind = "retries_exhausted" // transient errors kept happening
	FailFatal     FailureKind = "fatal"             // non-retryable error (bad request, unknown model)
	FailBudget    FailureKind = "budget_exceeded"   // token budget used up
	FailMaxTurns  FailureKind = "max_turns"         // agent looped too long
	FailChild     FailureKind = "child_failed"      // reserved for future strict sub-agent mode
)

// StepKind is what a step did.
type StepKind string

const (
	StepLLM  StepKind = "llm"
	StepTool StepKind = "tool"
)

// StepStatus is where a step is.
type StepStatus string

const (
	StepRunning         StepStatus = "running"          // started; may be re-executed after a crash
	StepCompleted       StepStatus = "completed"        // result saved; never executed again
	StepPendingApproval StepStatus = "pending_approval" // waiting for a human decision
	StepApproved        StepStatus = "approved"         // approved; executes on the next claim
	StepWaitingChild    StepStatus = "waiting_child"    // waiting for a sub-agent run
)

// ToolType says how a tool is executed.
type ToolType string

const (
	ToolHTTP    ToolType = "http"    // POST the JSON input to URL
	ToolMCP     ToolType = "mcp"     // call a tool on an MCP server at URL
	ToolAgent   ToolType = "agent"   // run a sub-agent and return its answer
	ToolBuiltin ToolType = "builtin" // in-process tool (used by the triage agent)
)

// ToolSpec defines one tool an agent may call.
//
// An MCP entry with an empty Name is a *server reference*: at run creation it
// expands into one ToolSpec per tool the server lists (optionally filtered by
// Include), so a run always records the exact tools it was given.
type ToolSpec struct {
	Type        ToolType        `json:"type"`
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	URL         string          `json:"url,omitempty"`
	Include     []string        `json:"include,omitempty"` // MCP server reference filter
	Agent       *AgentSpec      `json:"agent,omitempty"`   // sub-agent definition
	// Headers are sent with every HTTP or MCP request for this tool. Values may
	// reference ${STEPWISE_SECRET_*} variables, resolved at call time so secrets
	// are never stored with the run.
	Headers     map[string]string `json:"headers,omitempty"`
	TimeoutSecs int               `json:"timeout_seconds,omitempty"`
}

// AgentSpec defines an agent: model, instructions, tools and limits.
type AgentSpec struct {
	Model            string     `json:"model"`
	System           string     `json:"system,omitempty"`
	Tools            []ToolSpec `json:"tools,omitempty"`
	RequireApproval  []string   `json:"require_approval,omitempty"` // tool names that need a human OK
	MaxTurns         int        `json:"max_turns,omitempty"`        // LLM calls per run, default 20
	MaxTokensPerCall int        `json:"max_tokens_per_call,omitempty"`
	Temperature      *float64   `json:"temperature,omitempty"`
}

// Defaults applied when a spec leaves a limit unset.
const (
	DefaultMaxTurns         = 20
	DefaultMaxTokensPerCall = 1024
	DefaultBudgetTokens     = 200_000
)

// WithDefaults fills unset limits.
func (a AgentSpec) WithDefaults() AgentSpec {
	if a.MaxTurns <= 0 {
		a.MaxTurns = DefaultMaxTurns
	}
	if a.MaxTokensPerCall <= 0 {
		a.MaxTokensPerCall = DefaultMaxTokensPerCall
	}
	return a
}

// Tool finds a tool by name.
func (a AgentSpec) Tool(name string) (ToolSpec, bool) {
	for _, t := range a.Tools {
		if t.Name == name {
			return t, true
		}
	}
	return ToolSpec{}, false
}

// NeedsApproval reports whether calling the named tool needs a human decision.
func (a AgentSpec) NeedsApproval(name string) bool {
	for _, n := range a.RequireApproval {
		if n == name || n == "*" {
			return true
		}
	}
	return false
}

// ToolDefs converts the spec's tools into the provider-neutral definitions sent to the model.
func (a AgentSpec) ToolDefs() []provider.ToolDef {
	defs := make([]provider.ToolDef, 0, len(a.Tools))
	for _, t := range a.Tools {
		schema := t.InputSchema
		if len(schema) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		if t.Type == ToolAgent && len(t.InputSchema) == 0 {
			schema = json.RawMessage(`{"type":"object","properties":{"task":{"type":"string","description":"What the sub-agent should do"}},"required":["task"]}`)
		}
		defs = append(defs, provider.ToolDef{Name: t.Name, Description: t.Description, InputSchema: schema})
	}
	return defs
}

// Run is one durable agent execution.
type Run struct {
	ID             string          `json:"id"`
	Status         RunStatus       `json:"status"`
	Spec           AgentSpec       `json:"agent"`
	Input          string          `json:"input"`
	Output         string          `json:"output,omitempty"`
	Error          string          `json:"error,omitempty"`
	FailureKind    FailureKind     `json:"failure_kind,omitempty"`
	BudgetTokens   int             `json:"budget_tokens"`
	TokensUsed     int             `json:"tokens_used"`
	Attempts       int             `json:"attempts"` // consecutive failed tries of the current step
	Claims         int             `json:"claims"`   // how many times a worker picked this run up
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseToken     int64           `json:"-"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	AvailableAt    time.Time       `json:"available_at"`
	ParentRunID    string          `json:"parent_run_id,omitempty"`
	ParentStepID   int64           `json:"parent_step_id,omitempty"`
	WebhookURL     string          `json:"webhook_url,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Triage         json.RawMessage `json:"triage,omitempty"` // diagnosis recorded by the triage agent
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

// Approval records a human decision on a tool call.
type Approval struct {
	Approved  bool      `json:"approved"`
	Note      string    `json:"note,omitempty"`
	DecidedBy string    `json:"decided_by,omitempty"`
	DecidedAt time.Time `json:"decided_at"`
}

// Step is one saved action inside a run.
type Step struct {
	ID         int64      `json:"id"`
	RunID      string     `json:"run_id"`
	Seq        int        `json:"seq"` // position in the run, from 1
	Kind       StepKind   `json:"kind"`
	Status     StepStatus `json:"status"`
	Attempts   int        `json:"attempts"` // times this step was actually executed
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolName   string     `json:"tool_name,omitempty"`
	// ToolInput is the JSON the model passed to the tool.
	ToolInput json.RawMessage `json:"tool_input,omitempty"`
	// LLM is the model's response (llm steps only).
	LLM *provider.Response `json:"llm,omitempty"`
	// Result is the tool's output, as shown to the model (tool steps only).
	Result      string     `json:"result,omitempty"`
	IsError     bool       `json:"is_error,omitempty"`
	Approval    *Approval  `json:"approval,omitempty"`
	ChildRunID  string     `json:"child_run_id,omitempty"`
	DurationMs  int64      `json:"duration_ms,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// IdempotencyKey is stable across retries and crash recovery, so a tool that
// honours it performs its side effect once no matter how often it is called.
func (s Step) IdempotencyKey() string {
	if s.ToolCallID != "" {
		return s.RunID + ":" + s.ToolCallID
	}
	return s.RunID + ":step:" + strconv.Itoa(s.Seq)
}

// Tokens is the step's LLM token usage.
func (s Step) Tokens() int {
	if s.LLM == nil {
		return 0
	}
	return s.LLM.InputTokens + s.LLM.OutputTokens
}

// NewRun is what callers submit to start a run.
type NewRun struct {
	Spec           AgentSpec       `json:"agent"`
	Input          string          `json:"input"`
	BudgetTokens   int             `json:"budget_tokens,omitempty"`
	WebhookURL     string          `json:"webhook_url,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	ParentRunID    string          `json:"-"`
	ParentStepID   int64           `json:"-"`
}

// Budget is the run's token budget, or the default when none was set.
func (nr NewRun) Budget() int {
	if nr.BudgetTokens > 0 {
		return nr.BudgetTokens
	}
	return DefaultBudgetTokens
}

// Fence identifies a worker's claim on a run. Every write a worker makes
// carries it; the store rejects writes whose token is stale, so a worker that
// lost its lease (GC pause, network partition) can never overwrite the work of
// the worker that took over.
type Fence struct {
	RunID string
	Token int64
}

// Event types sent to webhooks and live watchers.
const (
	EventRunCompleted       = "run.completed"
	EventRunFailed          = "run.failed"
	EventRunCancelled       = "run.cancelled"
	EventApprovalRequested  = "run.approval_requested"
	EventRunTriaged         = "run.triaged"
	EventChildRunsRequested = "run.waiting_children"
)

// Errors shared across packages.
var (
	ErrNotFound    = errors.New("not found")
	ErrFenced      = errors.New("lease lost: another worker owns this run, or it was cancelled")
	ErrConflict    = errors.New("conflict")
	ErrInvalid     = errors.New("invalid request")
	ErrNotRunnable = errors.New("run is not in a state that allows this")
)

// NewID returns a random 128-bit hex ID.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
