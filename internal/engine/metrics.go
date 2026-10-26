package engine

import (
	"github.com/valayash/stepwise/internal/breaker"
	"github.com/valayash/stepwise/internal/metrics"
)

// Metrics are the engine's Prometheus series.
type Metrics struct {
	claims          *metrics.CounterVec
	steps           *metrics.CounterVec
	replayed        *metrics.CounterVec
	llmCalls        *metrics.CounterVec
	llmSeconds      *metrics.HistogramVec
	toolCalls       *metrics.CounterVec
	toolSeconds     *metrics.HistogramVec
	tokens          *metrics.CounterVec
	rateLimited     *metrics.CounterVec
	breakerRejected *metrics.CounterVec
	retries         *metrics.CounterVec
	finished        *metrics.CounterVec
	approvals       *metrics.CounterVec
	children        *metrics.CounterVec
	leaseLost       *metrics.CounterVec
	triaged         *metrics.CounterVec
}

// NewMetrics registers the engine's metrics on reg.
func NewMetrics(reg *metrics.Registry, breakers *breaker.Set) *Metrics {
	m := &Metrics{
		claims:          reg.NewCounterVec("stepwise_runs_claimed_total", "Runs claimed by a worker."),
		steps:           reg.NewCounterVec("stepwise_steps_completed_total", "Steps completed, by kind.", "kind"),
		replayed:        reg.NewCounterVec("stepwise_steps_reexecuted_total", "Interrupted steps executed again after a crash or retry.", "kind"),
		llmCalls:        reg.NewCounterVec("stepwise_llm_calls_total", "Model calls by provider and outcome.", "provider", "outcome"),
		llmSeconds:      reg.NewHistogramVec("stepwise_llm_call_seconds", "Model call latency.", nil, "provider"),
		toolCalls:       reg.NewCounterVec("stepwise_tool_calls_total", "Tool calls by type and outcome.", "type", "outcome"),
		toolSeconds:     reg.NewHistogramVec("stepwise_tool_call_seconds", "Tool call latency.", nil, "type"),
		tokens:          reg.NewCounterVec("stepwise_llm_tokens_total", "Tokens used, by model and direction (cached_input is the part of input served from the provider's cache).", "model", "direction"),
		rateLimited:     reg.NewCounterVec("stepwise_rate_limited_total", "Model calls deferred by the shared rate limiter.", "model"),
		breakerRejected: reg.NewCounterVec("stepwise_breaker_rejected_total", "Model calls deferred because a circuit was open.", "provider"),
		retries:         reg.NewCounterVec("stepwise_retries_total", "Runs handed back for a retry, by reason.", "reason"),
		finished:        reg.NewCounterVec("stepwise_runs_finished_total", "Runs finished, by status.", "status"),
		approvals:       reg.NewCounterVec("stepwise_approvals_total", "Approval requests and executions.", "event"),
		children:        reg.NewCounterVec("stepwise_subagents_started_total", "Sub-agent runs started."),
		leaseLost:       reg.NewCounterVec("stepwise_leases_lost_total", "Claims abandoned because the lease was lost."),
		triaged:         reg.NewCounterVec("stepwise_triage_runs_started_total", "Triage runs started for failed runs."),
	}
	if breakers != nil {
		reg.NewGaugeFunc("stepwise_breaker_state", "Circuit state per provider (0 closed, 1 open, 2 half-open).", func() []metrics.Sample {
			var out []metrics.Sample
			for name, st := range breakers.States() {
				out = append(out, metrics.Sample{LabelVals: []string{name}, Value: float64(st)})
			}
			return out
		}, "provider")
	}
	return m
}
