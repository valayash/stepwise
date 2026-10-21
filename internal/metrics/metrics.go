// Package metrics is a small, dependency-free Prometheus exporter: labelled
// counters, histograms and computed gauges, served in the Prometheus text
// exposition format at /metrics.
package metrics

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds metric families.
type Registry struct {
	mu       sync.Mutex
	families []family
}

type family interface {
	write(sb *strings.Builder)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f family) {
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
}

// Handler serves the registry in Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var sb strings.Builder
		r.mu.Lock()
		fams := append([]family(nil), r.families...)
		r.mu.Unlock()
		for _, f := range fams {
			f.write(&sb)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(sb.String()))
	})
}

func labelKey(vals []string) string { return strings.Join(vals, "\xff") }

func formatLabels(names, vals []string, extra ...string) string {
	if len(names) == 0 && len(extra) == 0 {
		return ""
	}
	parts := make([]string, 0, len(names)+1)
	for i, n := range names {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, n, escape(vals[i])))
	}
	if len(extra) == 2 {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, extra[0], extra[1]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func num(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// ---- counters ----

// CounterVec is a family of monotonically increasing counters.
type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	vals       map[string]float64
	keys       map[string][]string
}

// NewCounterVec registers a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labels, vals: map[string]float64{}, keys: map[string][]string{}}
	r.add(c)
	return c
}

// Add adds v (>= 0) to the counter with these label values.
func (c *CounterVec) Add(v float64, labelVals ...string) {
	if c == nil || v < 0 || len(labelVals) != len(c.labels) {
		return
	}
	k := labelKey(labelVals)
	c.mu.Lock()
	c.vals[k] += v
	c.keys[k] = labelVals
	c.mu.Unlock()
}

// Inc adds one.
func (c *CounterVec) Inc(labelVals ...string) { c.Add(1, labelVals...) }

// Value returns a counter's current value (tests).
func (c *CounterVec) Value(labelVals ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.vals[labelKey(labelVals)]
}

func (c *CounterVec) write(sb *strings.Builder) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
	for _, k := range sortedKeys(c.vals) {
		fmt.Fprintf(sb, "%s%s %s\n", c.name, formatLabels(c.labels, c.keys[k]), num(c.vals[k]))
	}
}

// ---- histograms ----

// HistogramVec is a family of histograms.
type HistogramVec struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*hist
}

type hist struct {
	labelVals []string
	counts    []uint64
	sum       float64
	count     uint64
}

// DefaultBuckets suit request latencies in seconds.
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// NewHistogramVec registers a histogram family.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	h := &HistogramVec{name: name, help: help, labels: labels, buckets: buckets, series: map[string]*hist{}}
	r.add(h)
	return h
}

// Observe records v.
func (h *HistogramVec) Observe(v float64, labelVals ...string) {
	if h == nil || len(labelVals) != len(h.labels) {
		return
	}
	k := labelKey(labelVals)
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[k]
	if !ok {
		s = &hist{labelVals: labelVals, counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	for i, b := range h.buckets {
		if v <= b {
			s.counts[i]++
		}
	}
	s.sum += v
	s.count++
}

func (h *HistogramVec) write(sb *strings.Builder) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	keys := make([]string, 0, len(h.series))
	for k := range h.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := h.series[k]
		for i, b := range h.buckets {
			fmt.Fprintf(sb, "%s_bucket%s %d\n", h.name, formatLabels(h.labels, s.labelVals, "le", num(b)), s.counts[i])
		}
		fmt.Fprintf(sb, "%s_bucket%s %d\n", h.name, formatLabels(h.labels, s.labelVals, "le", "+Inf"), s.count)
		fmt.Fprintf(sb, "%s_sum%s %s\n", h.name, formatLabels(h.labels, s.labelVals), num(s.sum))
		fmt.Fprintf(sb, "%s_count%s %d\n", h.name, formatLabels(h.labels, s.labelVals), s.count)
	}
}

// ---- computed gauges ----

// Sample is one gauge value.
type Sample struct {
	LabelVals []string
	Value     float64
}

type gaugeFunc struct {
	name, help string
	labels     []string
	fn         func() []Sample
}

// NewGaugeFunc registers a gauge computed at scrape time.
func (r *Registry) NewGaugeFunc(name, help string, fn func() []Sample, labels ...string) {
	r.add(&gaugeFunc{name: name, help: help, labels: labels, fn: fn})
}

func (g *gaugeFunc) write(sb *strings.Builder) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
	for _, s := range g.fn() {
		if len(s.LabelVals) == len(g.labels) {
			fmt.Fprintf(sb, "%s%s %s\n", g.name, formatLabels(g.labels, s.LabelVals), num(s.Value))
		}
	}
}

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
