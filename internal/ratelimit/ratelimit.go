// Package ratelimit keeps all workers under a provider's rate limits.
//
// Providers limit both requests per minute (RPM) and tokens per minute (TPM).
// Each rule is a pair of token buckets. A worker reserves one request plus its
// *estimated* tokens before calling the model, then settles the difference
// once the real usage is known. When a bucket is empty the caller gets the
// time to wait instead of blocking, so the engine can put the run back in the
// queue and keep its worker busy with other runs.
package ratelimit

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Rule is the budget for one model family.
type Rule struct {
	Prefix string // model-name prefix this rule covers, e.g. "claude-"
	RPM    int    // requests per minute; 0 = unlimited
	TPM    int    // tokens per minute; 0 = unlimited
}

// Limiter reserves and settles capacity. Implementations: Local (one process)
// and Redis (shared by every worker in the fleet).
type Limiter interface {
	// Reserve takes one request and n tokens for model. It returns 0 when the
	// capacity was taken, or how long to wait before trying again (nothing taken).
	Reserve(ctx context.Context, model string, n int) (time.Duration, error)
	// Settle corrects the token count once real usage is known: delta > 0
	// takes more, delta < 0 gives tokens back.
	Settle(ctx context.Context, model string, delta int) error
	// Pause empties model's buckets for d, e.g. after the provider answered
	// 429: every worker's Reserve then waits about d instead of calling it.
	Pause(ctx context.Context, model string, d time.Duration) error
}

// Rules maps models to rules by longest matching prefix.
type Rules []Rule

// Lookup finds the rule for model.
func (rs Rules) Lookup(model string) (Rule, bool) {
	best, found := Rule{}, false
	for _, r := range rs {
		if strings.HasPrefix(model, r.Prefix) && (!found || len(r.Prefix) > len(best.Prefix)) {
			best, found = r, true
		}
	}
	return best, found
}

// ParseRules reads "claude-=rpm:50,tpm:40000;gpt-=rpm:500,tpm:200000".
func ParseRules(s string) (Rules, error) {
	var out Rules
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		prefix, limits, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("rate limit %q: want prefix=rpm:N,tpm:N", part)
		}
		r := Rule{Prefix: strings.TrimSpace(prefix)}
		for _, kv := range strings.Split(limits, ",") {
			k, v, ok := strings.Cut(strings.TrimSpace(kv), ":")
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if !ok || err != nil || n < 0 {
				return nil, fmt.Errorf("rate limit %q: bad value %q", part, kv)
			}
			switch strings.ToLower(strings.TrimSpace(k)) {
			case "rpm":
				r.RPM = n
			case "tpm":
				r.TPM = n
			default:
				return nil, fmt.Errorf("rate limit %q: unknown key %q (use rpm or tpm)", part, k)
			}
		}
		out = append(out, r)
	}
	return out, nil
}

// bucket is a classic token bucket that refills continuously.
// Level may go negative after Settle: that "debt" is repaid by refill.
type bucket struct {
	capacity float64
	level    float64
	perSec   float64
	last     time.Time
}

func newBucket(perMinute int, now time.Time) *bucket {
	c := float64(perMinute)
	return &bucket{capacity: c, level: c, perSec: c / 60, last: now}
}

func (b *bucket) refill(now time.Time) {
	if el := now.Sub(b.last).Seconds(); el > 0 {
		b.level = math.Min(b.capacity, b.level+el*b.perSec)
		b.last = now
	}
}

// waitFor is how long until the bucket holds n.
func (b *bucket) waitFor(n float64) time.Duration {
	if b.level >= n {
		return 0
	}
	return time.Duration((n - b.level) / b.perSec * float64(time.Second))
}

// Local is an in-process limiter: correct for a single process, but each
// process has its own budget. Use Redis when running several workers.
type Local struct {
	rules Rules
	now   func() time.Time

	mu      sync.Mutex
	buckets map[string][2]*bucket // prefix → {requests, tokens}
}

// NewLocal builds a Local limiter. now may be nil.
func NewLocal(rules Rules, now func() time.Time) *Local {
	if now == nil {
		now = time.Now
	}
	return &Local{rules: rules, now: now, buckets: map[string][2]*bucket{}}
}

func (l *Local) get(r Rule, now time.Time) [2]*bucket {
	bs, ok := l.buckets[r.Prefix]
	if !ok {
		if r.RPM > 0 {
			bs[0] = newBucket(r.RPM, now)
		}
		if r.TPM > 0 {
			bs[1] = newBucket(r.TPM, now)
		}
		l.buckets[r.Prefix] = bs
	}
	for _, b := range bs {
		if b != nil {
			b.refill(now)
		}
	}
	return bs
}

// Reserve implements Limiter.
func (l *Local) Reserve(_ context.Context, model string, n int) (time.Duration, error) {
	r, ok := l.rules.Lookup(model)
	if !ok {
		return 0, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	bs := l.get(r, l.now())
	req, tok := bs[0], bs[1]

	need := float64(n)
	if tok != nil && need > tok.capacity {
		need = tok.capacity // one huge request can still run once the bucket is full
	}
	var wait time.Duration
	if req != nil {
		wait = max(wait, req.waitFor(1))
	}
	if tok != nil {
		wait = max(wait, tok.waitFor(need))
	}
	if wait > 0 {
		return wait, nil
	}
	if req != nil {
		req.level--
	}
	if tok != nil {
		tok.level -= need
	}
	return 0, nil
}

// Settle implements Limiter.
func (l *Local) Settle(_ context.Context, model string, delta int) error {
	r, ok := l.rules.Lookup(model)
	if !ok || r.TPM == 0 || delta == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	tok := l.get(r, l.now())[1]
	tok.level = math.Min(tok.capacity, tok.level-float64(delta))
	return nil
}

// Pause implements Limiter. It reuses the debt Settle allows: refill
// brings the level back to zero after d.
func (l *Local) Pause(_ context.Context, model string, d time.Duration) error {
	r, ok := l.rules.Lookup(model)
	if !ok {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, b := range l.get(r, l.now()) {
		if b != nil {
			b.level = math.Min(b.level, -d.Seconds()*b.perSec)
		}
	}
	return nil
}

// Unlimited never limits. Useful in tests.
type Unlimited struct{}

// Reserve implements Limiter.
func (Unlimited) Reserve(context.Context, string, int) (time.Duration, error) { return 0, nil }

// Settle implements Limiter.
func (Unlimited) Settle(context.Context, string, int) error { return nil }

// Pause implements Limiter.
func (Unlimited) Pause(context.Context, string, time.Duration) error { return nil }

// EstimateTokens guesses a request's cost before sending it: about four
// characters per token for the prompt, plus the full output allowance.
// Over-estimating is safe: Settle returns the difference.
func EstimateTokens(promptChars, maxOutput int) int {
	return promptChars/4 + 1 + maxOutput
}
