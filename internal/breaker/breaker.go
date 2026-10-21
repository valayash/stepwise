// Package breaker implements a circuit breaker and retry backoff.
//
// A breaker stops calling a provider that keeps failing, so workers don't
// burn retries (and the provider's patience) during an outage:
//
//	closed ──N failures in a row──▶ open ──cooldown──▶ half-open
//	   ▲                                                  │
//	   └──────────── probe succeeds ◀─────────────────────┤
//	                 probe fails ──▶ open again ◀─────────┘
package breaker

import (
	"math/rand/v2"
	"sync"
	"time"
)

// State of a breaker.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	return [...]string{"closed", "open", "half_open"}[s]
}

// Breaker guards one dependency. Safe for concurrent use.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool
}

// New returns a closed breaker. now may be nil.
func New(threshold int, cooldown time.Duration, now func() time.Time) *Breaker {
	if threshold <= 0 {
		threshold = 5
	}
	if now == nil {
		now = time.Now
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: now}
}

// Allow reports whether a call may proceed. When it may not, retryIn says
// when the breaker will next let a probe through.
func (b *Breaker) Allow() (ok bool, retryIn time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		wait := b.cooldown - b.now().Sub(b.openedAt)
		if wait > 0 {
			return false, wait
		}
		b.state = HalfOpen
		b.probing = false
		fallthrough
	case HalfOpen:
		if b.probing {
			return false, b.cooldown / 4 // one probe at a time
		}
		b.probing = true
		return true, 0
	default:
		return true, 0
	}
}

// Success records a successful call.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state, b.failures, b.probing = Closed, 0, false
}

// Abort releases a half-open probe slot that was granted but never used
// (the caller backed off for another reason, such as a rate limit).
func (b *Breaker) Abort() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == HalfOpen {
		b.probing = false
	}
}

// Failure records a failed call.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == HalfOpen || b.failures >= b.threshold {
		b.state, b.openedAt, b.probing = Open, b.now(), false
	}
}

// State returns the current state (for metrics).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cooldown {
		return HalfOpen
	}
	return b.state
}

// Set holds one breaker per key (provider name), created on first use.
type Set struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu sync.Mutex
	m  map[string]*Breaker
}

// NewSet builds a breaker set.
func NewSet(threshold int, cooldown time.Duration, now func() time.Time) *Set {
	return &Set{threshold: threshold, cooldown: cooldown, now: now, m: map[string]*Breaker{}}
}

// Get returns the breaker for key.
func (s *Set) Get(key string) *Breaker {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[key]
	if !ok {
		b = New(s.threshold, s.cooldown, s.now)
		s.m[key] = b
	}
	return b
}

// States reports every breaker's state, for metrics.
func (s *Set) States() map[string]State {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]State, len(s.m))
	for k, b := range s.m {
		out[k] = b.State()
	}
	return out
}

// Backoff returns the delay before retry number attempt (1-based):
// exponential growth capped at max, with "equal jitter" — half fixed, half
// random — so retries spread out instead of arriving in synchronized waves,
// while never collapsing to zero.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
