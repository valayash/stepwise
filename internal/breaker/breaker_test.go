package breaker

import (
	"testing"
	"time"
)

func TestBreakerLifecycle(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(3, 10*time.Second, func() time.Time { return now })

	for i := 0; i < 2; i++ {
		b.Failure()
	}
	if ok, _ := b.Allow(); !ok {
		t.Fatal("should stay closed below the threshold")
	}
	b.Failure()
	if ok, wait := b.Allow(); ok || wait != 10*time.Second {
		t.Fatalf("should be open with 10s wait, got ok=%v wait=%v", ok, wait)
	}

	now = now.Add(10 * time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("state = %v, want half_open", b.State())
	}
	if ok, _ := b.Allow(); !ok {
		t.Fatal("half-open should let one probe through")
	}
	if ok, _ := b.Allow(); ok {
		t.Fatal("only one probe at a time")
	}
	b.Failure() // probe failed
	if ok, _ := b.Allow(); ok {
		t.Fatal("failed probe should reopen")
	}

	now = now.Add(10 * time.Second)
	b.Allow()
	b.Success()
	if b.State() != Closed {
		t.Fatalf("successful probe should close, state = %v", b.State())
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	b := New(3, time.Second, nil)
	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	b.Failure()
	if ok, _ := b.Allow(); !ok {
		t.Fatal("failures should not accumulate across a success")
	}
}

func TestSetIsPerKey(t *testing.T) {
	s := NewSet(1, time.Minute, nil)
	s.Get("anthropic").Failure()
	if ok, _ := s.Get("openai").Allow(); !ok {
		t.Fatal("one provider's outage tripped another's breaker")
	}
	if st := s.States()["anthropic"]; st != Open {
		t.Errorf("anthropic state = %v", st)
	}
}

func TestBackoffGrowsWithJitterAndCap(t *testing.T) {
	base, max := 100*time.Millisecond, 2*time.Second
	for attempt, ceil := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 10: max} {
		for i := 0; i < 200; i++ {
			d := Backoff(attempt, base, max)
			if d < ceil/2 || d > ceil {
				t.Fatalf("Backoff(%d) = %v, want within [%v, %v]", attempt, d, ceil/2, ceil)
			}
		}
	}
}
