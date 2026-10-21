package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

var ctx = context.Background()

func TestParseRules(t *testing.T) {
	rs, err := ParseRules(" claude-=rpm:50,tpm:40000 ; claude-opus=tpm:1000;gpt-=rpm:10")
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 3 || rs[0] != (Rule{"claude-", 50, 40000}) || rs[2] != (Rule{"gpt-", 10, 0}) {
		t.Fatalf("rules = %+v", rs)
	}
	if r, _ := rs.Lookup("claude-opus-5"); r.Prefix != "claude-opus" {
		t.Errorf("longest prefix should win, got %q", r.Prefix)
	}
	if _, ok := rs.Lookup("mistral"); ok {
		t.Error("unmatched model should have no rule")
	}
	for _, bad := range []string{"claude", "claude-=rpm", "claude-=qps:3", "x=rpm:-1"} {
		if _, err := ParseRules(bad); err == nil {
			t.Errorf("ParseRules(%q) accepted", bad)
		}
	}
}

func TestTokenBucketWaitsAndRefills(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := NewLocal(Rules{{Prefix: "m", TPM: 600}}, c.now) // 10 tokens/second

	if w, _ := l.Reserve(ctx, "m1", 500); w != 0 {
		t.Fatalf("first reserve should pass, wait=%v", w)
	}
	w, _ := l.Reserve(ctx, "m1", 200) // 100 left, need 200 → 100 short at 10/s
	if w != 10*time.Second {
		t.Fatalf("wait = %v, want 10s", w)
	}
	c.add(10 * time.Second)
	if w, _ := l.Reserve(ctx, "m1", 200); w != 0 {
		t.Fatalf("after refill wait = %v", w)
	}
}

func TestSettleRefundsAndCharges(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := NewLocal(Rules{{Prefix: "m", TPM: 600}}, c.now)

	_, _ = l.Reserve(ctx, "m", 600) // estimate everything
	_ = l.Settle(ctx, "m", -500)    // actually used 100
	if w, _ := l.Reserve(ctx, "m", 500); w != 0 {
		t.Fatalf("refund not applied, wait=%v", w)
	}
	_ = l.Settle(ctx, "m", 100) // a call went over its estimate: bucket goes into debt
	if w, _ := l.Reserve(ctx, "m", 1); w == 0 {
		t.Fatal("debt should force a wait")
	}
}

func TestRequestsPerMinute(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := NewLocal(Rules{{Prefix: "m", RPM: 2}}, c.now)
	for i := 0; i < 2; i++ {
		if w, _ := l.Reserve(ctx, "m", 0); w != 0 {
			t.Fatalf("request %d blocked", i)
		}
	}
	if w, _ := l.Reserve(ctx, "m", 0); w != 30*time.Second {
		t.Fatalf("third request wait = %v, want 30s", w)
	}
}

func TestOversizedRequestRunsWhenFull(t *testing.T) {
	l := NewLocal(Rules{{Prefix: "m", TPM: 100}}, nil)
	if w, _ := l.Reserve(ctx, "m", 1000); w != 0 {
		t.Fatalf("a request bigger than the bucket must still be allowed when full, wait=%v", w)
	}
}

func TestUnmatchedModelIsUnlimited(t *testing.T) {
	l := NewLocal(Rules{{Prefix: "claude-", RPM: 1}}, nil)
	for i := 0; i < 100; i++ {
		if w, _ := l.Reserve(ctx, "gpt-x", 1e6); w != 0 {
			t.Fatal("unmatched model was limited")
		}
	}
}

func TestConcurrentReservationsNeverOverspend(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	l := NewLocal(Rules{{Prefix: "m", TPM: 1000}}, c.now)
	var mu sync.Mutex
	granted := 0
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w, _ := l.Reserve(ctx, "m", 100); w == 0 {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if granted != 10 {
		t.Errorf("granted %d reservations of 100 from a 1000 bucket, want exactly 10", granted)
	}
}
