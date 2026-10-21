package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisPair returns two limiters that share buckets, as two worker
// processes would. Set STEPWISE_TEST_REDIS_URL to run these tests.
func redisPair(t *testing.T, rules Rules) (*Redis, *Redis) {
	t.Helper()
	url := os.Getenv("STEPWISE_TEST_REDIS_URL")
	if url == "" {
		t.Skip("STEPWISE_TEST_REDIS_URL not set")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	prefix := "test:" + hex.EncodeToString(b[:]) // isolate each test's keys
	c1, c2 := redis.NewClient(opt), redis.NewClient(opt)
	t.Cleanup(func() { c1.Close(); c2.Close() })
	return NewRedisWithClient(c1, rules, prefix), NewRedisWithClient(c2, rules, prefix)
}

func TestRedisSharesRequestBudgetAcrossProcesses(t *testing.T) {
	a, b := redisPair(t, Rules{{Prefix: "claude-", RPM: 4}})
	ctx := context.Background()
	for i, l := range []*Redis{a, b, a, b} {
		if w, err := l.Reserve(ctx, "claude-x", 0); err != nil || w != 0 {
			t.Fatalf("request %d: wait=%v err=%v", i, w, err)
		}
	}
	w, err := b.Reserve(ctx, "claude-x", 0)
	if err != nil || w <= 0 || w > 16*time.Second {
		t.Fatalf("5th request across both processes: wait=%v err=%v, want ~15s", w, err)
	}
}

func TestRedisReserveAndSettleTokens(t *testing.T) {
	a, b := redisPair(t, Rules{{Prefix: "m", TPM: 6000}})
	ctx := context.Background()
	if w, _ := a.Reserve(ctx, "m", 6000); w != 0 {
		t.Fatalf("full reservation blocked: %v", w)
	}
	if w, _ := b.Reserve(ctx, "m", 3000); w == 0 {
		t.Fatal("the other process could take tokens that were already reserved")
	}
	if err := a.Settle(ctx, "m", -5000); err != nil { // actually used 1,000
		t.Fatal(err)
	}
	if w, _ := b.Reserve(ctx, "m", 3000); w != 0 {
		t.Fatalf("refunded tokens not visible to the other process: wait=%v", w)
	}
}

func TestRedisConcurrentReservationsNeverOverspend(t *testing.T) {
	a, b := redisPair(t, Rules{{Prefix: "m", TPM: 1000}})
	ctx := context.Background()
	var mu sync.Mutex
	granted := 0
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		l := a
		if i%2 == 1 {
			l = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w, err := l.Reserve(ctx, "m", 100); err == nil && w == 0 {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// Refill during the test adds at most a token or two, never a whole 100.
	if granted != 10 {
		t.Errorf("granted %d reservations of 100 from a 1,000-token bucket, want exactly 10", granted)
	}
}

func TestRedisUnmatchedModelSkipsRedis(t *testing.T) {
	a, _ := redisPair(t, Rules{{Prefix: "claude-", RPM: 1}})
	if w, err := a.Reserve(context.Background(), "gpt-x", 1e6); err != nil || w != 0 {
		t.Errorf("unmatched model: wait=%v err=%v", w, err)
	}
}

func TestRedisDownReturnsError(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	defer c.Close()
	l := NewRedisWithClient(c, Rules{{Prefix: "m", RPM: 1}}, "x")
	if _, err := l.Reserve(context.Background(), "m", 1); err == nil {
		t.Error("an unreachable Redis must surface an error (the engine then fails open)")
	}
}
