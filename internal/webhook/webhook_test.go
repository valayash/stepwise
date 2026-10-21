package webhook

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/metrics"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/store/memory"
)

var ctx = context.Background()

func TestSignAndVerify(t *testing.T) {
	secret, body, now := []byte("s3cret"), []byte(`{"type":"run.completed"}`), time.Unix(1_800_000_000, 0)
	h := Sign(secret, now, body)

	if err := Verify(secret, h, body, now.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := Verify([]byte("wrong"), h, body, now, time.Minute); err != ErrBadSignature {
		t.Errorf("wrong secret: %v", err)
	}
	if err := Verify(secret, h, []byte(`{"tampered":true}`), now, time.Minute); err != ErrBadSignature {
		t.Errorf("tampered body: %v", err)
	}
	if err := Verify(secret, h, body, now.Add(time.Hour), 5*time.Minute); err != ErrStale {
		t.Errorf("replayed later: %v", err)
	}
	if err := Verify(secret, "garbage", body, now, time.Minute); err != ErrBadSignature {
		t.Errorf("garbage header: %v", err)
	}
}

// finishedRun creates a completed run with a webhook URL, which queues a delivery.
func finishedRun(t *testing.T, s store.Store, url string) core.Run {
	t.Helper()
	r, _, _ := s.CreateRun(ctx, core.NewRun{Spec: core.AgentSpec{Model: "mock"}, Input: "x", WebhookURL: url})
	c, _ := s.ClaimRuns(ctx, "w", 1, time.Minute)
	if err := s.Finish(ctx, core.Fence{RunID: r.ID, Token: c[0].LeaseToken}, store.Outcome{Status: core.RunCompleted, Output: "ok"}); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDeliveryIsSignedAndRetried(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var gotSig, gotEvent string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway) // first attempt fails
			return
		}
		mu.Lock()
		gotSig, gotEvent = r.Header.Get(HeaderSignature), r.Header.Get(HeaderEvent)
		gotBody, _ = io.ReadAll(r.Body)
		mu.Unlock()
	}))
	defer srv.Close()

	s := memory.New(nil)
	run := finishedRun(t, s, srv.URL)
	reg := metrics.NewRegistry()
	d := &Dispatcher{Store: s, Secret: []byte("k"), BackoffBase: time.Millisecond, BackoffMax: 2 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	d.RegisterMetrics(reg)

	if n, _ := d.DeliverOnce(ctx); n != 1 {
		t.Fatalf("first pass attempted %d", n)
	}
	time.Sleep(5 * time.Millisecond)
	if n, _ := d.DeliverOnce(ctx); n != 1 {
		t.Fatalf("retry pass attempted %d", n)
	}

	mu.Lock()
	defer mu.Unlock()
	if err := Verify([]byte("k"), gotSig, gotBody, time.Now(), time.Minute); err != nil {
		t.Errorf("receiver could not verify: %v", err)
	}
	if gotEvent != core.EventRunCompleted {
		t.Errorf("event = %q", gotEvent)
	}
	ds, _ := s.ListDeliveries(ctx, run.ID)
	if ds[0].Status != store.DeliveryDelivered || ds[0].Attempts != 2 {
		t.Errorf("delivery = %+v", ds[0])
	}
	if d.sent.Value("retry") != 1 || d.sent.Value("delivered") != 1 {
		t.Errorf("metrics retry=%v delivered=%v", d.sent.Value("retry"), d.sent.Value("delivered"))
	}
}

func TestDeliveryGoesDeadAfterMaxAttempts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s := memory.New(nil)
	run := finishedRun(t, s, srv.URL)
	d := &Dispatcher{Store: s, MaxAttempts: 3, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	for i := 0; i < 6; i++ {
		_, _ = d.DeliverOnce(ctx)
		time.Sleep(3 * time.Millisecond)
	}
	ds, _ := s.ListDeliveries(ctx, run.ID)
	if ds[0].Status != store.DeliveryDead || ds[0].Attempts != 3 || ds[0].LastError == "" {
		t.Errorf("delivery = %+v", ds[0])
	}
}
