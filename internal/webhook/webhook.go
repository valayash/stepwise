// Package webhook delivers run events from the transactional outbox.
//
// The store writes a delivery row in the same transaction as the state
// change it describes, so a webhook is never lost or sent for a change that
// rolled back. This dispatcher sends pending rows, signs them, and retries
// with backoff until the receiver answers 2xx or attempts run out.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valayash/stepwise/internal/breaker"
	"github.com/valayash/stepwise/internal/metrics"
	"github.com/valayash/stepwise/internal/store"
)

// Header names.
const (
	HeaderSignature = "Stepwise-Signature"
	HeaderEvent     = "Stepwise-Event"
	HeaderDelivery  = "Stepwise-Delivery-Id"
)

// Dispatcher sends outbox deliveries.
type Dispatcher struct {
	Store       store.Store
	Secret      []byte
	HTTP        *http.Client
	Log         *slog.Logger
	Interval    time.Duration // idle poll (default 1s)
	MaxAttempts int           // before a delivery is dead (default 8)
	BackoffBase time.Duration // default 2s
	BackoffMax  time.Duration // default 30m
	Concurrency int           // parallel sends (default 8)

	sent *metrics.CounterVec
	once sync.Once
}

// RegisterMetrics adds delivery counters to reg.
func (d *Dispatcher) RegisterMetrics(reg *metrics.Registry) {
	d.sent = reg.NewCounterVec("stepwise_webhook_deliveries_total", "Webhook delivery attempts by outcome.", "outcome")
}

func (d *Dispatcher) init() {
	d.once.Do(func() {
		if d.HTTP == nil {
			d.HTTP = &http.Client{Timeout: 10 * time.Second}
		}
		if d.Log == nil {
			d.Log = slog.Default()
		}
		if d.Interval <= 0 {
			d.Interval = time.Second
		}
		if d.MaxAttempts <= 0 {
			d.MaxAttempts = 8
		}
		if d.BackoffBase <= 0 {
			d.BackoffBase = 2 * time.Second
		}
		if d.BackoffMax <= 0 {
			d.BackoffMax = 30 * time.Minute
		}
		if d.Concurrency <= 0 {
			d.Concurrency = 8
		}
	})
}

// Run delivers until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	d.init()
	events, stop := d.Store.Subscribe()
	defer stop()
	for ctx.Err() == nil {
		n, err := d.DeliverOnce(ctx)
		if err != nil && ctx.Err() == nil {
			d.Log.Warn("webhook claim failed", "err", err)
		}
		if n > 0 {
			continue // there may be more
		}
		select {
		case <-ctx.Done():
		case <-events:
		case <-time.After(d.Interval):
		}
	}
}

// DeliverOnce claims and sends one batch. It returns how many it attempted.
func (d *Dispatcher) DeliverOnce(ctx context.Context) (int, error) {
	d.init()
	batch, err := d.Store.ClaimDeliveries(ctx, d.Concurrency, time.Minute)
	if err != nil || len(batch) == 0 {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, del := range batch {
		wg.Add(1)
		go func(del store.Delivery) {
			defer wg.Done()
			d.deliver(ctx, del)
		}(del)
	}
	wg.Wait()
	return len(batch), nil
}

func (d *Dispatcher) deliver(ctx context.Context, del store.Delivery) {
	err := d.send(ctx, del)
	bg := context.WithoutCancel(ctx)
	if err == nil {
		d.count("delivered")
		if cerr := d.Store.CompleteDelivery(bg, del.ID); cerr != nil {
			d.Log.Warn("could not mark webhook delivered", "delivery", del.ID, "err", cerr)
		}
		return
	}
	dead := del.Attempts >= d.MaxAttempts
	next := time.Now().Add(breaker.Backoff(del.Attempts, d.BackoffBase, d.BackoffMax))
	if dead {
		d.count("dead")
		d.Log.Warn("webhook delivery dead", "delivery", del.ID, "run_id", del.RunID, "attempts", del.Attempts, "err", err)
	} else {
		d.count("retry")
	}
	if ferr := d.Store.FailDelivery(bg, del.ID, next, err.Error(), dead); ferr != nil {
		d.Log.Warn("could not record webhook failure", "delivery", del.ID, "err", ferr)
	}
}

func (d *Dispatcher) count(outcome string) {
	if d.sent != nil {
		d.sent.Inc(outcome)
	}
}

func (d *Dispatcher) send(ctx context.Context, del store.Delivery) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, del.URL, bytes.NewReader(del.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Stepwise-Webhooks/1")
	req.Header.Set(HeaderEvent, del.Event)
	req.Header.Set(HeaderDelivery, strconv.FormatInt(del.ID, 10))
	if len(d.Secret) > 0 {
		req.Header.Set(HeaderSignature, Sign(d.Secret, time.Now(), del.Payload))
	}
	resp, err := d.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("receiver answered %d", resp.StatusCode)
	}
	return nil
}

// Sign returns the signature header value: "t=<unix>,v1=<hex hmac>". The
// timestamp is signed too, so a captured request cannot be replayed later.
func Sign(secret []byte, at time.Time, body []byte) string {
	ts := strconv.FormatInt(at.Unix(), 10)
	return "t=" + ts + ",v1=" + mac(secret, ts, body)
}

func mac(secret []byte, ts string, body []byte) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(ts))
	h.Write([]byte("."))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// Errors returned by Verify.
var (
	ErrBadSignature = errors.New("webhook signature does not match")
	ErrStale        = errors.New("webhook timestamp outside tolerance")
)

// Verify checks a signature header, for receivers written in Go.
func Verify(secret []byte, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var ts, sig string
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			ts = v
		case "v1":
			sig = v
		}
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || sig == "" {
		return ErrBadSignature
	}
	if d := now.Sub(time.Unix(sec, 0)); d > tolerance || d < -tolerance {
		return ErrStale
	}
	if !hmac.Equal([]byte(sig), []byte(mac(secret, ts, body))) {
		return ErrBadSignature
	}
	return nil
}
