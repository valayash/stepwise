// Package demo is a fake e-commerce backend exposed as an MCP server, used by
// the refund-agent example, the integration tests and the chaos test.
package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/valayash/stepwise/internal/mcp"
)

// Order is a shop order.
type Order struct {
	ID          string   `json:"order_id"`
	Customer    string   `json:"customer"`
	Status      string   `json:"status"`
	Amount      float64  `json:"amount"`
	Currency    string   `json:"currency"`
	Items       []string `json:"items"`
	DeliveredAt string   `json:"delivered_at,omitempty"`
}

// Refund is an issued refund.
type Refund struct {
	ID             string  `json:"refund_id"`
	OrderID        string  `json:"order_id"`
	Amount         float64 `json:"amount"`
	IdempotencyKey string  `json:"idempotency_key"`
}

// Stats prove exactly-once side effects under retries and crashes.
type Stats struct {
	Calls          map[string]int `json:"calls"`           // tool executions, including repeats
	RefundsIssued  int            `json:"refunds_issued"`  // distinct refunds created
	DuplicatesSeen int            `json:"duplicates_seen"` // repeated refund calls answered from the idempotency store
	InjectedFaults int            `json:"injected_faults"`
}

// Shop is the fake backend. Safe for concurrent use.
type Shop struct {
	// Latency is added to every tool call.
	Latency time.Duration

	mu       sync.Mutex
	failRate float64
	orders   map[string]Order
	refunds  map[string]Refund // idempotency key → refund
	stats    Stats
	mcp      *mcp.Server
}

// NewShop returns a shop with three sample orders. now fixes delivery dates.
func NewShop(now time.Time) *Shop {
	day := 24 * time.Hour
	s := &Shop{
		orders: map[string]Order{
			"ORD-1001": {ID: "ORD-1001", Customer: "Asha", Status: "delivered", Amount: 49.99, Currency: "USD", Items: []string{"Wireless earbuds"}, DeliveredAt: now.Add(-3 * day).Format(time.DateOnly)},
			"ORD-1002": {ID: "ORD-1002", Customer: "Ravi", Status: "in_transit", Amount: 120.00, Currency: "USD", Items: []string{"Running shoes"}},
			"ORD-1003": {ID: "ORD-1003", Customer: "Meera", Status: "delivered", Amount: 15.50, Currency: "USD", Items: []string{"Phone case"}, DeliveredAt: now.Add(-45 * day).Format(time.DateOnly)},
		},
		refunds: map[string]Refund{},
		stats:   Stats{Calls: map[string]int{}},
	}
	str := func(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
	schema := func(props map[string]any, required ...string) json.RawMessage {
		b, _ := json.Marshal(map[string]any{"type": "object", "properties": props, "required": required})
		return b
	}
	s.mcp = mcp.NewServer("demo-shop", "1.0.0",
		mcp.ServerTool{
			Tool: mcp.Tool{Name: "get_order", Description: "Look up an order by ID: status, amount, items and delivery date.",
				InputSchema: schema(map[string]any{"order_id": str("Order ID, e.g. ORD-1001")}, "order_id")},
			Handler: s.getOrder,
		},
		mcp.ServerTool{
			Tool: mcp.Tool{Name: "check_refund_policy", Description: "Check whether an order can be refunded. Delivered orders are refundable within 30 days.",
				InputSchema: schema(map[string]any{"order_id": str("Order ID"), "reason": str("Why the customer wants a refund")}, "order_id")},
			Handler: s.checkPolicy,
		},
		mcp.ServerTool{
			Tool: mcp.Tool{Name: "issue_refund", Description: "Refund an order. Moves money: requires approval in most setups.",
				InputSchema: schema(map[string]any{"order_id": str("Order ID"), "amount": map[string]any{"type": "number", "description": "Amount to refund"}}, "order_id", "amount")},
			Handler: s.issueRefund,
		},
	)
	return s
}

// SetFailRate makes a fraction (0..1) of MCP requests fail with HTTP 503.
// Safe to change while the shop is serving.
func (s *Shop) SetFailRate(r float64) {
	s.mu.Lock()
	s.failRate = r
	s.mu.Unlock()
}

// AddOrder adds or replaces an order (tests and chaos runs create many).
func (s *Shop) AddOrder(o Order) {
	s.mu.Lock()
	s.orders[o.ID] = o
	s.mu.Unlock()
}

func (s *Shop) begin(ctx context.Context, tool string) error {
	s.mu.Lock()
	s.stats.Calls[tool]++
	s.mu.Unlock()
	if s.Latency > 0 {
		select {
		case <-time.After(s.Latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *Shop) order(args json.RawMessage) (Order, error) {
	var in struct {
		OrderID string `json:"order_id"`
	}
	_ = json.Unmarshal(args, &in)
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[in.OrderID]
	if !ok {
		return Order{}, fmt.Errorf("order %q not found", in.OrderID)
	}
	return o, nil
}

func (s *Shop) getOrder(ctx context.Context, args json.RawMessage, _ mcp.Meta) (string, error) {
	if err := s.begin(ctx, "get_order"); err != nil {
		return "", err
	}
	o, err := s.order(args)
	if err != nil {
		return "", err
	}
	b, _ := json.Marshal(o)
	return string(b), nil
}

func (s *Shop) checkPolicy(ctx context.Context, args json.RawMessage, _ mcp.Meta) (string, error) {
	if err := s.begin(ctx, "check_refund_policy"); err != nil {
		return "", err
	}
	o, err := s.order(args)
	if err != nil {
		return "", err
	}
	eligible, why := false, ""
	switch {
	case o.Status != "delivered":
		why = "Order is not delivered yet; offer to cancel instead."
	default:
		d, _ := time.Parse(time.DateOnly, o.DeliveredAt)
		if days := int(time.Since(d).Hours() / 24); days <= 30 {
			eligible, why = true, fmt.Sprintf("Delivered %d days ago, within the 30-day window.", days)
		} else {
			why = fmt.Sprintf("Delivered %d days ago, outside the 30-day window.", days)
		}
	}
	b, _ := json.Marshal(map[string]any{"order_id": o.ID, "eligible": eligible, "max_refund": o.Amount, "explanation": why})
	return string(b), nil
}

func (s *Shop) issueRefund(ctx context.Context, args json.RawMessage, meta mcp.Meta) (string, error) {
	if err := s.begin(ctx, "issue_refund"); err != nil {
		return "", err
	}
	if meta.IdempotencyKey == "" {
		return "", errors.New("idempotency key required for refunds")
	}
	var in struct {
		OrderID string  `json:"order_id"`
		Amount  float64 `json:"amount"`
	}
	_ = json.Unmarshal(args, &in)

	r, err := s.refundOnce(in.OrderID, in.Amount, meta.IdempotencyKey)
	if err != nil {
		return "", err
	}
	// The refund is committed. The reply now takes time to travel back; a
	// caller that dies in this window never learns the outcome and will call
	// again — exactly the case the idempotency key exists for.
	time.Sleep(s.Latency)
	b, _ := json.Marshal(r)
	return string(b), nil
}

func (s *Shop) refundOnce(orderID string, amount float64, key string) (Refund, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.refunds[key]; ok {
		// Same call again (retry or crash recovery): return the original result.
		s.stats.DuplicatesSeen++
		return r, nil
	}
	o, ok := s.orders[orderID]
	if !ok {
		return Refund{}, fmt.Errorf("order %q not found", orderID)
	}
	if amount <= 0 || amount > o.Amount {
		return Refund{}, fmt.Errorf("amount must be between 0 and %.2f", o.Amount)
	}
	s.stats.RefundsIssued++
	r := Refund{ID: fmt.Sprintf("RF-%05d", s.stats.RefundsIssued), OrderID: o.ID, Amount: amount, IdempotencyKey: key}
	s.refunds[key] = r
	return r, nil
}

// Stats returns a snapshot of the counters.
func (s *Shop) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.stats
	out.Calls = map[string]int{}
	for k, v := range s.stats.Calls {
		out.Calls[k] = v
	}
	return out
}

// Refunds lists issued refunds.
func (s *Shop) Refunds() []Refund {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Refund, 0, len(s.refunds))
	for _, r := range s.refunds {
		out = append(out, r)
	}
	return out
}

// Handler serves the MCP endpoint at /mcp plus /stats and /refunds for humans and the chaos harness.
func (s *Shop) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		fail := s.failRate > 0 && rand.Float64() < s.failRate
		if fail {
			s.stats.InjectedFaults++
		}
		s.mu.Unlock()
		if fail {
			http.Error(w, "injected fault", http.StatusServiceUnavailable)
			return
		}
		s.mcp.ServeHTTP(w, r)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Stats())
	})
	mux.HandleFunc("GET /refunds", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Refunds())
	})
	return mux
}
