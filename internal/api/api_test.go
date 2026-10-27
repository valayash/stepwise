package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/demo"
	"github.com/valayash/stepwise/internal/engine"
	"github.com/valayash/stepwise/internal/provider"
	"github.com/valayash/stepwise/internal/store/memory"
	"github.com/valayash/stepwise/internal/tools"
)

type env struct {
	t     *testing.T
	url   string
	shop  string
	token string
	store *memory.Store
}

func setup(t *testing.T, token string, opts ...func(*Server)) *env {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	shop := httptest.NewServer(demo.NewShop(time.Now()).Handler())
	t.Cleanup(shop.Close)

	st := memory.New(nil)
	runner := tools.NewRunner()
	router := provider.NewRouter(provider.Route{Prefix: "mock", Provider: provider.NewMock()})
	eng := engine.New(engine.Config{WorkerID: "t", Workers: 2, PollInterval: 20 * time.Millisecond}, engine.Deps{
		Store: st, Router: router, Tools: runner, Logger: quiet,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { eng.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	s := &Server{Store: st, Tools: runner, Router: router, Token: token, Log: quiet}
	for _, o := range opts {
		o(s)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &env{t: t, url: srv.URL, shop: shop.URL + "/mcp", token: token, store: st}
}

func (e *env) do(method, path, body string, hdr ...string) (int, map[string]any) {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.url+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (e *env) waitStatus(id, want string) map[string]any {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, r := e.do("GET", "/v1/runs/"+id, "")
		if r["status"] == want {
			return r
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, r := e.do("GET", "/v1/runs/"+id, "")
	e.t.Fatalf("status = %v, want %s", r["status"], want)
	return nil
}

func refundBody(shop string) string {
	return fmt.Sprintf(`{"agent":{"model":"mock","tools":[{"type":"mcp","url":%q}],"require_approval":["issue_refund"]},
		"input":"order_id: ORD-1001, amount: 49.99","metadata":{"ticket":"SUP-1"}}`, shop)
}

func TestRefundFlowOverHTTP(t *testing.T) {
	e := setup(t, "")
	code, run := e.do("POST", "/v1/runs", refundBody(e.shop))
	if code != 201 {
		t.Fatalf("create = %d %v", code, run)
	}
	id := run["id"].(string)
	agent := run["agent"].(map[string]any)
	if len(agent["tools"].([]any)) != 3 {
		t.Errorf("MCP server should expand into 3 tools: %v", agent["tools"])
	}

	e.waitStatus(id, "waiting")
	_, inbox := e.do("GET", "/v1/approvals", "")
	items := inbox["approvals"].([]any)
	if len(items) != 1 {
		t.Fatalf("inbox = %v", inbox)
	}
	item := items[0].(map[string]any)
	if item["tool_name"] != "issue_refund" || item["run_id"] != id {
		t.Errorf("inbox item = %v", item)
	}

	stepID := int(item["step_id"].(float64))
	code, _ = e.do("POST", fmt.Sprintf("/v1/runs/%s/steps/%d/approve", id, stepID), `{"decided_by":"lead"}`)
	if code != 200 {
		t.Fatalf("approve = %d", code)
	}
	code, _ = e.do("POST", fmt.Sprintf("/v1/runs/%s/steps/%d/approve", id, stepID), "")
	if code != 409 {
		t.Errorf("second decision = %d, want 409", code)
	}

	done := e.waitStatus(id, "completed")
	if !strings.Contains(done["output"].(string), "RF-") || len(done["steps"].([]any)) != 7 {
		t.Errorf("run = %v", done)
	}
	if code, _ := e.do("POST", "/v1/runs/"+id+"/cancel", ""); code != 409 {
		t.Errorf("cancelling a completed run = %d", code)
	}
}

func TestCreateValidation(t *testing.T) {
	e := setup(t, "")
	cases := map[string]string{
		"empty body":    ``,
		"bad json":      `{`,
		"no agent":      `{"input":"x"}`,
		"no input":      `{"agent":{"model":"mock"}}`,
		"unknown model": `{"agent":{"model":"gpt-99"},"input":"x"}`,
		"bad tool":      `{"agent":{"model":"mock","tools":[{"type":"shell","name":"rm"}]},"input":"x"}`,
		"bad sub-agent": `{"agent":{"model":"mock","tools":[{"type":"agent","name":"sub","agent":{"model":"nope"}}]},"input":"x"}`,
	}
	for name, body := range cases {
		if code, out := e.do("POST", "/v1/runs", body); code != 400 {
			t.Errorf("%s: code %d %v", name, code, out)
		}
	}
	if code, _ := e.do("GET", "/v1/runs/nope", ""); code != 404 {
		t.Errorf("missing run = %d", code)
	}
}

func TestMonthlyTokenLimit(t *testing.T) {
	e := setup(t, "", func(s *Server) { s.MonthlyTokenLimit = 1 })
	body := `{"agent":{"model":"mock"},"input":"hi"}`
	_, run := e.do("POST", "/v1/runs", body)
	e.waitStatus(run["id"].(string), "completed")
	if code, out := e.do("POST", "/v1/runs", body); code != 429 {
		t.Errorf("over the limit: %d %v", code, out)
	}
	_, u := e.do("GET", "/v1/usage", "")
	if u["runs"] != 1.0 || u["tokens"].(float64) < 1 || u["monthly_token_limit"] != 1.0 {
		t.Errorf("usage = %v", u)
	}
}

func TestConversationOverHTTP(t *testing.T) {
	e := setup(t, "")
	_, run := e.do("POST", "/v1/runs", `{"agent":{"model":"mock"},"input":"hi"}`)
	id := run["id"].(string)
	e.waitStatus(id, "completed")
	if code, _ := e.do("POST", "/v1/runs/"+id+"/messages", `{"content":"  "}`); code != 400 {
		t.Errorf("empty message = %d", code)
	}
	if code, out := e.do("POST", "/v1/runs/"+id+"/messages", `{"content":"more please"}`); code != 200 || out["status"] != "queued" {
		t.Fatalf("message = %d %v", code, out)
	}
	if done := e.waitStatus(id, "completed"); done["output"] != "mock: more please" {
		t.Errorf("reply = %v", done["output"])
	}
	_, waiting := e.do("POST", "/v1/runs", refundBody(e.shop))
	e.waitStatus(waiting["id"].(string), "waiting")
	if code, _ := e.do("POST", "/v1/runs/"+waiting["id"].(string)+"/messages", `{"content":"hello?"}`); code != 409 {
		t.Errorf("message to a waiting run = %d, want 409", code)
	}
}

func TestKillNeedsWorkersInProcess(t *testing.T) {
	e := setup(t, "") // like `stepwise api`: no workers in this process
	_, run := e.do("POST", "/v1/runs", `{"agent":{"model":"mock"},"input":"hi"}`)
	if code, out := e.do("POST", "/v1/runs/"+run["id"].(string)+"/kill", ""); code != 409 || !strings.Contains(fmt.Sprint(out), "kill -9") {
		t.Errorf("kill without in-process workers = %d %v", code, out)
	}
}

func TestIdempotencyKeyHeader(t *testing.T) {
	e := setup(t, "")
	body := `{"agent":{"model":"mock"},"input":"hi"}`
	c1, r1 := e.do("POST", "/v1/runs", body, "Idempotency-Key", "order-77")
	c2, r2 := e.do("POST", "/v1/runs", body, "Idempotency-Key", "order-77")
	if c1 != 201 || c2 != 200 || r1["id"] != r2["id"] {
		t.Errorf("codes %d/%d ids %v/%v", c1, c2, r1["id"], r2["id"])
	}
}

func TestAuth(t *testing.T) {
	e := setup(t, "secret")
	e.token = ""
	if code, _ := e.do("GET", "/v1/runs", ""); code != 401 {
		t.Errorf("no token = %d", code)
	}
	e.token = "wrong"
	if code, _ := e.do("GET", "/v1/runs", ""); code != 401 {
		t.Errorf("wrong token = %d", code)
	}
	e.token = "secret"
	if code, _ := e.do("GET", "/v1/runs", ""); code != 200 {
		t.Errorf("right token = %d", code)
	}
	// Health and the UI stay public.
	for _, p := range []string{"/healthz", "/readyz", "/"} {
		resp, _ := http.Get(e.url + p)
		if resp.StatusCode != 200 {
			t.Errorf("%s = %d", p, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

func TestCancelAndRetry(t *testing.T) {
	e := setup(t, "")
	_, run := e.do("POST", "/v1/runs", refundBody(e.shop))
	id := run["id"].(string)
	e.waitStatus(id, "waiting")
	if code, r := e.do("POST", "/v1/runs/"+id+"/cancel", ""); code != 200 || r["status"] != "cancelled" {
		t.Fatalf("cancel = %d %v", code, r)
	}
	if code, _ := e.do("POST", "/v1/runs/"+id+"/retry", ""); code != 409 {
		t.Errorf("retrying a cancelled run = %d, want 409", code)
	}
	_, list := e.do("GET", "/v1/runs?status=cancelled", "")
	if len(list["runs"].([]any)) != 1 {
		t.Errorf("filtered list = %v", list)
	}
}

func TestEventsStream(t *testing.T) {
	e := setup(t, "")
	_, run := e.do("POST", "/v1/runs", `{"agent":{"model":"mock"},"input":"stream me"}`)
	resp, err := http.Get(e.url + "/v1/runs/" + run["id"].(string) + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var last map[string]any
	for sc.Scan() {
		if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			_ = json.Unmarshal([]byte(data), &last)
		}
	}
	// The stream ends by itself once the run is terminal.
	if last["status"] != string(core.RunCompleted) || last["output"] != "mock: stream me" {
		t.Errorf("last event = %v", last)
	}
}
