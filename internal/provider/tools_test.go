package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var refundSchema = json.RawMessage(`{"type":"object","properties":{
	"order_id":{"type":"string"},"amount":{"type":"number"},"notify":{"type":"boolean"}},
	"required":["order_id"]}`)

// agentConversation is a full tool-use turn: the model called two tools and
// both results came back.
func agentConversation() []Message {
	return []Message{
		{Role: "system", Content: "You are a refund agent."},
		{Role: "user", Content: "Refund order_id: ORD-1"},
		{Role: "assistant", Content: "Looking it up.", ToolCalls: []ToolCall{
			{ID: "t1", Name: "get_order", Input: json.RawMessage(`{"order_id":"ORD-1"}`)},
			{ID: "t2", Name: "check_policy", Input: nil},
		}},
		{Role: "tool", ToolCallID: "t1", Content: `{"status":"delivered"}`},
		{Role: "tool", ToolCallID: "t2", Content: "policy unavailable", IsError: true},
	}
}

func TestToAnthropicMessagesMergesToolResults(t *testing.T) {
	system, msgs := toAnthropicMessages(agentConversation())
	if system != "You are a refund agent." {
		t.Errorf("system = %q", system)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want user, assistant, user(tool results): %+v", len(msgs), msgs)
	}

	asst, ok := msgs[1].Content.([]anthropicBlock)
	if !ok || len(asst) != 3 || asst[0].Type != "text" || asst[1].Type != "tool_use" || asst[2].Type != "tool_use" {
		t.Fatalf("assistant blocks = %+v", msgs[1].Content)
	}
	if string(asst[2].Input) != "{}" {
		t.Errorf("nil tool input should become {}, got %s", asst[2].Input)
	}

	results, ok := msgs[2].Content.([]anthropicBlock)
	if !ok || msgs[2].Role != "user" || len(results) != 2 {
		t.Fatalf("both tool results must be in one user message: %+v", msgs[2])
	}
	if results[0].ToolUseID != "t1" || results[1].ToolUseID != "t2" || !results[1].IsError {
		t.Errorf("tool results = %+v", results)
	}
}

func TestAnthropicParsesToolUse(t *testing.T) {
	var got anthropicRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"id":"msg_2","stop_reason":"tool_use","content":[
			{"type":"text","text":"Let me check."},
			{"type":"tool_use","id":"toolu_1","name":"get_order","input":{"order_id":"ORD-1"}}],
			"usage":{"input_tokens":50,"output_tokens":20}}`))
	}))
	defer srv.Close()

	res, err := NewAnthropic("k", WithAnthropicBaseURL(srv.URL)).Complete(context.Background(), Request{
		Model:    "claude-x",
		Messages: []Message{{Role: "user", Content: "hi"}},
		Tools:    []ToolDef{{Name: "get_order", Description: "Look up an order", InputSchema: refundSchema}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tools) != 1 || got.Tools[0].Name != "get_order" {
		t.Errorf("tools not sent: %+v", got.Tools)
	}
	if res.FinishReason != "tool_calls" || len(res.ToolCalls) != 1 {
		t.Fatalf("res = %+v", res)
	}
	tc := res.ToolCalls[0]
	if tc.ID != "toolu_1" || tc.Name != "get_order" || string(tc.Input) != `{"order_id":"ORD-1"}` {
		t.Errorf("tool call = %+v", tc)
	}
	if res.Content != "Let me check." {
		t.Errorf("content = %q", res.Content)
	}
}

func TestOpenAIToolRoundTrip(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sk-test" {
			t.Error("missing bearer token")
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"id":"chatcmpl-9","choices":[{"finish_reason":"tool_calls","message":{
			"role":"assistant","content":null,"tool_calls":[
			{"id":"call_a","type":"function","function":{"name":"get_order","arguments":"{\"order_id\":\"ORD-1\"}"}},
			{"id":"call_b","type":"function","function":{"name":"check_policy","arguments":"{broken"}}]}}],
			"usage":{"prompt_tokens":30,"completion_tokens":12}}`))
	}))
	defer srv.Close()

	res, err := NewOpenAI("sk-test", WithOpenAIBaseURL(srv.URL)).Complete(context.Background(), Request{
		Model:    "gpt-x",
		Messages: agentConversation(),
		Tools:    []ToolDef{{Name: "get_order", InputSchema: refundSchema}},
		// Structured output rides alongside tools.
		ResponseSchema: json.RawMessage(`{"type":"object"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if rf, _ := got["response_format"].(map[string]any); rf["type"] != "json_schema" || rf["json_schema"].(map[string]any)["strict"] != true {
		t.Errorf("response_format = %v", got["response_format"])
	}

	// Request shape: tool calls carry arguments as a JSON string; tool results use role "tool".
	msgs := got["messages"].([]any)
	asst := msgs[2].(map[string]any)
	call := asst["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)
	if call["arguments"] != `{"order_id":"ORD-1"}` {
		t.Errorf("arguments not string-encoded: %v", call["arguments"])
	}
	toolMsg := msgs[4].(map[string]any)
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "t2" || !strings.HasPrefix(toolMsg["content"].(string), "ERROR:") {
		t.Errorf("error tool result = %v", toolMsg)
	}
	if got["tools"].([]any)[0].(map[string]any)["type"] != "function" {
		t.Error("tools not wrapped as functions")
	}

	// Response: arguments decoded back into objects; broken JSON becomes {}.
	if res.FinishReason != "tool_calls" || len(res.ToolCalls) != 2 {
		t.Fatalf("res = %+v", res)
	}
	if string(res.ToolCalls[0].Input) != `{"order_id":"ORD-1"}` || string(res.ToolCalls[1].Input) != `{}` {
		t.Errorf("inputs = %s, %s", res.ToolCalls[0].Input, res.ToolCalls[1].Input)
	}
	if res.InputTokens != 30 || res.OutputTokens != 12 {
		t.Errorf("usage = %d/%d", res.InputTokens, res.OutputTokens)
	}
}

func TestOpenAIStreamAndErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "fail") {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n" +
			"data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\" there\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n" +
			"data: [DONE]\n\n"))
	}))
	defer srv.Close()

	var text strings.Builder
	res, err := NewOpenAI("k", WithOpenAIBaseURL(srv.URL)).Stream(context.Background(),
		Request{Model: "gpt-x", Messages: []Message{{Role: "user", Content: "hi"}}},
		func(s string) error { text.WriteString(s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if text.String() != "Hi there" || res.FinishReason != "stop" || res.InputTokens != 4 || res.OutputTokens != 2 {
		t.Errorf("text=%q res=%+v", text.String(), res)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer bad.Close()
	_, err = NewOpenAI("k", WithOpenAIBaseURL(bad.URL)).Complete(context.Background(),
		Request{Model: "gpt-x", Messages: []Message{{Role: "user", Content: "x"}}})
	var ue *UpstreamError
	if !errors.As(err, &ue) || ue.Status != 429 || ue.RetryAfter != "7" {
		t.Errorf("err = %v", err)
	}
}

func TestMockAgentCallsEachToolThenAnswers(t *testing.T) {
	m := NewMock()
	tools := []ToolDef{
		{Name: "get_order", InputSchema: refundSchema},
		{Name: "issue_refund", InputSchema: refundSchema},
	}
	msgs := []Message{{Role: "user", Content: "Please refund order_id: ORD-1001, amount: 49.99, notify: yes"}}

	for _, want := range []string{"get_order", "issue_refund"} {
		res, err := m.Complete(context.Background(), Request{Messages: msgs, Tools: tools})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.ToolCalls) != 1 || res.ToolCalls[0].Name != want || res.FinishReason != "tool_calls" {
			t.Fatalf("want a call to %s, got %+v", want, res)
		}
		var args map[string]any
		_ = json.Unmarshal(res.ToolCalls[0].Input, &args)
		if args["order_id"] != "ORD-1001" || args["amount"] != 49.99 || args["notify"] != true {
			t.Errorf("args = %v", args)
		}
		msgs = append(msgs,
			Message{Role: "assistant", Content: res.Content, ToolCalls: res.ToolCalls},
			Message{Role: "tool", ToolCallID: res.ToolCalls[0].ID, Content: want + " ok"})
	}

	res, _ := m.Complete(context.Background(), Request{Messages: msgs, Tools: tools})
	if len(res.ToolCalls) != 0 || res.FinishReason != "stop" || !strings.Contains(res.Content, "issue_refund ok") {
		t.Errorf("final answer = %+v", res)
	}
}

func TestMockFailRate(t *testing.T) {
	m := &Mock{FailRate: 1}
	_, err := m.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}})
	if !Retryable(err) {
		t.Errorf("simulated outage should be retryable: %v", err)
	}
}

func TestRetryable(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&UpstreamError{Status: 429}, true},
		{&UpstreamError{Status: 503}, true},
		{&UpstreamError{Status: 400}, false},
		{&UpstreamError{Status: 401}, false},
		{&StreamError{Type: "overloaded_error"}, true},
		{&StreamError{Type: "invalid_request_error"}, false},
		{context.Canceled, false},
		{context.DeadlineExceeded, true},
		{ErrIncompleteStream, true},
		{nil, false},
	}
	for _, c := range cases {
		if got := Retryable(c.err); got != c.want {
			t.Errorf("Retryable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
