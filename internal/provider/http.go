package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxSSELine = 1 << 20 // 1 MiB: largest single SSE line we accept

// postJSON sends body to url as JSON. Non-2xx answers become *UpstreamError;
// on success the caller owns resp.Body.
func postJSON(ctx context.Context, client *http.Client, name, url string, header http.Header, body any) (*http.Response, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("build %s request: %w", name, err)
	}
	req.Header = header
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", name, err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, &UpstreamError{
			Provider: name, Status: resp.StatusCode, Body: truncate(string(raw), 512),
			RetryAfter: resp.Header.Get("Retry-After"),
		}
	}
	return resp, nil
}

// decodeBody reads a whole JSON response into v and closes the body.
func decodeBody(resp *http.Response, name string, v any) error {
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return fmt.Errorf("read %s response: %w", name, err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decode %s response: %w", name, err)
	}
	return nil
}

// readSSE calls fn with the payload of each server-sent "data:" line until fn
// reports done, then closes the body. A body that ends before that returns
// ErrIncompleteStream.
func readSSE(ctx context.Context, resp *http.Response, name string, fn func(data string) (done bool, err error)) error {
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), maxSSELine)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue
		}
		if done, err := fn(strings.TrimSpace(data)); err != nil || done {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err // client left or deadline hit; the body read failed because of it
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read %s stream: %w", name, err)
	}
	return ErrIncompleteStream
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
