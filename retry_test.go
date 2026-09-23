package resilientclient_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	resilient "github.com/xingjianluo/resilient-api-client-go"
	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

// 正常调用：2xx 返回、请求标识保留。
func TestDoSuccess(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(2)...))
	c := newTestClient(t, srv)

	var got resilient.Item
	reqID, err := c.GetJSON(context.Background(), "/v1/items/a", &got)
	if err != nil {
		t.Fatalf("正常调用失败: %v", err)
	}
	if got.ID != "a" {
		t.Fatalf("响应解码错误: %+v", got)
	}
	if reqID == "" {
		t.Fatal("服务端请求标识 X-Request-Id 必须保留")
	}
	t.Logf("请求参数: GET /v1/items/a，判定依据: 2xx 且 request_id=%s 非空，响应=%+v", reqID, got)
}

// 幂等请求在 503 后重试，最终成功；日志打印重试次数。
func TestRetryIdempotentRecovers(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(3)...))
	c := newTestClient(t, srv, resilient.WithRetryPolicy(resilient.RetryPolicy{
		MaxAttempts: 4, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 50 * time.Millisecond, RetryAfterCap: time.Second,
	}))

	// 前 2 次 503，第 3 次成功。
	srv.Fail("/v1/items/b", 2, http.StatusServiceUnavailable, 0)

	var got resilient.Item
	_, err := c.GetJSON(context.Background(), "/v1/items/b", &got)
	if err != nil {
		t.Fatalf("期望重试后成功，实际: %v", err)
	}
	if hits := srv.Hits("/v1/items/b"); hits != 3 {
		t.Fatalf("重试次数判定：期望服务端命中 3 次（首次+2 次重试），实际 %d 次", hits)
	}
	t.Logf("请求参数: GET /v1/items/b，重试次数=2，判定依据: 幂等GET+503可恢复，服务端命中=%d", srv.Hits("/v1/items/b"))
}

// 重试次数耗尽：最终错误仍是可区分的 KindStatus，并带最后一次的请求标识。
func TestRetryExhausted(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(1)...))
	c := newTestClient(t, srv, resilient.WithRetryPolicy(resilient.RetryPolicy{
		MaxAttempts: 3, BaseDelay: time.Millisecond,
		MaxDelay: 5 * time.Millisecond, RetryAfterCap: time.Second,
	}))
	srv.Fail("/v1/items/a", 10, http.StatusBadGateway, 0)

	_, err := c.GetJSON(context.Background(), "/v1/items/a", new(resilient.Item))
	ae := assertKind(t, err, resilient.KindStatus)
	if ae.StatusCode != http.StatusBadGateway {
		t.Fatalf("判定依据：状态码应为 502，实际 %d", ae.StatusCode)
	}
	if hits := srv.Hits("/v1/items/a"); hits != 3 {
		t.Fatalf("判定依据：重试次数耗尽应恰好命中 3 次，实际 %d", hits)
	}
	t.Logf("请求参数: GET /v1/items/a，重试上限=3，判定依据: 3 次均 502，命中=%d", hits(srv, "/v1/items/a"))
}

// 服务端 Retry-After：退避时长必须尊重服务端提示（不短于提示）。
func TestRetryRespectsRetryAfter(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(1)...))
	c := newTestClient(t, srv, resilient.WithRetryPolicy(resilient.RetryPolicy{
		MaxAttempts: 3, BaseDelay: time.Millisecond,
		MaxDelay: 10 * time.Second, RetryAfterCap: 10 * time.Second,
	}))
	srv.Fail("/v1/items/a", 1, http.StatusTooManyRequests, 250*time.Millisecond)

	start := time.Now()
	var got resilient.Item
	_, err := c.GetJSON(context.Background(), "/v1/items/a", &got)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("期望第二次成功: %v", err)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("判定依据：应尊重 Retry-After 等待约 250ms，实际仅 %v", elapsed)
	}
	t.Logf("请求参数: GET /v1/items/a，服务端提示 Retry-After=250ms，实际退避=%v，重试次数=1", elapsed)
}

// 非幂等请求（POST）即使遇到可恢复的 503 也绝不重试。
func TestNoRetryForNonIdempotent(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(1)...))
	c := newTestClient(t, srv, resilient.WithRetryPolicy(resilient.RetryPolicy{
		MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond,
	}))
	srv.Fail("/v1/echo", 10, http.StatusServiceUnavailable, 0)

	status, _, reqID, err := c.Do(context.Background(), http.MethodPost, "/v1/echo", strings.NewReader(`{"k":1}`))
	if err == nil {
		t.Fatal("POST 503 应返回错误")
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("判定依据：应透出 503 状态，实际 %d", status)
	}
	if srv.Hits("/v1/echo") != 1 {
		t.Fatalf("判定依据：非幂等 POST 不得重试，期望命中 1 次，实际 %d", srv.Hits("/v1/echo"))
	}
	t.Logf("请求参数: POST /v1/echo body={\"k\":1}，重试次数=0，判定依据: POST 非幂等，request_id=%s", reqID)
}

// 非可恢复状态码（404/400）不重试。
func TestNonRetryableStatus(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(1)...))
	c := newTestClient(t, srv)
	status, _, _, err := c.Do(context.Background(), http.MethodGet, "/v1/items/missing", nil)
	ae := assertKind(t, err, resilient.KindStatus)
	if status != http.StatusNotFound || ae.StatusCode != 404 {
		t.Fatalf("判定依据：404 不应重试，status=%d", status)
	}
	if srv.Hits("/v1/items/missing") != 1 {
		t.Fatalf("判定依据：404 命中应仅 1 次")
	}
}

// errors.Is 必须能区分底层原因。
func TestErrorsIsSentinel(t *testing.T) {
	if !errors.Is(&resilient.APIError{Kind: resilient.KindTimeout}, resilient.ErrTimeout) {
		t.Fatal("KindTimeout 应 errors.Is ErrTimeout")
	}
	if errors.Is(&resilient.APIError{Kind: resilient.KindTimeout}, resilient.ErrCanceled) {
		t.Fatal("超时不得被误判为取消")
	}
}

func hits(s *mockapi.Server, p string) int { return s.Hits(p) }
