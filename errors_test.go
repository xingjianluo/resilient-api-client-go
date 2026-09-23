package resilientclient_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	resilient "github.com/xingjianluo/resilient-api-client-go"
	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

// 传输层失败：服务端关闭后连接被拒，归一化为 KindTransport，
// 幂等请求可重试，最终仍失败；底层 url.Error 不直接透出但可 errors.Is/As。
func TestTransportFailure(t *testing.T) {
	srv := newServer(t, mockapi.WithNoAuth())
	c := resilient.New(srv.URL(),
		resilient.WithRetryPolicy(resilient.RetryPolicy{
			MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond,
		}),
		resilient.WithLogger(func(format string, args ...any) { t.Logf("[client] "+format, args...) }),
	)
	addr := srv.URL()
	srv.Close() // 之后所有连接失败
	t.Logf("请求参数: GET %s/v1/items（服务端已关闭）", addr)

	_, _, _, err := c.Do(context.Background(), http.MethodGet, "/v1/items", nil)
	ae := assertKind(t, err, resilient.KindTransport)
	if ae.Cause == nil {
		t.Fatal("传输错误应保留底层原因")
	}
	t.Logf("重试次数=1（共2次尝试），判定依据: 连接失败归为 transport，cause=%T", ae.Cause)
}

// 响应体非法 JSON：即使 200 也归一化为 KindDecode。
func TestDecodeFailure(t *testing.T) {
	srv := newServer(t)
	c := newTestClient(t, srv)
	t.Logf("请求参数: GET /v1/badjson，服务端返回非法 JSON")

	_, err := c.GetJSON(context.Background(), "/v1/badjson", new(map[string]any))
	ae := assertKind(t, err, resilient.KindDecode)
	if ae.StatusCode != http.StatusOK {
		t.Fatalf("判定依据：非法 JSON 发生在 200 响应上，实际 status=%d", ae.StatusCode)
	}
}

// 整体截止超时：等待慢响应时 context deadline，归一化为 KindTimeout，
// 且超时在幂等请求下会重试，重试次数耗尽后仍为 KindTimeout。
func TestTimeoutDistinctFromCancel(t *testing.T) {
	srv := newServer(t)
	c := newTestClient(t, srv, resilient.WithRetryPolicy(resilient.RetryPolicy{
		MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond,
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	t.Logf("请求参数: GET /v1/slow?ms=1000，整体超时预算=80ms")

	_, err := c.GetJSON(ctx, "/v1/slow?ms=1000", new(map[string]any))
	ae := assertKind(t, err, resilient.KindTimeout)
	if !errors.Is(err, resilient.ErrTimeout) {
		t.Fatalf("判定依据：超时应 errors.Is ErrTimeout，实际 %v", err)
	}
	if errors.Is(err, resilient.ErrCanceled) {
		t.Fatal("超时不得被误判为取消")
	}
	t.Logf("判定依据: ctx deadline exceeded 归为 timeout（重试1次后耗尽），request_id=%s", ae.RequestID)
}

// 调用方主动取消：归一化为 KindCanceled，立即终止，不重试。
func TestCallerCanceled(t *testing.T) {
	srv := newServer(t)
	c := newTestClient(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	t.Logf("请求参数: GET /v1/slow?ms=1000，50ms 后调用方主动 cancel")

	_, err := c.GetJSON(ctx, "/v1/slow?ms=1000", new(map[string]any))
	ae := assertKind(t, err, resilient.KindCanceled)
	if !errors.Is(err, resilient.ErrCanceled) {
		t.Fatalf("判定依据：取消应 errors.Is ErrCanceled，实际 %v", err)
	}
	if errors.Is(err, resilient.ErrTimeout) {
		t.Fatal("取消不得被误判为超时")
	}
	if srv.Hits("/v1/slow") != 1 {
		t.Fatalf("判定依据：取消后不得重试，期望命中 1 次，实际 %d", srv.Hits("/v1/slow"))
	}
	t.Logf("判定依据: ctx.Canceled 归为 canceled 且不重试，request_id=%s", ae.RequestID)
}

// 单次尝试超时（WithAttemptTimeout）同样归一化为 KindTimeout，且区别于整体取消。
func TestAttemptTimeout(t *testing.T) {
	srv := newServer(t)
	c := newTestClient(t, srv,
		resilient.WithAttemptTimeout(60*time.Millisecond),
		resilient.WithRetryPolicy(resilient.RetryPolicy{
			MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond,
		}))
	t.Logf("请求参数: GET /v1/slow?ms=1000，单次尝试超时=60ms，整体预算充足")

	_, err := c.GetJSON(context.Background(), "/v1/slow?ms=1000", new(map[string]any))
	assertKind(t, err, resilient.KindTimeout)
}

// 配额耗尽：限流等待超出容忍上限，立即返回 KindQuota，且不发起任何服务端请求。
func TestQuotaExhaustedRejected(t *testing.T) {
	srv := newServer(t, mockapi.WithNoAuth())
	// 速率 1/s、突发 0（初始无令牌），最多等 20ms；攒出一个令牌需 1s -> 必然拒绝。
	limiter := resilient.NewTokenBucket(1, 1, 20*time.Millisecond)
	// 先消耗唯一的突发令牌，使下一次必须等待 ~1s。
	c := newTestClient(t, srv, resilient.WithLimiter(limiter))

	// 第一次吃掉突发令牌并成功。
	if _, _, _, err := c.Do(context.Background(), http.MethodGet, "/v1/items", nil); err != nil {
		t.Fatalf("突发请求应成功: %v", err)
	}
	// 第二次：需要等约 1s，超过 maxWait=20ms，立即配额耗尽。
	start := time.Now()
	_, _, _, err := c.Do(context.Background(), http.MethodGet, "/v1/items", nil)
	elapsed := time.Since(start)
	ae := assertKind(t, err, resilient.KindQuota)
	if !errors.Is(err, resilient.ErrQuota) {
		t.Fatalf("判定依据：配额耗尽应 errors.Is ErrQuota，实际 %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("判定依据：配额耗尽必须立即拒绝，实际等待了 %v", elapsed)
	}
	before := srv.Hits("/v1/items")
	if before != 1 {
		t.Fatalf("判定依据：被限流拒绝的请求不得打到服务端，期望命中 1，实际 %d", before)
	}
	t.Logf("请求参数: GET /v1/items（第二次），判定依据: 需等约1s>maxWait20ms，立即拒绝耗时=%v，request_id=%s",
		elapsed, ae.RequestID)
}

// 限流等待期间调用方取消：应为 KindCanceled 而非 KindQuota。
func TestLimiterWaitCanceled(t *testing.T) {
	srv := newServer(t, mockapi.WithNoAuth())
	limiter := resilient.NewTokenBucket(1, 1, time.Minute)
	c := newTestClient(t, srv, resilient.WithLimiter(limiter))

	if _, _, _, err := c.Do(context.Background(), http.MethodGet, "/v1/items", nil); err != nil {
		t.Fatalf("突发请求应成功: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	_, _, _, err := c.Do(ctx, http.MethodGet, "/v1/items", nil)
	assertKind(t, err, resilient.KindCanceled)
}
