// Package integration 使用进程内 mockserver 复现真实调用，
// 验证限流、重试、分页与凭证刷新在各入口下行为一致。
package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/resilient-api-client-go/mockserver"
	"github.com/xingjianluo/resilient-api-client-go/sdk"
)

// newClient 创建带日志的客户端，日志打印请求参数、重试次数与判定依据。
func newClient(t *testing.T, baseURL string, tokens *sdk.TokenProvider) *sdk.Client {
	t.Helper()
	c := sdk.NewClient(baseURL)
	c.Tokens = tokens
	c.Retry.Logger = func(format string, args ...any) {
		t.Logf("[client] "+format, args...)
	}
	return c
}

// tokenProvider 返回基于 mockserver /token 端点的凭证提供者。
func tokenProvider(serverURL string) *sdk.TokenProvider {
	return sdk.NewTokenProvider(func(ctx context.Context) (sdk.Token, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/token", nil)
		if err != nil {
			return sdk.Token{}, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return sdk.Token{}, err
		}
		defer resp.Body.Close()
		var body struct {
			Token     string `json:"token"`
			ExpiresAt int64  `json:"expires_at"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return sdk.Token{}, err
		}
		return sdk.Token{Value: body.Token, ExpiresAt: time.Unix(body.ExpiresAt, 0)}, nil
	}, 200*time.Millisecond)
}

// itemsPage 是 /items 的响应结构。
type itemsPage struct {
	Items      []mockserver.Item `json:"items"`
	NextCursor string            `json:"next_cursor"`
}

func TestNormalCallAndPagination(t *testing.T) {
	var items []mockserver.Item
	for i := 0; i < 7; i++ {
		items = append(items, mockserver.Item{ID: "id-" + strconv.Itoa(i), Name: "n"})
	}
	srv := mockserver.New(items, 60)
	defer srv.Close()

	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	fetch := func(ctx context.Context, cursor string) (sdk.Page[mockserver.Item], error) {
		var p itemsPage
		err := c.Do(ctx, sdk.Request{Method: "GET", Path: "/items?cursor=" + cursor + "&limit=3"}, &p)
		return sdk.Page[mockserver.Item]{Items: p.Items, NextCursor: p.NextCursor}, err
	}
	it := sdk.NewIterator(fetch, func(i mockserver.Item) string { return i.ID })
	got, err := it.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 7 {
		t.Fatalf("got %d items want 7", len(got))
	}
	t.Logf("分页遍历完成: items=%d 判定依据: next_cursor 为空即终止", len(got))
}

func TestRetryOn503ThenSuccess(t *testing.T) {
	srv := mockserver.New([]mockserver.Item{{ID: "a"}}, 60)
	defer srv.Close()
	srv.FailNext(2) // 前两次 503，第三次成功

	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	c.Retry.BaseDelay = 10 * time.Millisecond
	c.Retry.MaxDelay = 50 * time.Millisecond

	var p itemsPage
	err := c.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, &p)
	if err != nil {
		t.Fatalf("idempotent GET must retry and succeed: %v", err)
	}
	t.Logf("判定依据: 503 可恢复 + GET 幂等 -> 重试后成功")
}

func TestNonIdempotentNotRetried(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	c := newClient(t, ts.URL, nil)
	c.Retry.BaseDelay = time.Millisecond
	err := c.Do(context.Background(), sdk.Request{Method: "POST", Path: "/x", Body: map[string]int{"a": 1}}, nil)
	if sdk.KindOf(err) != sdk.KindStatus {
		t.Fatalf("want KindStatus, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("POST must not be retried, attempts=%d", attempts)
	}
	t.Logf("判定依据: POST 非幂等 -> attempts=%d (不重试)", attempts)
}

func TestTimeoutCanceledQuotaDistinct(t *testing.T) {
	srv := mockserver.New(nil, 60)
	srv.Delay = 300 * time.Millisecond
	defer srv.Close()
	c := newClient(t, srv.URL(), nil)

	// 1. 超时
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := c.Do(ctx, sdk.Request{Method: "GET", Path: "/items"}, nil)
	cancel()
	if k := sdk.KindOf(err); k != sdk.KindTimeout {
		t.Fatalf("timeout: want %q got %q (%v)", sdk.KindTimeout, k, err)
	}
	t.Logf("超时 -> kind=%s", sdk.KindOf(err))

	// 2. 调用方取消
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel2() }()
	err = c.Do(ctx2, sdk.Request{Method: "GET", Path: "/items"}, nil)
	if k := sdk.KindOf(err); k != sdk.KindCanceled {
		t.Fatalf("cancel: want %q got %q (%v)", sdk.KindCanceled, k, err)
	}
	t.Logf("取消 -> kind=%s", sdk.KindOf(err))

	// 3. 限流配额耗尽（客户端限流器）
	c2 := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	c2.Limiter = sdk.NewRateLimiter(0.2, 1)
	if err := c2.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, nil); err != nil {
		t.Fatal(err) // 第一个令牌立即可得
	}
	ctx3, cancel3 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel3()
	err = c2.Do(ctx3, sdk.Request{Method: "GET", Path: "/items"}, nil)
	if k := sdk.KindOf(err); k != sdk.KindQuotaExhausted {
		t.Fatalf("quota: want %q got %q (%v)", sdk.KindQuotaExhausted, k, err)
	}
	t.Logf("配额耗尽 -> kind=%s (与超时/取消可区分)", sdk.KindOf(err))
}

func TestDecodeErrorAndRequestID(t *testing.T) {
	srv := mockserver.New(nil, 60)
	defer srv.Close()
	c := newClient(t, srv.URL(), nil)

	var out itemsPage
	err := c.Do(context.Background(), sdk.Request{Method: "GET", Path: "/bad"}, &out)
	if k := sdk.KindOf(err); k != sdk.KindDecode {
		t.Fatalf("want KindDecode, got %q (%v)", k, err)
	}
	if id := sdk.RequestIDOf(err); id != "req-bad" {
		t.Fatalf("request id not preserved, got %q", id)
	}
	t.Logf("响应体不合法 -> kind=%s request_id=%s", sdk.KindOf(err), sdk.RequestIDOf(err))
}

func TestStatusErrorKeepsRequestID(t *testing.T) {
	srv := mockserver.New(nil, 60)
	defer srv.Close()
	c := newClient(t, srv.URL(), nil) // 无凭证 -> 401

	err := c.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, nil)
	if k := sdk.KindOf(err); k != sdk.KindStatus {
		t.Fatalf("want KindStatus, got %q (%v)", k, err)
	}
	if id := sdk.RequestIDOf(err); id == "" {
		t.Fatal("request id must be preserved for troubleshooting")
	}
	t.Logf("401 -> kind=%s request_id=%s (4xx 不重试)", sdk.KindOf(err), sdk.RequestIDOf(err))
}

func TestServerRateLimit429Retried(t *testing.T) {
	srv := mockserver.New([]mockserver.Item{{ID: "a"}}, 60)
	srv.RateLimit = 10 // 每秒 10 个，首请求令牌不足 -> 429
	defer srv.Close()

	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	c.Retry.BaseDelay = 50 * time.Millisecond
	c.Retry.MaxDelay = 200 * time.Millisecond // Retry-After=1s 被封顶到 200ms

	var p itemsPage
	err := c.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, &p)
	if err != nil {
		t.Fatalf("429 must be retried and eventually succeed: %v", err)
	}
	t.Logf("判定依据: 429+Retry-After 可恢复 -> 退避后重试成功")
}

func TestConcurrentTokenRefreshOnce(t *testing.T) {
	srv := mockserver.New([]mockserver.Item{{ID: "a"}}, 1) // 凭证 1 秒过期
	defer srv.Close()

	tp := tokenProvider(srv.URL())
	c := newClient(t, srv.URL(), tp)

	// 首次调用建立缓存凭证。
	if err := c.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, nil); err != nil {
		t.Fatal(err)
	}
	if got := srv.RefreshCount(); got != 1 {
		t.Fatalf("initial refresh count=%d want 1", got)
	}

	time.Sleep(1100 * time.Millisecond) // 等待凭证过期

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, nil)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	if got := srv.RefreshCount(); got != 2 {
		t.Fatalf("concurrent refresh must execute exactly once more, total=%d want 2", got)
	}
	t.Logf("并发=%d 过期后实际刷新=1 (总计=%d) 判定依据: singleflight", n, srv.RefreshCount())
}

func TestTransportErrorNormalized(t *testing.T) {
	c := newClient(t, "http://127.0.0.1:1", nil) // 不可达地址
	c.Retry.BaseDelay = time.Millisecond
	c.Retry.MaxDelay = 5 * time.Millisecond
	err := c.Do(context.Background(), sdk.Request{Method: "GET", Path: "/x"}, nil)
	if k := sdk.KindOf(err); k != sdk.KindTransport {
		t.Fatalf("want KindTransport, got %q (%v)", k, err)
	}
	// 底层 *url.Error 不得直接透出：错误链顶端必须是 *sdk.Error。
	if _, ok := err.(*sdk.Error); !ok {
		t.Fatalf("underlying library error leaked: %T", err)
	}
	t.Logf("连接失败 -> kind=%s (底层异常已归一化)", sdk.KindOf(err))
}

