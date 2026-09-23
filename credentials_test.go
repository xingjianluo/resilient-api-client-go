package resilientclient_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	resilient "github.com/xingjianluo/resilient-api-client-go"
	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

// 并发取凭证：过期瞬间 N 个调用方同时触发，实际刷新只执行一次。
func TestCredentialSingleFlight(t *testing.T) {
	srv := newServer(t,
		mockapi.WithNoAuth(),
		mockapi.WithTokenTTL(time.Hour),
		mockapi.WithInitialItems(mustItems(2)...),
	)
	c := newTestClient(t, srv)

	const n = 25
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start // 栅栏：尽量让 25 个调用同时进入
			_, _, _, err := c.Do(context.Background(), http.MethodGet, "/v1/items", nil)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发调用失败: %v", err)
		}
	}
	if hits := srv.TokenHits(); hits != 1 {
		t.Fatalf("判定依据：25 个并发调用只应触发 1 次凭证刷新，实际令牌端点命中 %d 次", hits)
	}
	t.Logf("请求参数: 25 个并发 GET /v1/items（冷启动无缓存），判定依据: token 端点命中=1")
}

// 凭证有效期内不再刷新：缓存命中。
func TestCredentialCachedUntilExpiry(t *testing.T) {
	srv := newServer(t,
		mockapi.WithTokenTTL(time.Hour),
		mockapi.WithInitialItems(mustItems(2)...),
	)
	c := newTestClient(t, srv)
	for i := 0; i < 5; i++ {
		var it resilient.Item
		if _, err := c.GetJSON(context.Background(), "/v1/items/a", &it); err != nil {
			t.Fatalf("第 %d 次调用失败: %v", i+1, err)
		}
	}
	if hits := srv.TokenHits(); hits != 1 {
		t.Fatalf("判定依据：有效期内 5 次调用只应刷新 1 次，实际 %d 次", hits)
	}
	t.Logf("请求参数: 5 次 GET（令牌 TTL=1h），判定依据: 缓存命中，token 端点命中=1")
}

// 凭证过期后自动刷新，且新凭证被用于重试：旧令牌 401 -> 刷新 -> 200。
func TestCredentialExpiryAutoRefresh(t *testing.T) {
	srv := newServer(t, mockapi.WithTokenTTL(60*time.Millisecond),
		mockapi.WithInitialItems(mustItems(2)...))
	c := newTestClient(t, srv)

	var it resilient.Item
	if _, err := c.GetJSON(context.Background(), "/v1/items/a", &it); err != nil {
		t.Fatalf("首次调用失败: %v", err)
	}
	time.Sleep(80 * time.Millisecond) // 等令牌过期

	var it2 resilient.Item
	reqID, err := c.GetJSON(context.Background(), "/v1/items/a", &it2)
	if err != nil {
		t.Fatalf("过期后调用应透明刷新并成功: %v", err)
	}
	if hits := srv.TokenHits(); hits != 2 {
		t.Fatalf("判定依据：过期后应触发第 2 次刷新，实际 %d 次", hits)
	}
	t.Logf("请求参数: GET /v1/items/a（令牌已过期），重试次数=1（401->刷新->200），request_id=%s", reqID)
}

// 某个等待方超时不影响其他等待方共享同一次刷新结果（不互相阻塞/放大）。
func TestCredentialWaiterTimeoutDoesNotKillRefresh(t *testing.T) {
	var refreshCalls int64
	src := func(ctx context.Context) (resilient.AccessToken, error) {
		atomic.AddInt64(&refreshCalls, 1)
		time.Sleep(150 * time.Millisecond) // 刷新耗时
		return resilient.AccessToken{Value: "fresh", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	p := resilient.NewCachedTokenProvider(src, 0)

	// 等待方 A：只给 40ms，必定在刷新完成前超时。
	ctxA, cancelA := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelA()
	_, errA := p.Token(ctxA)
	if !errors.Is(errA, resilient.ErrTimeout) {
		t.Fatalf("判定依据：等待方 A 应超时，实际 %v", errA)
	}
	// 等待方 B：给 500ms，应共享同一次刷新并成功，即便 A 已超时离开。
	ctxB, cancelB := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelB()
	tokB, errB := p.Token(ctxB)
	if errB != nil {
		t.Fatalf("判定依据：等待方 B 应成功共享刷新结果，实际 %v", errB)
	}
	if tokB != "fresh" {
		t.Fatalf("判定依据：B 应拿到新令牌 fresh，实际 %q", tokB)
	}
	if n := atomic.LoadInt64(&refreshCalls); n != 1 {
		t.Fatalf("判定依据：A 超时离开后刷新不得被取消/重发，实际刷新 %d 次", n)
	}
	t.Logf("判定依据: A 40ms 超时返回 KindTimeout；刷新继续；B 共享同一刷新，实际刷新次数=1")
}

// 刷新失败：归一化为 KindCredential，后续新调用会重新尝试（失败结果不被缓存）。
func TestCredentialRefreshFailureRetriable(t *testing.T) {
	var shouldFail int64 = 1
	var calls int64
	src := func(ctx context.Context) (resilient.AccessToken, error) {
		atomic.AddInt64(&calls, 1)
		if atomic.LoadInt64(&shouldFail) == 1 {
			return resilient.AccessToken{}, errors.New("auth server down")
		}
		return resilient.AccessToken{Value: "ok", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	p := resilient.NewCachedTokenProvider(src, 0)

	_, err := p.Token(context.Background())
	if !errors.Is(err, resilient.ErrCredential) {
		t.Fatalf("判定依据：刷新失败应归一化为 KindCredential，实际 %v", err)
	}
	atomic.StoreInt64(&shouldFail, 0)
	tok, err := p.Token(context.Background())
	if err != nil || tok != "ok" {
		t.Fatalf("判定依据：失败不缓存，后续应刷新成功，tok=%q err=%v", tok, err)
	}
	if calls != 2 {
		t.Fatalf("判定依据：失败后再次调用应重新刷新，实际调用 %d 次", calls)
	}
	t.Logf("判定依据: 刷新失败->KindCredential（不缓存），恢复后->成功，实际刷新次数=%d", calls)
}

// 无效凭证在非幂等请求上不会被自动重试：POST 遇 401 直接返回 KindStatus(401)。
func TestCredential401NotRetriedForPost(t *testing.T) {
	srv := newServer(t, mockapi.WithTokenTTL(40*time.Millisecond),
		mockapi.WithInitialItems(mustItems(2)...))
	c := newTestClient(t, srv, resilient.WithRetryPolicy(resilient.RetryPolicy{
		MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond,
	}))

	// 先用一次 GET 取得缓存令牌，然后等其过期。
	var it resilient.Item
	if _, err := c.GetJSON(context.Background(), "/v1/items/a", &it); err != nil {
		t.Fatalf("预热失败: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	// 过期后的 POST：401 不允许通过重试掩盖（POST 非幂等），直接返回。
	status, _, _, err := c.Do(context.Background(), http.MethodPost, "/v1/echo",
		strings.NewReader(`{"hello":"world"}`))
	ae := assertKind(t, err, resilient.KindStatus)
	if status != http.StatusUnauthorized || ae.StatusCode != 401 {
		t.Fatalf("判定依据：过期令牌的 POST 应直接返回 401，status=%d", status)
	}
	if srv.Hits("/v1/echo") != 1 {
		t.Fatalf("判定依据：POST 401 不得重试，期望 /v1/echo 命中 1 次，实际 %d", srv.Hits("/v1/echo"))
	}
	t.Logf("请求参数: POST /v1/echo（令牌已过期），重试次数=0，判定依据: 非幂等请求不自动刷新重试")
}
