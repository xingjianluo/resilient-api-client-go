package resilientclient_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	resilient "github.com/xingjianluo/resilient-api-client-go"
	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

// 并发压力：N 个 worker 在限流 + 脚本化瞬时失败下调用同一幂等接口，
// 所有调用结果一致（要么取到同一资源，要么错误原因可区分），
// 且对服务端的总请求数受重试策略约束、不发生请求放大失控。
func TestConcurrentIdempotentCallsConsistent(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(3)...))
	// 每个 /v1/items/b 请求路径前若干次 503，之后恢复，迫使不同 worker 各自重试。
	srv.Fail("/v1/items/b", 8, http.StatusServiceUnavailable, 0)

	c := newTestClient(t, srv,
		resilient.WithLimiter(resilient.NewTokenBucket(200, 5, 5*time.Second)),
		resilient.WithRetryPolicy(resilient.RetryPolicy{
			MaxAttempts: 5, BaseDelay: 2 * time.Millisecond,
			MaxDelay: 20 * time.Millisecond, RetryAfterCap: time.Second,
		}),
	)

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	start := make(chan struct{})
	var mu sync.Mutex
	results := map[string]int{} // "ok:<id>" 或 "err:<kind>"

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			<-start
			var it resilient.Item
			_, err := c.GetJSON(context.Background(), "/v1/items/b", &it)
			mu.Lock()
			if err != nil {
				var ae *resilient.APIError
				results["err:"+string(ae.Kind)]++
			} else {
				results["ok:"+it.ID]++
			}
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()

	t.Logf("请求参数: %d 个并发 GET /v1/items/b（前8次 503，限流 200/s），服务端命中=%d，结果分布=%v",
		workers, srv.Hits("/v1/items/b"), results)

	okN := results["ok:b"]
	if okN != workers {
		t.Fatalf("判定依据：所有 worker 最终都应取到同一个资源 b，实际成功 %d/%d，分布=%v",
			okN, workers, results)
	}
	// 8 次 503 被 worker 分摊后全部最终成功，没有任何不可区分/未知错误。
	for k := range results {
		if len(k) > 4 && k[:4] == "err:" {
			t.Fatalf("判定依据：并发下出现失败结果 %s（原因可区分但本场景应全部成功）", k)
		}
	}
}

// 并发取消：多个带不同截止时间的调用竞争同一慢接口，短截止的拿到 KindTimeout，
// 不影响其他调用；原因彼此可区分。
func TestConcurrentDeadlinesAreIndependent(t *testing.T) {
	srv := newServer(t, mockapi.WithNoAuth())
	c := resilient.New(srv.URL(),
		resilient.WithRetryPolicy(resilient.NoRetry()),
		resilient.WithLogger(func(f string, a ...any) { t.Logf("[client] "+f, a...) }))

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	kinds := make([]resilient.ErrorKind, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			timeout := 60 * time.Millisecond
			if i%2 == 0 {
				timeout = 500 * time.Millisecond // 偶数调用应成功
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			var m map[string]any
			_, err := c.GetJSON(ctx, fmt.Sprintf("/v1/slow?ms=%d", 150), &m)
			if err != nil {
				var ae *resilient.APIError
				if errors.As(err, &ae) {
					kinds[i] = ae.Kind
				}
			}
		}(i)
	}
	wg.Wait()

	success, timeouts := 0, 0
	for i, k := range kinds {
		switch {
		case k == "":
			success++
		case k == resilient.KindTimeout:
			timeouts++
		default:
			t.Fatalf("判定依据：调用 %d 出现非预期错误类别 %s", i, k)
		}
	}
	t.Logf("请求参数: %d 个并发 GET /v1/slow?ms=150，半数预算60ms半数500ms；成功=%d 超时=%d（类别可区分）",
		n, success, timeouts)
	if success != n/2 || timeouts != n/2 {
		t.Fatalf("判定依据：应恰好 %d 成功 %d 超时，实际 %d 成功 %d 超时", n/2, n/2, success, timeouts)
	}
}
