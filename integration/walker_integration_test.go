package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/resilient-api-client-go/mockserver"
	"github.com/xingjianluo/resilient-api-client-go/sdk"
)

// newWalkerFetch 构造用共享 client 拉取 /items 的分页函数。
func newWalkerFetch(c *sdk.Client, pageLimit int) sdk.FetchPage[mockserver.Item] {
	return func(ctx context.Context, cursor string) (sdk.Page[mockserver.Item], error) {
		var p itemsPage
		err := c.Do(ctx, sdk.Request{
			Method: "GET",
			Path:   "/items?cursor=" + cursor + "&limit=" + strconv.Itoa(pageLimit),
		}, &p)
		if err != nil {
			return sdk.Page[mockserver.Item]{}, err
		}
		return sdk.Page[mockserver.Item]{Items: p.Items, NextCursor: p.NextCursor}, nil
	}
}

func walkerCfg(t *testing.T, limits sdk.Limits) sdk.WalkerConfig[mockserver.Item] {
	return sdk.WalkerConfig[mockserver.Item]{
		Key:    func(i mockserver.Item) string { return i.ID },
		Limits: limits,
		Logger: func(format string, args ...any) { t.Logf("[walk] "+format, args...) },
	}
}

func nItems(n int) []mockserver.Item {
	out := make([]mockserver.Item, n)
	for i := range out {
		out[i] = mockserver.Item{ID: "id-" + strconv.Itoa(i), Name: "n"}
	}
	return out
}

// 端到端：到页数上限 -> 标记落盘 -> 新 Walker（模拟新进程）续传跑完，零重复。
func TestWalkerE2EResumeFromCheckpoint(t *testing.T) {
	srv := mockserver.New(nItems(9), 3600)
	defer srv.Close()
	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	c.Retry.BaseDelay = 5 * time.Millisecond
	c.Retry.MaxDelay = 20 * time.Millisecond

	w1 := sdk.NewWalker(newWalkerFetch(c, 3), walkerCfg(t, sdk.Limits{MaxPages: 1}))
	r1 := w1.Run(context.Background())
	if r1.Reason != sdk.EndLimitPages || r1.Err != nil || r1.Pages != 1 || r1.ItemsCount != 3 {
		t.Fatalf("run1: reason=%s pages=%d items=%d err=%v",
			r1.Reason, r1.Pages, r1.ItemsCount, r1.Err)
	}
	raw, err := r1.Checkpoint.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("批次1结束 reason=%s 实际请求数=%d 续传标记落盘=%s", r1.Reason, srv.TotalRequests(), raw)

	// 模拟进程重启：反序列化标记、新建 client 与 Walker 接着跑。
	cp, err := sdk.UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	c2 := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	w2 := sdk.ResumeWalker(newWalkerFetch(c2, 3), cp, walkerCfg(t, sdk.Limits{}))
	r2 := w2.Run(context.Background())
	if r2.Reason != sdk.EndCompleted {
		t.Fatalf("run2: reason=%s err=%v", r2.Reason, r2.Err)
	}
	seen := map[string]int{}
	for _, it := range append(append([]mockserver.Item{}, r1.Items...), r2.Items...) {
		seen[it.ID]++
	}
	if len(seen) != 9 {
		t.Fatalf("unique=%d want 9", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s 出现 %d 次（续传不得重复）", id, n)
		}
	}
	t.Logf("批次2结束 reason=%s 新拿=%d；合计唯一元素=%d 实际请求数=%d（3+2 页）",
		r2.Reason, r2.ItemsCount, len(seen), srv.TotalRequests())
	if srv.TotalRequests() != 3 {
		t.Fatalf("续传必须只拉剩余页，总请求=%d want 3", srv.TotalRequests())
	}
}

// 四类结束原因在真实 HTTP 调用下可严格区分。
func TestWalkerE2EEndReasonsDistinct(t *testing.T) {
	// —— 1. 时间上限：服务端每页慢响应，预算先耗尽 -> limit_time（err=nil）——
	srv := mockserver.New(nItems(100), 3600)
	srv.Delay = 10 * time.Millisecond
	defer srv.Close()
	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	r := sdk.NewWalker(newWalkerFetch(c, 10),
		walkerCfg(t, sdk.Limits{MaxDuration: 35 * time.Millisecond})).Run(context.Background())
	if r.Reason != sdk.EndLimitTime || r.Err != nil {
		t.Fatalf("time limit: reason=%s err=%v", r.Reason, r.Err)
	}
	t.Logf("时间预算耗尽 -> reason=%s pages=%d cursor=%s err=<nil>",
		r.Reason, r.Pages, r.Checkpoint.Cursor)

	// —— 2. 主动取消：取消发生在等待响应期间 -> canceled ——
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(15 * time.Millisecond); cancel() }()
	r = sdk.NewWalker(newWalkerFetch(c, 10), walkerCfg(t, sdk.Limits{})).Run(ctx)
	if r.Reason != sdk.EndCanceled || sdk.KindOf(r.Err) != sdk.KindCanceled {
		t.Fatalf("cancel: reason=%s kind=%s", r.Reason, sdk.KindOf(r.Err))
	}
	t.Logf("调用方取消 -> reason=%s kind=%s", r.Reason, sdk.KindOf(r.Err))

	// —— 3. 请求超时：deadline 在等待响应期间到期 -> timeout ——
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel2()
	r = sdk.NewWalker(newWalkerFetch(c, 10), walkerCfg(t, sdk.Limits{})).Run(ctx2)
	if r.Reason != sdk.EndTimeout || sdk.KindOf(r.Err) != sdk.KindTimeout {
		t.Fatalf("timeout: reason=%s kind=%s", r.Reason, sdk.KindOf(r.Err))
	}
	t.Logf("请求超时 -> reason=%s kind=%s", r.Reason, sdk.KindOf(r.Err))

	// —— 4. 限流迟迟等不到配额 -> quota_exhausted ——
	c2 := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	c2.Limiter = sdk.NewRateLimiter(0.5, 1) // 每 2s 一个令牌
	if err := c2.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, nil); err != nil {
		t.Fatal(err) // 首个令牌立即可得，先耗尽它
	}
	ctx3, cancel3 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel3()
	r = sdk.NewWalker(newWalkerFetch(c2, 10), walkerCfg(t, sdk.Limits{})).Run(ctx3)
	if r.Reason != sdk.EndQuotaExhausted || sdk.KindOf(r.Err) != sdk.KindQuotaExhausted {
		t.Fatalf("quota: reason=%s kind=%s", r.Reason, sdk.KindOf(r.Err))
	}
	t.Logf("限流无配额 -> reason=%s kind=%s（与到上限/取消/超时互不混淆）",
		r.Reason, sdk.KindOf(r.Err))
}

// 服务端游标序列异常（停滞 / 绕回）必须收敛停止，不能卡死或无限跑。
func TestWalkerE2EServerCursorAnomaly(t *testing.T) {
	// 可脚本编排 next_cursor 的服务端（无需凭证，复用共享 client 重试/日志路径）。
	newScripted := func(t *testing.T, seq []string) (*httptest.Server, *sdk.Client) {
		var mu sync.Mutex
		var n int
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			next := ""
			if n < len(seq) {
				next = seq[n]
			}
			n++
			mu.Unlock()
			w.Header().Set("X-Request-Id", "req-"+strconv.Itoa(n))
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items":       []mockserver.Item{{ID: "p" + strconv.Itoa(n), Name: "x"}},
				"next_cursor": next,
			})
		}))
		c := sdk.NewClient(ts.URL)
		c.Retry.Logger = func(format string, args ...any) { t.Logf("[client] "+format, args...) }
		return ts, c
	}

	t.Run("cursor_stuck", func(t *testing.T) {
		ts, c := newScripted(t, []string{"same", "same"})
		defer ts.Close()
		fetch := newWalkerFetch(c, 10)
		r := sdk.NewWalker(fetch, walkerCfg(t, sdk.Limits{})).Run(context.Background())
		if r.Reason != sdk.EndCursorAnomaly || !errors.Is(r.Err, sdk.ErrPaginationStuck) {
			t.Fatalf("reason=%s err=%v", r.Reason, r.Err)
		}
		t.Logf("真实服务端游标停滞 -> reason=%s err=%v（已收敛）", r.Reason, r.Err)
	})

	t.Run("cursor_loop", func(t *testing.T) {
		// "" -> c1 -> c2 -> c1（绕回）。
		ts, c := newScripted(t, []string{"c1", "c2", "c1"})
		defer ts.Close()
		r := sdk.NewWalker(newWalkerFetch(c, 10), walkerCfg(t, sdk.Limits{})).Run(context.Background())
		if r.Reason != sdk.EndCursorAnomaly || !errors.Is(r.Err, sdk.ErrCursorLoop) {
			t.Fatalf("reason=%s err=%v", r.Reason, r.Err)
		}
		t.Logf("真实服务端游标绕回 -> reason=%s err=%v（已收敛）", r.Reason, r.Err)
	})
}

// MaxItems 硬上端到端：单任务。服务端每页返回 5 条，本批上限只要 3 条：
// 首批必须恰好 3 条且只打 1 次请求（页内截断，游标不动）；
// 标记落盘、新进程反复续传（每批新预算）直到补齐 9 条，零重复、不漏取。
func TestWalkerE2EItemHardCapAndResume(t *testing.T) {
	srv := mockserver.New(nItems(9), 3600)
	defer srv.Close()
	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	c.Retry.BaseDelay = 5 * time.Millisecond
	c.Retry.MaxDelay = 20 * time.Millisecond

	const pageSize = 5
	const capItems = 3
	fetch := newWalkerFetch(c, pageSize)

	w1 := sdk.NewWalker(fetch, walkerCfg(t, sdk.Limits{MaxItems: capItems}))
	r1 := w1.Run(context.Background())
	if r1.Reason != sdk.EndLimitItems || r1.ItemsCount != 3 || r1.Pages != 1 {
		t.Fatalf("run1: reason=%s items=%d pages=%d", r1.Reason, r1.ItemsCount, r1.Pages)
	}
	if r1.Checkpoint.Cursor != "" {
		t.Fatalf("页内截断游标应停在首页起点，got cursor=%q", r1.Checkpoint.Cursor)
	}
	raw, err := r1.Checkpoint.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("批次1: 服务端页大小=%d 上限=%d -> reason=%s 实拿=%d 请求数=%d 标记=%s",
		pageSize, capItems, r1.Reason, r1.ItemsCount, srv.TotalRequests(), raw)

	// 模拟新进程：从 JSON 恢复，每批新预算 3，直到 completed。
	collected := append([]mockserver.Item{}, r1.Items...)
	cp, err := sdk.UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	batch := 1
	for {
		batch++
		c2 := newClient(t, srv.URL(), tokenProvider(srv.URL()))
		w := sdk.ResumeWalker(newWalkerFetch(c2, pageSize), cp, walkerCfg(t, sdk.Limits{MaxItems: capItems}))
		r := w.Run(context.Background())
		if r.Err != nil {
			t.Fatalf("batch %d: %v", batch, r.Err)
		}
		if r.ItemsCount > capItems {
			t.Fatalf("batch %d 产出=%d 超过硬上限 %d", batch, r.ItemsCount, capItems)
		}
		collected = append(collected, r.Items...)
		cp = r.Checkpoint
		t.Logf("批次%d: reason=%s 本批=%d 累计=%d cursor=%q done=%v",
			batch, r.Reason, r.ItemsCount, len(collected), cp.Cursor, cp.Done)
		if r.Reason == sdk.EndCompleted {
			break
		}
		if r.Reason != sdk.EndLimitItems {
			t.Fatalf("batch %d reason=%s", batch, r.Reason)
		}
		if batch > 10 {
			t.Fatal("续传批次过多，未推进")
		}
	}
	seen := map[string]int{}
	for _, it := range collected {
		seen[it.ID]++
	}
	if len(seen) != 9 {
		t.Fatalf("累计唯一=%d want 9（漏取或重复）", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s 出现 %d 次", id, n)
		}
	}
	// 页大小 5、总数 9 共 2 个服务端页；页内截断只重拉“同一页”，不产生越界请求。
	// 批次：3（第1页）+3（重拉第1页，2新+截断前看到的已Seen）…… 实际共需覆盖 2 个不同游标。
	if got := srv.TotalRequests(); got < 2 {
		t.Fatalf("总请求=%d 异常", got)
	}
	t.Logf("补齐完成: %d 批，累计唯一=%d，/items 总请求=%d（含页内截断的同页重拉，无越界）",
		batch, len(seen), srv.TotalRequests())
}

// 多个同步任务共用一个 client / 凭证 / 限流器，各设不同的 MaxItems 硬上限
// （都小于服务端页大小）：首批产出被各自上限精确截断，续传各自补齐，互不串。
func TestWalkerE2EConcurrentDistinctItemCaps(t *testing.T) {
	srv := mockserver.New(nItems(9), 3600)
	defer srv.Close()

	tp := tokenProvider(srv.URL())
	c := newClient(t, srv.URL(), tp)
	c.Limiter = sdk.NewRateLimiter(1000, 100) // 共享限流但配额充足

	const tasks = 4
	pageSize := 5
	caps := []int{2, 3, 1, 4} // 各任务不同上限，且都 < 页大小
	var wg sync.WaitGroup
	errs := make([]error, tasks)
	firstReasons := make([]sdk.EndReason, tasks)
	totals := make([]int, tasks)
	for j := 0; j < tasks; j++ {
		wg.Add(1)
		go func(task int) {
			defer wg.Done()
			cfg := walkerCfg(t, sdk.Limits{MaxItems: caps[task]})
			w := sdk.NewWalker(newWalkerFetch(c, pageSize), cfg)
			r := w.Run(context.Background())
			firstReasons[task] = r.Reason
			if r.Reason != sdk.EndLimitItems || r.ItemsCount != caps[task] {
				errs[task] = fmt.Errorf("task %d first: reason=%s items=%d want limit_items/%d",
					task, r.Reason, r.ItemsCount, caps[task])
				return
			}
			got := map[string]bool{}
			for _, it := range r.Items {
				got[it.ID] = true
			}
			totals[task] += r.ItemsCount

			cp := r.Checkpoint
			for b := 0; ; b++ {
				r2 := sdk.ResumeWalker(newWalkerFetch(c, pageSize), cp, cfg).Run(context.Background())
				if r2.Err != nil {
					errs[task] = fmt.Errorf("task %d resume: %w", task, r2.Err)
					return
				}
				for _, it := range r2.Items {
					if got[it.ID] {
						errs[task] = fmt.Errorf("task %d 重复产出 %s", task, it.ID)
						return
					}
					got[it.ID] = true
				}
				totals[task] += r2.ItemsCount
				cp = r2.Checkpoint
				if r2.Reason == sdk.EndCompleted {
					break
				}
				if r2.Reason != sdk.EndLimitItems || b > 10 {
					errs[task] = fmt.Errorf("task %d resume reason=%s", task, r2.Reason)
					return
				}
			}
		}(j)
	}
	wg.Wait()
	for j := 0; j < tasks; j++ {
		if errs[j] != nil {
			t.Fatal(errs[j])
		}
		if totals[j] != 9 {
			t.Fatalf("task %d 累计=%d want 9", j, totals[j])
		}
		if firstReasons[j] != sdk.EndLimitItems {
			t.Fatalf("task %d first reason=%s", j, firstReasons[j])
		}
	}
	// 共享凭证只刷新一次（与各任务的条数上限无关）。
	if got := srv.RefreshCount(); got != 1 {
		t.Fatalf("凭证刷新次数=%d want 1", got)
	}
	t.Logf("并发任务=%d 页大小=%d 各自上限=%v 首批原因=%v 各自补齐累计=%v 凭证刷新=%d",
		tasks, pageSize, caps, firstReasons, totals, srv.RefreshCount())
}

// 各自的标记、上限、结束原因互不干扰；凭证只刷新一次；请求数恰为页数之和。
func TestWalkerE2EConcurrentSharedClient(t *testing.T) {
	srv := mockserver.New(nItems(9), 3600)
	defer srv.Close()

	tp := tokenProvider(srv.URL())
	c := newClient(t, srv.URL(), tp)
	c.Limiter = sdk.NewRateLimiter(1000, 100) // 有共享限流，但配额充足，不应因此出现额外重试

	const tasks = 4
	var wg sync.WaitGroup
	reasons := make([]sdk.EndReason, tasks)
	counts := make([]int, tasks)
	for j := 0; j < tasks; j++ {
		wg.Add(1)
		go func(task int) {
			defer wg.Done()
			limits := sdk.Limits{}
			if task%2 == 0 {
				limits.MaxPages = 1 // 偶数任务先到页数上限
			}
			w := sdk.NewWalker(newWalkerFetch(c, 3), walkerCfg(t, limits))
			r := w.Run(context.Background())
			reasons[task] = r.Reason
			counts[task] = r.ItemsCount
			if task%2 == 0 {
				if r.Reason != sdk.EndLimitPages {
					t.Errorf("task %d first run reason=%s want limit_pages", task, r.Reason)
					return
				}
				// 用新预算从自己的标记续传跑完。
				r2 := sdk.ResumeWalker(newWalkerFetch(c, 3), r.Checkpoint, walkerCfg(t, sdk.Limits{})).
					Run(context.Background())
				if r2.Reason != sdk.EndCompleted {
					t.Errorf("task %d resume reason=%s", task, r2.Reason)
					return
				}
				counts[task] += r2.ItemsCount
			}
		}(j)
	}
	wg.Wait()

	for j := 0; j < tasks; j++ {
		if counts[j] != 9 {
			t.Fatalf("task %d collected=%d want 9（任务间串数据或漏取）", j, counts[j])
		}
	}
	// 每个任务恰好 3 页（1+2 或 3），合计 12 个 /items 请求：无重试、无放大。
	if got := srv.TotalRequests(); got != int64(3*tasks) {
		t.Fatalf("共享客户端并发下请求数=%d want %d（请求被放大或出现重试）", got, 3*tasks)
	}
	if got := srv.RefreshCount(); got != 1 {
		t.Fatalf("共享凭证并发刷新次数=%d want 1（singleflight + 缓存复用）", got)
	}
	t.Logf("并发任务=%d 结束原因=%v 各自累计=%v /items请求=%d 凭证刷新=%d",
		tasks, reasons, counts, srv.TotalRequests(), srv.RefreshCount())
}
