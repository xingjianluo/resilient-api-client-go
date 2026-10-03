package integration

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/resilient-api-client-go/mockserver"
	"github.com/xingjianluo/resilient-api-client-go/sdk"
)

// walkItems 用共享客户端构造一页 /items 的拉取函数；任务标识只体现在日志里，
// 数据隔离由各任务自己的 fetch/Walker/checkpoint 保证。
func walkItems(c *sdk.Client, pageLimit int) sdk.FetchPage[mockserver.Item] {
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

// runBatches 模拟多个进程批次：每批一个 Walker、达到页数上限后存盘，
// checkpoint 强制经过 JSON 序列化，直到 completed。
func runBatches(t *testing.T, c *sdk.Client, pageLimit, maxPagesPerRun int) ([]string, []sdk.EndReason) {
	t.Helper()
	var cp sdk.Checkpoint
	var all []string
	var reasons []sdk.EndReason
	first := true
	for batch := 0; batch < 50; batch++ {
		cfg := sdk.WalkConfig[mockserver.Item]{
			Fetch:  walkItems(c, pageLimit),
			Key:    func(i mockserver.Item) string { return i.ID },
			Limits: sdk.WalkLimits{MaxPages: maxPagesPerRun},
			Logger: func(format string, args ...any) {
				t.Logf("[batch%d] "+format, append([]any{batch}, args...)...)
			},
		}
		var w *sdk.Walker[mockserver.Item]
		var err error
		if first {
			w = sdk.NewWalker(cfg)
		} else {
			w, err = sdk.ResumeWalker(cfg, cp)
			if err != nil {
				t.Fatal(err)
			}
		}
		r, err := w.Run(context.Background())
		if err != nil {
			t.Fatalf("batch %d: %v", batch, err)
		}
		reasons = append(reasons, r.Reason)
		for _, it := range r.Items {
			all = append(all, it.ID)
		}
		raw, err := r.Checkpoint.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		cp, err = sdk.UnmarshalCheckpoint(raw) // 跨进程：只认 JSON 标记
		if err != nil {
			t.Fatal(err)
		}
		if r.Reason == sdk.EndCompleted {
			return all, reasons
		}
		first = false
	}
	t.Fatal("未在 50 批内完成")
	return nil, nil
}

func TestResumableExportAcrossBatches(t *testing.T) {
	var items []mockserver.Item
	for i := 0; i < 13; i++ {
		items = append(items, mockserver.Item{ID: "doc-" + strconv.Itoa(i), Name: "n"})
	}
	srv := mockserver.New(items, 300)
	defer srv.Close()

	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	got, reasons := runBatches(t, c, 4, 2) // 每页 4 条，每批最多 2 页 => 8 条/批

	seen := map[string]int{}
	for _, id := range got {
		seen[id]++
	}
	if len(seen) != 13 {
		t.Fatalf("unique=%d want 13 total=%d", len(seen), len(got))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s 重复 %d 次", id, n)
		}
	}
	if reasons[len(reasons)-1] != sdk.EndCompleted {
		t.Fatalf("最后一批必须 completed, reasons=%v", reasons)
	}
	for i := 0; i < len(reasons)-1; i++ {
		if reasons[i] != sdk.EndMaxPages {
			t.Fatalf("reasons=%v", reasons)
		}
	}
	t.Logf("分批导出完成 reasons=%v 去重后=%d/13 判定依据: 每批 MaxPages 正常收敛+JSON 续传", reasons, len(seen))
}

func TestConcurrentSharedClientIndependentWalks(t *testing.T) {
	// 两组数据放在同一服务端，两个任务各自只处理自己的前缀（模拟不同同步任务）。
	var items []mockserver.Item
	for _, prefix := range []string{"alpha", "beta"} {
		for i := 0; i < 11; i++ {
			items = append(items, mockserver.Item{ID: prefix + "-" + strconv.Itoa(i)})
		}
	}
	srv := mockserver.New(items, 300)
	defer srv.Close()

	tp := tokenProvider(srv.URL())
	c := newClient(t, srv.URL(), tp)
	c.Limiter = sdk.NewRateLimiter(100, 100) // 共享限流，不成为瓶颈

	type out struct {
		prefix string
		ids    map[string]int
		reason sdk.EndReason
		err    error
	}
	res := make([]out, 2)
	var wg sync.WaitGroup
	for idx, prefix := range []string{"alpha", "beta"} {
		wg.Add(1)
		go func(idx int, prefix string) {
			defer wg.Done()
			fetch := func(ctx context.Context, cursor string) (sdk.Page[mockserver.Item], error) {
				p, err := walkItems(c, 5)(ctx, cursor)
				if err != nil {
					return p, err
				}
				// 任务各自只认领自己的数据。
				filtered := sdk.Page[mockserver.Item]{NextCursor: p.NextCursor}
				for _, it := range p.Items {
					if len(it.ID) >= len(prefix) && it.ID[:len(prefix)] == prefix {
						filtered.Items = append(filtered.Items, it)
					}
				}
				return filtered, nil
			}
			cfg := sdk.WalkConfig[mockserver.Item]{
				Fetch: fetch,
				Key:   func(i mockserver.Item) string { return i.ID },
				Logger: func(format string, args ...any) {
					t.Logf("["+prefix+"] "+format, args...)
				},
			}
			if idx == 0 {
				cfg.Limits = sdk.WalkLimits{MaxPages: 1} // alpha 到页数上限
			}
			r, err := sdk.NewWalker(cfg).Run(context.Background())
			o := out{prefix: prefix, ids: map[string]int{}, err: err}
			if r != nil {
				o.reason = r.Reason
				for _, it := range r.Items {
					o.ids[it.ID]++
				}
			}
			res[idx] = o
		}(idx, prefix)
	}
	wg.Wait()

	for _, o := range res {
		if o.err != nil {
			t.Fatalf("%s: %v", o.prefix, o.err)
		}
		for id, n := range o.ids {
			if len(id) < len(o.prefix) || id[:len(o.prefix)] != o.prefix {
				t.Fatalf("%s 串入其它任务数据: %q", o.prefix, id)
			}
			if n != 1 {
				t.Fatalf("%s: %s 重复 %d 次", o.prefix, id, n)
			}
		}
	}
	if res[0].reason != sdk.EndMaxPages || len(res[0].ids) != 5 {
		t.Fatalf("alpha: reason=%s ids=%d want MaxPages/5", res[0].reason, len(res[0].ids))
	}
	if res[1].reason != sdk.EndCompleted || len(res[1].ids) != 11 {
		t.Fatalf("beta: reason=%s ids=%d want completed/11", res[1].reason, len(res[1].ids))
	}
	// 共享 TokenProvider：并发任务不得重复刷新（初始 1 次 + 过期后 0 次）。
	if n := srv.RefreshCount(); n != 1 {
		t.Fatalf("共享凭证刷新次数=%d want 1（并发任务共享 singleflight 结果）", n)
	}
	t.Logf("并发任务结束原因独立: alpha=%s(%d 条), beta=%s(%d 条); 共享凭证刷新=1",
		res[0].reason, len(res[0].ids), res[1].reason, len(res[1].ids))
}

func TestResumeUnderServerMutation(t *testing.T) {
	var items []mockserver.Item
	for i := 0; i < 10; i++ {
		items = append(items, mockserver.Item{ID: "mut-" + strconv.Itoa(i)})
	}
	srv := mockserver.New(items, 300)
	defer srv.Close()
	c := newClient(t, srv.URL(), tokenProvider(srv.URL()))

	cfg := sdk.WalkConfig[mockserver.Item]{
		Fetch:  walkItems(c, 5),
		Key:    func(i mockserver.Item) string { return i.ID },
		Limits: sdk.WalkLimits{MaxPages: 1}, // 每批只拉 1 页
		Logger: func(format string, args ...any) { t.Logf("[walk] "+format, args...) },
	}

	// 第一批：拿到前 5 条后到页数上限。
	r1, err := sdk.NewWalker(cfg).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r1.Reason != sdk.EndMaxPages || len(r1.Items) != 5 {
		t.Fatalf("batch1 reason=%s items=%d", r1.Reason, len(r1.Items))
	}
	raw, _ := r1.Checkpoint.Marshal()
	cp, err := sdk.UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}

	// 存盘期间服务端数据变动：删除一个已处理元素、追加两个新元素。
	srv.RemoveFirst() // mut-0 被删除
	srv.AddItem(mockserver.Item{ID: "mut-10"})
	srv.AddItem(mockserver.Item{ID: "mut-11"})
	t.Logf("服务端变动: 删除 mut-0（已处理），追加 mut-10/mut-11；offset 分页会使元素前移")

	// 后续批次放开工作量上限，恢复后一次跑完；seen 集合保证前移的旧元素不重复。
	continueCfg := cfg
	continueCfg.Limits = sdk.WalkLimits{}
	w2, err := sdk.ResumeWalker(continueCfg, cp)
	if err != nil {
		t.Fatal(err)
	}
	collected := map[string]int{}
	for _, it := range r1.Items {
		collected[it.ID]++
	}
	r2, err := w2.Run(context.Background())
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if r2.Reason != sdk.EndCompleted {
		t.Fatalf("resume reason=%s want completed", r2.Reason)
	}
	for _, it := range r2.Items {
		collected[it.ID]++
	}

	// 第一批产出的 5 个元素（含删除前已正常导出的 mut-0）各计一次。
	for id, n := range collected {
		if n != 1 {
			t.Fatalf("%s 重复 %d 次（seen 去重必须生效）", id, n)
		}
	}
	if !containsID(collected, "mut-10") || !containsID(collected, "mut-11") {
		t.Fatalf("新增元素必须被导出: collected=%v", keysOf(collected))
	}
	// 续传批次不得重新产出删除前已处理的元素（offset 前移的 mut-1..4 会被 seen 拦掉）。
	for _, id := range r2.Items {
		for _, old := range r1.Items {
			if id.ID == old.ID {
				t.Fatalf("续传重复产出已处理元素 %s", id.ID)
			}
		}
	}
	t.Logf("变动后续传收敛: 导出 %d 个唯一元素=%v，无重复；mut-0 删除前已正常交付",
		len(collected), keysOf(collected))
}

func containsID(m map[string]int, id string) bool { _, ok := m[id]; return ok }

func keysOf(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestWalkerQuotaExhaustedDistinctFromLimit(t *testing.T) {
	srv := mockserver.New([]mockserver.Item{{ID: "z"}}, 300)
	defer srv.Close()

	// 1. 到工作量上限：正常收敛，无 error，有明确 reason。
	c1 := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	r, err := sdk.NewWalker(sdk.WalkConfig[mockserver.Item]{
		Fetch: walkItems(c1, 10), Key: func(i mockserver.Item) string { return i.ID },
		Limits: sdk.WalkLimits{MaxItems: 1},
		Logger: func(format string, args ...any) { t.Logf("[limit] "+format, args...) },
	}).Run(context.Background())
	if err != nil || r.Reason != sdk.EndMaxItems {
		t.Fatalf("上限收敛必须无错误且 reason 明确: err=%v reason=%s", err, r.Reason)
	}

	// 2. 共享客户端限流配额迟迟不可得：KindQuotaExhausted 错误，reason 为空。
	c2 := newClient(t, srv.URL(), tokenProvider(srv.URL()))
	c2.Limiter = sdk.NewRateLimiter(0.1, 1)
	if err := c2.Do(context.Background(), sdk.Request{Method: "GET", Path: "/items"}, nil); err != nil {
		t.Fatal(err) // 消耗突发令牌
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = sdk.NewWalker(sdk.WalkConfig[mockserver.Item]{
		Fetch: walkItems(c2, 10), Key: func(i mockserver.Item) string { return i.ID },
		Logger: func(format string, args ...any) { t.Logf("[quota] "+format, args...) },
	}).Run(ctx)
	if sdk.KindOf(err) != sdk.KindQuotaExhausted {
		t.Fatalf("配额耗尽 -> want %s got %v", sdk.KindQuotaExhausted, err)
	}
	t.Logf("判定依据: 上限收敛 reason=%s/err=nil；配额不可得 kind=%s —— 两类不互相冒充",
		sdk.EndMaxItems, sdk.KindOf(err))
}
