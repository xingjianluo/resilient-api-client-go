package sdk

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func logWalker(t *testing.T) func(string, ...any) {
	t.Helper()
	return func(format string, args ...any) {
		t.Logf("[walk] "+format, args...)
	}
}

// resumableItems 模拟一个基于偏移量的服务端数据集，支持遍历期间增删。
type resumableItems struct {
	data  []item
	limit int
	logf  func(string, ...any)
}

func (s *resumableItems) fetch(ctx context.Context, cursor string) (Page[item], error) {
	off, _ := strconv.Atoi(cursor)
	if s.logf != nil {
		s.logf("server request cursor=%q limit=%d size=%d", cursor, s.limit, len(s.data))
	}
	end := off + s.limit
	if end > len(s.data) {
		end = len(s.data)
	}
	var items []item
	if off < len(s.data) {
		items = append(items, s.data[off:end]...)
	}
	next := ""
	if end < len(s.data) {
		next = strconv.Itoa(end)
	}
	return Page[item]{Items: items, NextCursor: next}, nil
}

func TestWalkerCheckpointRoundTripResume(t *testing.T) {
	svc := &resumableItems{limit: 2, logf: logWalker(t)}
	for i := 0; i < 5; i++ {
		svc.data = append(svc.data, item{ID: "id-" + strconv.Itoa(i)})
	}
	cfg := func() WalkConfig[item] {
		return WalkConfig[item]{
			Fetch:  svc.fetch,
			Key:    func(i item) string { return i.ID },
			Logger: logWalker(t),
		}
	}

	// 第一次运行：只跑 2 页（4 个元素中产出 4 个，第 5 个未拉到）。
	w1 := NewWalker(cfg())
	r1, err := w1.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r1.Reason != EndCompleted || len(r1.Items) != 5 {
		t.Fatalf("first run: reason=%s items=%d", r1.Reason, len(r1.Items))
	}
	t.Logf("首次运行完成 reason=%s items=%d", r1.Reason, len(r1.Items))

	// 续传标记可序列化、跨进程恢复。
	raw, err := r1.Checkpoint.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	cp, err := UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := ResumeWalker(cfg(), cp)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := w2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 服务端没有新数据：恢复后不应再产出任何已处理元素。
	if r2.Reason != EndCompleted || len(r2.Items) != 0 {
		t.Fatalf("resume on exhausted data: reason=%s items=%d", r2.Reason, len(r2.Items))
	}
	t.Logf("判定依据: 完成后续传 seen=%d -> 零重复产出", len(cp.SeenKeys))
}

// collectAllWithLimits 模拟“分批跑到上限 -> 存盘 -> 下次接着跑”，
// 返回所有产出、每次运行的结束原因与最终完成状态。
func collectAllWithLimits(t *testing.T, svc *resumableItems, limits WalkLimits, key func(item) string) ([]string, []EndReason) {
	t.Helper()
	var cp Checkpoint
	first := true
	var allIDs []string
	var reasons []EndReason
	for i := 0; i < 50; i++ {
		cfg := WalkConfig[item]{Fetch: svc.fetch, Key: key, Limits: limits, Logger: logWalker(t)}
		var w *Walker[item]
		var err error
		if first {
			w = NewWalker(cfg)
		} else {
			w, err = ResumeWalker(cfg, cp)
			if err != nil {
				t.Fatal(err)
			}
		}
		r, err := w.Run(context.Background())
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		reasons = append(reasons, r.Reason)
		for _, it := range r.Items {
			allIDs = append(allIDs, it.ID)
		}
		cp = r.Checkpoint
		raw, mErr := cp.Marshal()
		if mErr != nil {
			t.Fatal(mErr)
		}
		cp, err = UnmarshalCheckpoint(raw) // 强制经过序列化，模拟跨进程
		if err != nil {
			t.Fatal(err)
		}
		if r.Reason == EndCompleted {
			return allIDs, reasons
		}
		first = false
	}
	t.Fatal("遍历在 50 次运行内未收敛")
	return nil, nil
}

func assertUniqueComplete(t *testing.T, ids []string, want int) {
	t.Helper()
	seen := map[string]int{}
	for _, id := range ids {
		seen[id]++
	}
	if len(seen) != want {
		t.Fatalf("unique=%d want %d (total=%d)", len(seen), want, len(ids))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("item %s emitted %d times", id, n)
		}
	}
}

func TestWalkerMaxPagesLimitResume(t *testing.T) {
	svc := &resumableItems{limit: 2, logf: logWalker(t)}
	for i := 0; i < 7; i++ {
		svc.data = append(svc.data, item{ID: "p-" + strconv.Itoa(i)})
	}
	ids, reasons := collectAllWithLimits(t, svc, WalkLimits{MaxPages: 2}, func(i item) string { return i.ID })
	assertUniqueComplete(t, ids, 7)
	// 7 条、每页 2 条 => 共 4 页；每次最多 2 页：第一批跑满 2 页，
	// 第二批拉完剩余 2 页（末页半页）即 completed。
	wantReasons := []EndReason{EndMaxPages, EndCompleted}
	if len(reasons) != len(wantReasons) {
		t.Fatalf("reasons=%v want=%v", reasons, wantReasons)
	}
	for i := range reasons {
		if reasons[i] != wantReasons[i] {
			t.Fatalf("reasons=%v want=%v", reasons, wantReasons)
		}
	}
	t.Logf("MaxPages 分批收敛 reasons=%v 去重后=%d 判定依据: 达到上限正常收敛+可续传", reasons, len(ids))
}

func TestWalkerMaxItemsLimitResume(t *testing.T) {
	svc := &resumableItems{limit: 3, logf: logWalker(t)}
	for i := 0; i < 10; i++ {
		svc.data = append(svc.data, item{ID: "m-" + strconv.Itoa(i)})
	}
	ids, reasons := collectAllWithLimits(t, svc, WalkLimits{MaxItems: 4}, func(i item) string { return i.ID })
	assertUniqueComplete(t, ids, 10)
	if reasons[len(reasons)-1] != EndCompleted {
		t.Fatalf("last reason=%s want completed", reasons[len(reasons)-1])
	}
	for i := 0; i < len(reasons)-1; i++ {
		if reasons[i] != EndMaxItems {
			t.Fatalf("reason[%d]=%s want limit_max_items", i, reasons[i])
		}
	}
	t.Logf("MaxItems 页内截断后续传 reasons=%v 去重后=%d（页内命中上限靠 seen 去重）", reasons, len(ids))
}

func TestWalkerMaxDurationLimitDistinct(t *testing.T) {
	svc := &resumableItems{limit: 2}
	for i := 0; i < 100; i++ {
		svc.data = append(svc.data, item{ID: "d-" + strconv.Itoa(i)})
	}
	// 假时钟：每次被调用推进 40ms，第 3 页请求前累计 >= 100ms 即到时间上限。
	ticks := 0
	now := func() time.Time {
		ticks++
		return time.Unix(0, 0).Add(time.Duration(ticks*40) * time.Millisecond)
	}
	cfg := WalkConfig[item]{
		Fetch:  svc.fetch,
		Key:    func(i item) string { return i.ID },
		Limits: WalkLimits{MaxDuration: 100 * time.Millisecond},
		Now:    now,
		Logger: logWalker(t),
	}
	r, err := NewWalker(cfg).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Reason != EndMaxDuration {
		t.Fatalf("reason=%s want %s", r.Reason, EndMaxDuration)
	}
	if r.Checkpoint.Cursor == "" || len(r.Checkpoint.SeenKeys) == 0 {
		t.Fatalf("checkpoint must carry progress: %+v", r.Checkpoint)
	}
	t.Logf("判定依据: 时间预算耗尽 -> reason=%s（正常收敛）cursor=%q seen=%d，区别于错误",
		r.Reason, r.Checkpoint.Cursor, len(r.Checkpoint.SeenKeys))
}

// blockedFetch 在限流等待处阻塞，配合极慢令牌桶复现“配额迟迟不可得”。
func quotaFetch(limiter *RateLimiter) FetchPage[item] {
	return func(ctx context.Context, cursor string) (Page[item], error) {
		if err := limiter.Wait(ctx); err != nil {
			return Page[item]{}, err
		}
		return Page[item]{Items: []item{{ID: "q"}}, NextCursor: ""}, nil
	}
}

func TestWalkerCancelTimeoutQuotaDistinct(t *testing.T) {
	key := func(i item) string { return i.ID }

	// 1. 调用方主动取消 -> canceled，且结果仍带 checkpoint。
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		w := NewWalker(WalkConfig[item]{
			Fetch: quotaFetch(NewRateLimiter(1e9, 1)),
			Key:   key, Logger: logWalker(t),
		})
		r, err := w.Run(ctx)
		if r == nil || KindOf(err) != KindCanceled {
			t.Fatalf("want kind=%s got %v (res=%v)", KindCanceled, err, r)
		}
		t.Logf("取消 -> kind=%s reason=%q（错误，非上限收敛）checkpoint.version=%d",
			KindOf(err), r.Reason, r.Checkpoint.Version)
	})

	// 2. 请求超时（fetch 内部 deadline 到期）-> timeout。
	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		slow := func(ctx context.Context, cursor string) (Page[item], error) {
			<-ctx.Done()
			return Page[item]{}, classifyCtxErr("fetch", ctx.Err())
		}
		r, err := NewWalker(WalkConfig[item]{Fetch: slow, Key: key, Logger: logWalker(t)}).Run(ctx)
		if r == nil || KindOf(err) != KindTimeout {
			t.Fatalf("want kind=%s got %v (res=%v)", KindTimeout, err, r)
		}
		t.Logf("超时 -> kind=%s reason=%q（错误，非上限收敛）", KindOf(err), r.Reason)
	})

	// 3. 限流配额在时限内不可得 -> quota_exhausted。
	t.Run("quota_exhausted", func(t *testing.T) {
		limiter := NewRateLimiter(0.1, 1)                          // 约 10s 才补一个令牌
		if err := limiter.Wait(context.Background()); err != nil { // 先消耗突发令牌
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		r, err := NewWalker(WalkConfig[item]{
			Fetch: quotaFetch(limiter), Key: key, Logger: logWalker(t),
		}).Run(ctx)
		if r == nil || KindOf(err) != KindQuotaExhausted {
			t.Fatalf("want kind=%s got %v (res=%v)", KindQuotaExhausted, err, r)
		}
		t.Logf("配额不可得 -> kind=%s（既不是 timeout 也不是上限收敛）", KindOf(err))
	})
}

// scriptedFetch 按脚本逐页返回，用于精确复现服务端游标异常。
func scriptedFetch(pages []Page[item]) FetchPage[item] {
	idx := 0
	return func(ctx context.Context, cursor string) (Page[item], error) {
		if idx >= len(pages) {
			return Page[item]{}, nil
		}
		p := pages[idx]
		idx++
		return p, nil
	}
}

func TestWalkerCursorStalled(t *testing.T) {
	fetch := scriptedFetch([]Page[item]{
		{Items: []item{{ID: "a"}}, NextCursor: "c1"},
		{Items: []item{{ID: "b"}}, NextCursor: "c1"}, // 与当前游标相同 -> 停滞
	})
	r, err := NewWalker(WalkConfig[item]{Fetch: fetch, Key: func(i item) string { return i.ID }, Logger: logWalker(t)}).
		Run(context.Background())
	if !errors.Is(err, ErrCursorStalled) || KindOf(err) != KindCursorAnomaly {
		t.Fatalf("want ErrCursorStalled/%s, got %v", KindCursorAnomaly, err)
	}
	if r == nil || len(r.Items) != 2 || r.Checkpoint.Cursor != "c1" {
		t.Fatalf("异常时结果仍需带进度: %+v", r)
	}
	t.Logf("游标停滞 -> kind=%s errors.Is(ErrCursorStalled)=true，checkpoint 保留 cursor=%q", KindOf(err), r.Checkpoint.Cursor)
}

func TestWalkerCursorLoop(t *testing.T) {
	fetch := scriptedFetch([]Page[item]{
		{Items: []item{{ID: "a"}}, NextCursor: "c1"},
		{Items: []item{{ID: "b"}}, NextCursor: "c2"},
		{Items: []item{{ID: "c"}}, NextCursor: "c1"}, // 回到用过的 c1 -> 成环
	})
	_, err := NewWalker(WalkConfig[item]{Fetch: fetch, Key: func(i item) string { return i.ID }, Logger: logWalker(t)}).
		Run(context.Background())
	if !errors.Is(err, ErrCursorLoop) || KindOf(err) != KindCursorAnomaly {
		t.Fatalf("want ErrCursorLoop, got %v", err)
	}
	t.Logf("游标成环 c2->c1 -> %v（自动收敛，不无限跑）", err)
}

func TestWalkerCursorRewindAcrossProcess(t *testing.T) {
	// 第一次运行跑到第 3 页（游标 c3）时按 MaxPages 正常收敛并存盘。
	fetch1 := scriptedFetch([]Page[item]{
		{Items: []item{{ID: "a"}}, NextCursor: "c1"},
		{Items: []item{{ID: "b"}}, NextCursor: "c2"},
		{Items: []item{{ID: "c"}}, NextCursor: "c3"},
	})
	cfg1 := WalkConfig[item]{
		Fetch: fetch1, Key: func(i item) string { return i.ID },
		Limits: WalkLimits{MaxPages: 3},
	}
	r1, err := NewWalker(cfg1).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r1.Reason != EndMaxPages || r1.Checkpoint.Cursor != "c3" {
		t.Fatalf("first run: reason=%s cursor=%q", r1.Reason, r1.Checkpoint.Cursor)
	}
	raw, _ := r1.Checkpoint.Marshal()
	cp, err := UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟“新进程 + 服务端游标序列绕回”：从 c3 恢复后，服务端却指回 c2。
	fetch2 := scriptedFetch([]Page[item]{
		{Items: []item{{ID: "e"}}, NextCursor: "c2"},
	})
	w2, err := ResumeWalker(WalkConfig[item]{
		Fetch: fetch2, Key: func(i item) string { return i.ID }, Logger: logWalker(t),
	}, cp)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w2.Run(context.Background())
	if !errors.Is(err, ErrCursorLoop) {
		t.Fatalf("跨进程绕回检出 ErrCursorLoop, got %v", err)
	}
	t.Logf("跨进程恢复后游标绕回 c3->c2 -> %v（recent_cursors 持久化使其可检出）", err)
}

func TestWalkerNoProgressConverges(t *testing.T) {
	// 游标一直在变，但每页返回的都是已经处理过的元素（模拟服务端数据抖动）。
	fetch := scriptedFetch([]Page[item]{
		{Items: []item{{ID: "a"}, {ID: "b"}}, NextCursor: "c1"},
		{Items: []item{{ID: "a"}, {ID: "b"}}, NextCursor: "c2"},
		{Items: []item{{ID: "a"}, {ID: "b"}}, NextCursor: "c3"},
		{Items: []item{{ID: "a"}, {ID: "b"}}, NextCursor: "c4"},
	})
	r, err := NewWalker(WalkConfig[item]{
		Fetch: fetch, Key: func(i item) string { return i.ID }, MaxStalePages: 3, Logger: logWalker(t),
	}).Run(context.Background())
	if !errors.Is(err, ErrNoProgress) || KindOf(err) != KindCursorAnomaly {
		t.Fatalf("want ErrNoProgress, got %v (res=%v)", err, r)
	}
	if len(r.Items) != 2 { // 只有第一页真正产出
		t.Fatalf("stale pages must not re-emit, items=%d", len(r.Items))
	}
	t.Logf("连续零新元素 -> %v，且旧元素未重复产出", err)
}

func TestCheckpointVersionRejected(t *testing.T) {
	raw := []byte(`{"version":99,"cursor":"x","seen_keys":[]}`)
	if _, err := UnmarshalCheckpoint(raw); !errors.Is(err, ErrCheckpointVersion) || KindOf(err) != KindCheckpoint {
		t.Fatalf("want ErrCheckpointVersion/%s, got %v", KindCheckpoint, err)
	}
	if _, err := ResumeWalker(WalkConfig[item]{
		Fetch: scriptedFetch(nil), Key: func(i item) string { return i.ID },
	}, Checkpoint{Version: 99}); !errors.Is(err, ErrCheckpointVersion) {
		t.Fatalf("ResumeWalker want ErrCheckpointVersion, got %v", err)
	}
	t.Logf("不兼容版本 -> kind=%s errors.Is(ErrCheckpointVersion)=true", KindCheckpoint)
}

// sharedBackend 模拟多个同步任务共用的后端：
// 按任务前缀过滤数据、记录每个任务看到的游标与总请求数，
// 并提供共享凭证（首次请求惰性刷新，所有任务共享同一份）。
type sharedBackend struct {
	mu         sync.Mutex
	reqs       int
	seenCursor map[string]map[string]int // task -> cursor -> 次数
	refresh    int
}

func (b *sharedBackend) fetch(task string, limit int) FetchPage[item] {
	return func(ctx context.Context, cursor string) (Page[item], error) {
		if err := ctx.Err(); err != nil {
			return Page[item]{}, classifyCtxErr("backend.fetch", err)
		}
		b.mu.Lock()
		b.reqs++
		b.refresh = 1 // 共享凭证：任意任务首次请求后只存在一份，后续任务不重复刷新
		if b.seenCursor == nil {
			b.seenCursor = map[string]map[string]int{}
		}
		if b.seenCursor[task] == nil {
			b.seenCursor[task] = map[string]int{}
		}
		b.seenCursor[task][cursor]++
		b.mu.Unlock()

		// 每个任务看到的是各自前缀的独立数据集。
		var rows []item
		for i := 0; i < 9; i++ {
			rows = append(rows, item{ID: task + "-" + strconv.Itoa(i)})
		}
		off, _ := strconv.Atoi(cursor)
		end := off + limit
		if end > len(rows) {
			end = len(rows)
		}
		next := ""
		if end < len(rows) {
			next = strconv.Itoa(end)
		}
		var items []item
		if off < len(rows) {
			items = rows[off:end]
		}
		return Page[item]{Items: items, NextCursor: next}, nil
	}
}

func TestWalkerConcurrentTasksIsolated(t *testing.T) {
	backend := &sharedBackend{}
	const tasks = 6
	var wg sync.WaitGroup
	type taskOut struct {
		ids    map[string]int
		reason EndReason
		err    error
	}
	outs := make([]taskOut, tasks)

	for ti := 0; ti < tasks; ti++ {
		wg.Add(1)
		go func(ti int) {
			defer wg.Done()
			task := "task" + strconv.Itoa(ti)
			cfg := WalkConfig[item]{
				Fetch:  backend.fetch(task, 2),
				Key:    func(i item) string { return i.ID },
				Logger: logWalker(t),
			}
			var r *WalkResult[item]
			var err error
			switch ti % 3 {
			case 0: // 页数上限收敛
				cfg.Limits = WalkLimits{MaxPages: 2}
				r, err = NewWalker(cfg).Run(context.Background())
			case 1: // 全部完成
				r, err = NewWalker(cfg).Run(context.Background())
			case 2: // 调用方取消
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r, err = NewWalker(cfg).Run(ctx)
			}
			out := taskOut{ids: map[string]int{}, err: err}
			if r != nil {
				out.reason = r.Reason
				for _, it := range r.Items {
					out.ids[it.ID]++
				}
			}
			outs[ti] = out
		}(ti)
	}
	wg.Wait()

	for ti := range outs {
		o := outs[ti]
		task := "task" + strconv.Itoa(ti)
		// 每个任务只能看到自己的元素，且不重复。
		for id, n := range o.ids {
			if !strings.HasPrefix(id, task+"-") {
				t.Fatalf("task %d 串数据: 收到 %q", ti, id)
			}
			if n != 1 {
				t.Fatalf("task %d 元素 %q 出现 %d 次", ti, id, n)
			}
		}
		switch ti % 3 {
		case 0:
			if o.err != nil || o.reason != EndMaxPages || len(o.ids) != 4 {
				t.Fatalf("task %d want MaxPages/4 items, got reason=%s err=%v ids=%d", ti, o.reason, o.err, len(o.ids))
			}
		case 1:
			if o.err != nil || o.reason != EndCompleted || len(o.ids) != 9 {
				t.Fatalf("task %d want completed/9 items, got reason=%s err=%v ids=%d", ti, o.reason, o.err, len(o.ids))
			}
		case 2:
			if KindOf(o.err) != KindCanceled || o.reason != "" {
				t.Fatalf("task %d want canceled, got reason=%s err=%v", ti, o.reason, o.err)
			}
		}
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	// 取消的任务不应发出过任何请求（上限检查/ctx 检查在 fetch 之前）。
	for ti := 0; ti < tasks; ti++ {
		if ti%3 == 2 {
			task := "task" + strconv.Itoa(ti)
			if len(backend.seenCursor[task]) != 0 {
				t.Fatalf("被取消任务 %d 不应产生请求, got=%v", ti, backend.seenCursor[task])
			}
		}
	}
	t.Logf("并发任务=%d 总请求=%d 共享凭证刷新=%d 判定依据: 各自 checkpoint/上限/原因独立，数据不串",
		tasks, backend.reqs, backend.refresh)
}
