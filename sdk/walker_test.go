package sdk

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

// testLogger 让单测日志展示每次请求参数与结束判定依据。
func testLogger(t *testing.T) func(string, ...any) {
	t.Helper()
	return func(format string, args ...any) { t.Logf("[walk] "+format, args...) }
}

// dataFetch 基于可变切片的偏移分页（模拟服务端，支持测试期间增删数据）。
func dataFetch(data *[]item, limit int) FetchPage[item] {
	var mu sync.Mutex
	return func(ctx context.Context, cursor string) (Page[item], error) {
		mu.Lock()
		defer mu.Unlock()
		off, _ := strconv.Atoi(cursor)
		d := *data
		end := off + limit
		if end > len(d) {
			end = len(d)
		}
		var items []item
		if off < len(d) {
			items = append(items, d[off:end]...)
		}
		next := ""
		if end < len(d) {
			next = strconv.Itoa(end)
		}
		return Page[item]{Items: items, NextCursor: next}, nil
	}
}

func mkItems(n int) []item {
	out := make([]item, n)
	for i := range out {
		out[i] = item{ID: "id-" + strconv.Itoa(i)}
	}
	return out
}

// ---- 续传标记：序列化、版本兼容 ----

func TestCheckpointRoundTrip(t *testing.T) {
	cp := Checkpoint{
		Cursor:      "20",
		Seen:        map[string]bool{"a": true, "b": true},
		Visited:     []string{"", "10"},
		PagesDone:   2,
		ItemsDone:   2,
		EmptyStreak: 0,
	}
	raw, err := cp.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("checkpoint json=%s", raw)

	got, err := UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != CheckpointVersion {
		t.Fatalf("version=%d want %d", got.Version, CheckpointVersion)
	}
	if got.Cursor != "20" || got.PagesDone != 2 || !got.Seen["a"] || len(got.Visited) != 2 {
		t.Fatalf("round trip lost fields: %+v", got)
	}
}

func TestCheckpointVersionCompat(t *testing.T) {
	// 未来版本：拒绝静默错续。
	future, _ := (&Checkpoint{Version: CheckpointVersion + 1, Cursor: "1"}).Marshal()
	// 手工构造更高版本，绕过 Marshal 自动盖版本。
	future = []byte(`{"version":999,"cursor":"1"}`)
	if _, err := UnmarshalCheckpoint(future); !errors.Is(err, ErrCheckpointUnsupportedVersion) {
		t.Fatalf("future version: want ErrCheckpointUnsupportedVersion, got %v", err)
	}
	// 旧版本/缺字段：宽容接受，Seen 自动初始化。
	old := []byte(`{"cursor":"10","pages_done":1}`)
	got, err := UnmarshalCheckpoint(old)
	if err != nil {
		t.Fatalf("legacy checkpoint must load: %v", err)
	}
	if got.Version != 0 || got.Cursor != "10" || got.Seen == nil {
		t.Fatalf("legacy load mismatch: %+v", got)
	}
	t.Logf("判定依据: version>当前 -> 拒绝；缺 version -> 宽容恢复")
}

// ---- 断点续传：跨进程（经 JSON）恢复，已处理内容不重复 ----

func TestWalkerResumeAcrossProcessesNoRepeats(t *testing.T) {
	data := mkItems(7)
	cfg := WalkerConfig[item]{
		Key:    func(i item) string { return i.ID },
		Limits: Limits{MaxPages: 2},
		Logger: testLogger(t),
	}

	// 第一次“进程”：只给 2 页预算，拿到 4 条后到页数上限，导出标记。
	w1 := NewWalker(dataFetch(&data, 2), cfg)
	r1 := w1.Run(context.Background())
	if r1.Reason != EndLimitPages {
		t.Fatalf("run1 reason=%s want %s (err=%v)", r1.Reason, EndLimitPages, r1.Err)
	}
	if r1.ItemsCount != 4 || r1.Err != nil {
		t.Fatalf("run1 items=%d err=%v", r1.ItemsCount, r1.Err)
	}
	if r1.Checkpoint.Done {
		t.Fatal("checkpoint must not be Done at limit")
	}
	raw, err := r1.Checkpoint.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("进程1结束 reason=%s 标记=%s （模拟落盘/跨进程传递）", r1.Reason, raw)

	// 第二次“进程”：从 JSON 恢复，默认不限量跑完剩余。
	cp, err := UnmarshalCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	w2 := ResumeWalker(dataFetch(&data, 2), cp, cfg)
	r2 := w2.Run(context.Background())
	if r2.Reason != EndCompleted {
		t.Fatalf("run2 reason=%s want completed (err=%v)", r2.Reason, r2.Err)
	}
	t.Logf("进程2结束 reason=%s 新拿=%d", r2.Reason, r2.ItemsCount)

	// 合并两次结果，每个 key 恰好出现一次，且累计为完整 7 条。
	counts := map[string]int{}
	for _, it := range append(append([]item{}, r1.Items...), r2.Items...) {
		counts[it.ID]++
	}
	if len(counts) != 7 {
		t.Fatalf("unique items=%d want 7", len(counts))
	}
	for id, n := range counts {
		if n != 1 {
			t.Fatalf("item %s appeared %d times after resume", id, n)
		}
	}
	if r2.Checkpoint.ItemsDone != 7 || !r2.Checkpoint.Done {
		t.Fatalf("final checkpoint: items=%d done=%v", r2.Checkpoint.ItemsDone, r2.Checkpoint.Done)
	}
}

// ---- 三类工作量上限：正常收敛、可续传、Err 为 nil ----

func TestWalkerLimitPages(t *testing.T) {
	data := mkItems(10)
	cfg := WalkerConfig[item]{
		Key:    func(i item) string { return i.ID },
		Limits: Limits{MaxPages: 2},
		Logger: testLogger(t),
	}
	r := NewWalker(dataFetch(&data, 3), cfg).Run(context.Background())
	if r.Reason != EndLimitPages || r.Err != nil || r.Pages != 2 || r.ItemsCount != 6 {
		t.Fatalf("pages=%d reason=%s err=%v items=%d", r.Pages, r.Reason, r.Err, r.ItemsCount)
	}
	if r.Checkpoint.Cursor != "6" || r.Checkpoint.Done {
		t.Fatalf("checkpoint cursor=%q done=%v", r.Checkpoint.Cursor, r.Checkpoint.Done)
	}
	t.Logf("判定依据: 到页数上限=%d -> reason=%s（正常收敛，可续传 cursor=%s）",
		cfg.Limits.MaxPages, r.Reason, r.Checkpoint.Cursor)
}

func TestWalkerLimitItems(t *testing.T) {
	data := mkItems(10)
	cfg := WalkerConfig[item]{
		Key: func(i item) string { return i.ID },
		// MaxItems 以页为粒度：第二页 3 条入账后 6>=5 即收敛（最多超一页的量）。
		Limits: Limits{MaxItems: 5},
		Logger: testLogger(t),
	}
	r := NewWalker(dataFetch(&data, 3), cfg).Run(context.Background())
	if r.Reason != EndLimitItems || r.Err != nil || r.ItemsCount != 6 {
		t.Fatalf("reason=%s err=%v items=%d", r.Reason, r.Err, r.ItemsCount)
	}
	t.Logf("判定依据: 到条数上限 -> reason=%s items=%d cursor=%s",
		r.Reason, r.ItemsCount, r.Checkpoint.Cursor)

	// 续传后不应再产出这 5 条。
	r2 := ResumeWalker(dataFetch(&data, 3), r.Checkpoint, cfg).Run(context.Background())
	for _, it := range r2.Items {
		for _, seen := range r.Items {
			if it.ID == seen.ID {
				t.Fatalf("resume yielded already processed item %s", it.ID)
			}
		}
	}
}

// fakeClock 每次 Now 推进固定步长，用于确定性地触发时间上限，避免真实睡眠。
type fakeClock struct {
	t time.Time
	d time.Duration
}

func (c *fakeClock) Now() time.Time { c.t = c.t.Add(c.d); return c.t }

func TestWalkerLimitTime(t *testing.T) {
	data := mkItems(100)
	clk := &fakeClock{t: time.Unix(1_000_000, 0), d: 60 * time.Millisecond}
	cfg := WalkerConfig[item]{
		Key:    func(i item) string { return i.ID },
		Limits: Limits{MaxDuration: 100 * time.Millisecond, MaxPages: 1000},
		Logger: testLogger(t),
		Now:    clk.Now,
	}
	r := NewWalker(dataFetch(&data, 2), cfg).Run(context.Background())
	if r.Reason != EndLimitTime || r.Err != nil {
		t.Fatalf("reason=%s err=%v want limit_time", r.Reason, r.Err)
	}
	if r.Pages < 1 || r.Checkpoint.Done {
		t.Fatalf("pages=%d done=%v", r.Pages, r.Checkpoint.Done)
	}
	t.Logf("判定依据: elapsed>=MaxDuration -> reason=%s（正常收敛，已拉 %d 页，cursor=%s）",
		r.Reason, r.Pages, r.Checkpoint.Cursor)
}

// ---- 上限 vs 取消 vs 超时 vs 配额耗尽：四者严格区分 ----

func TestWalkerEndReasonsDistinct(t *testing.T) {
	key := func(i item) string { return i.ID }

	// 1. 调用方主动取消。
	cancelFetch := func(ctx context.Context, cursor string) (Page[item], error) {
		<-ctx.Done()
		return Page[item]{}, classifyCtxErr("fetch", ctx.Err())
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	r := NewWalker(cancelFetch, WalkerConfig[item]{Key: key, Logger: testLogger(t)}).Run(ctx)
	if r.Reason != EndCanceled || KindOf(r.Err) != KindCanceled {
		t.Fatalf("cancel: reason=%s kind=%s", r.Reason, KindOf(r.Err))
	}
	t.Logf("主动取消 -> reason=%s kind=%s", r.Reason, KindOf(r.Err))

	// 2. 请求超时（调用方 deadline 在 fetch 期间到期）。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	r = NewWalker(cancelFetch, WalkerConfig[item]{Key: key, Logger: testLogger(t)}).Run(ctx2)
	if r.Reason != EndTimeout || KindOf(r.Err) != KindTimeout {
		t.Fatalf("timeout: reason=%s kind=%s", r.Reason, KindOf(r.Err))
	}
	t.Logf("请求超时 -> reason=%s kind=%s", r.Reason, KindOf(r.Err))

	// 3. 限流迟迟等不到配额。
	quotaFetch := func(ctx context.Context, cursor string) (Page[item], error) {
		return Page[item]{}, &Error{Kind: KindQuotaExhausted, Op: "fetch",
			Err: errors.New("quota not available")}
	}
	r = NewWalker(quotaFetch, WalkerConfig[item]{Key: key, Logger: testLogger(t)}).Run(context.Background())
	if r.Reason != EndQuotaExhausted || KindOf(r.Err) != KindQuotaExhausted {
		t.Fatalf("quota: reason=%s kind=%s", r.Reason, KindOf(r.Err))
	}
	t.Logf("配额耗尽 -> reason=%s kind=%s", r.Reason, KindOf(r.Err))

	// 4. 上限收敛的 Err 必须是 nil（与上面三种有本质区别）。
	data := mkItems(4)
	r = NewWalker(dataFetch(&data, 2), WalkerConfig[item]{
		Key: key, Limits: Limits{MaxPages: 1}, Logger: testLogger(t),
	}).Run(context.Background())
	if r.Reason != EndLimitPages || r.Err != nil {
		t.Fatalf("limit: reason=%s err=%v", r.Reason, r.Err)
	}
	t.Logf("到上限 -> reason=%s err=<nil>（与取消/超时/配额/错误互不混淆）", r.Reason)
}

// ---- 异常游标：停滞、绕回、连续空页，都必须收敛停止 ----

func TestWalkerCursorStuck(t *testing.T) {
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		return Page[item]{Items: []item{{ID: "x"}}, NextCursor: "same"}, nil
	}
	r := NewWalker(fetch, WalkerConfig[item]{Key: func(i item) string { return i.ID }, Logger: testLogger(t)}).
		Run(context.Background())
	if r.Reason != EndCursorAnomaly || !errors.Is(r.Err, ErrPaginationStuck) {
		t.Fatalf("reason=%s err=%v", r.Reason, r.Err)
	}
}

func TestWalkerCursorLoop(t *testing.T) {
	// 序列："" -> a -> b -> a（绕回已访问游标）。
	var calls int
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		calls++
		switch cursor {
		case "":
			return Page[item]{Items: []item{{ID: "1"}}, NextCursor: "a"}, nil
		case "a":
			return Page[item]{Items: []item{{ID: "2"}}, NextCursor: "b"}, nil
		default: // b
			return Page[item]{Items: []item{{ID: "3"}}, NextCursor: "a"}, nil
		}
	}
	r := NewWalker(fetch, WalkerConfig[item]{Key: func(i item) string { return i.ID }, Logger: testLogger(t)}).
		Run(context.Background())
	if r.Reason != EndCursorAnomaly || !errors.Is(r.Err, ErrCursorLoop) {
		t.Fatalf("reason=%s err=%v calls=%d", r.Reason, r.Err, calls)
	}
	if calls != 3 {
		t.Fatalf("looping cursor must stop after 3 fetches, got %d", calls)
	}
	t.Logf("判定依据: next_cursor=a 命中历史游标 -> %v（第 %d 次请求即收敛停止）", r.Err, calls)
}

func TestWalkerCursorLoopDetectedAfterResume(t *testing.T) {
	// 第一次跑到游标 b 落盘；恢复后服务端异常把 b 的下一页指回 a（跨进程绕回）。
	cp := Checkpoint{
		Version: CheckpointVersion, Cursor: "b",
		Seen:    map[string]bool{"1": true, "2": true},
		Visited: []string{"", "a"},
	}
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		if cursor != "b" {
			t.Fatalf("resume cursor=%q want b", cursor)
		}
		return Page[item]{Items: []item{{ID: "3"}}, NextCursor: "a"}, nil
	}
	r := ResumeWalker(fetch, cp, WalkerConfig[item]{Key: func(i item) string { return i.ID }, Logger: testLogger(t)}).
		Run(context.Background())
	if r.Reason != EndCursorAnomaly || !errors.Is(r.Err, ErrCursorLoop) {
		t.Fatalf("reason=%s err=%v", r.Reason, r.Err)
	}
	t.Logf("判定依据: 续传后绕回历史游标 a（记录在标记内）-> 跨进程同样收敛")
}

func TestWalkerNoProgressEmptyPages(t *testing.T) {
	// 游标不断前进，但每页都是空集合：连续空页达到阈值必须停止。
	var calls int
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		calls++
		return Page[item]{Items: nil, NextCursor: strconv.Itoa(calls * 10)}, nil
	}
	r := NewWalker(fetch, WalkerConfig[item]{
		Key:    func(i item) string { return i.ID },
		Limits: Limits{MaxEmptyPages: 3},
		Logger: testLogger(t),
	}).Run(context.Background())
	if r.Reason != EndCursorAnomaly || !errors.Is(r.Err, ErrPaginationNoProgress) || calls != 3 {
		t.Fatalf("reason=%s err=%v calls=%d", r.Reason, r.Err, calls)
	}
	t.Logf("判定依据: 连续空页=3 -> %v", r.Err)
}

// ---- 续传期间数据增删：已处理内容不重复（偏移模型允许跳过新增） ----

func TestWalkerResumeDedupUnderMutation(t *testing.T) {
	data := []item{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}
	cfg := WalkerConfig[item]{
		Key:    func(i item) string { return i.ID },
		Limits: Limits{MaxPages: 1},
		Logger: testLogger(t),
	}
	fetch := dataFetch(&data, 2)
	r1 := NewWalker(fetch, cfg).Run(context.Background())
	if r1.Reason != EndLimitPages || len(r1.Items) != 2 {
		t.Fatalf("run1 reason=%s items=%d", r1.Reason, len(r1.Items))
	}

	// 进程间数据变动：头部插入 x，尾部追加 e（偏移漂移会让 b 再次出现在页内）。
	data = append([]item{{ID: "x"}}, data...)
	data = append(data, item{ID: "e"})
	t.Logf("mutation 后数据集=%v（b 会漂移到续传首页，应被 Seen 过滤）", data)

	cfg.Limits = Limits{} // 第二次跑完
	r2 := ResumeWalker(fetch, r1.Checkpoint, cfg).Run(context.Background())
	if r2.Reason != EndCompleted {
		t.Fatalf("run2 reason=%s err=%v", r2.Reason, r2.Err)
	}
	seen := map[string]int{}
	for _, it := range append(append([]item{}, r1.Items...), r2.Items...) {
		seen[it.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("item %s appeared %d times", id, n)
		}
	}
	if seen["b"] != 1 {
		t.Fatal("already-processed item b must not reappear after resume")
	}
	t.Logf("续传后产出=%v，历史元素零重复（注：头部新增 x 可能被偏移模型跳过）", r2.Items)
}

// ---- 完成态幂等：Done 后再 Run 立即返回 completed、零请求 ----

func TestWalkerCompletedIsTerminal(t *testing.T) {
	data := mkItems(3)
	var calls int
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		calls++
		return dataFetch(&data, 10)(ctx, cursor)
	}
	w := NewWalker(fetch, WalkerConfig[item]{Key: func(i item) string { return i.ID }, Logger: testLogger(t)})
	r1 := w.Run(context.Background())
	if r1.Reason != EndCompleted {
		t.Fatalf("r1=%s", r1.Reason)
	}
	r2 := w.Run(context.Background())
	if r2.Reason != EndCompleted || r2.Pages != 0 || len(r2.Items) != 0 {
		t.Fatalf("rerun: reason=%s pages=%d items=%d", r2.Reason, r2.Pages, len(r2.Items))
	}
	if calls != 1 {
		t.Fatalf("completed walker must issue no more requests, calls=%d", calls)
	}
}

// ---- 并发隔离：多个 Walker 各跑各的标记与上限，互不串数据 ----

func TestWalkerConcurrentIsolation(t *testing.T) {
	const tasks = 4
	makeData := func(task int) []item {
		out := make([]item, 8)
		for i := range out {
			out[i] = item{ID: "task" + strconv.Itoa(task) + "-" + strconv.Itoa(i)}
		}
		return out
	}

	var wg sync.WaitGroup
	errs := make([]error, tasks)
	reasons := make([]EndReason, tasks)
	counts := make([]int, tasks)
	for j := 0; j < tasks; j++ {
		wg.Add(1)
		go func(task int) {
			defer wg.Done()
			data := makeData(task)
			// 奇数任务给 1 页预算（到上限），偶数任务直接跑完——结束原因不应互相污染。
			cfg := WalkerConfig[item]{
				Key:    func(i item) string { return i.ID },
				Logger: testLogger(t),
			}
			if task%2 == 1 {
				cfg.Limits = Limits{MaxPages: 1}
			}
			r := NewWalker(dataFetch(&data, 3), cfg).Run(context.Background())
			reasons[task] = r.Reason
			counts[task] = r.ItemsCount
			if r.Reason == EndLimitPages {
				// 续传跑完剩余：使用新的（无上限）预算。
				resumeCfg := cfg
				resumeCfg.Limits = Limits{}
				r2 := ResumeWalker(dataFetch(&data, 3), r.Checkpoint, resumeCfg).Run(context.Background())
				if r2.Reason != EndCompleted {
					errs[task] = errors.New("resume not completed: " + string(r2.Reason))
					return
				}
				counts[task] += r2.ItemsCount
			}
		}(j)
	}
	wg.Wait()
	for j := 0; j < tasks; j++ {
		if errs[j] != nil {
			t.Fatalf("task %d: %v", j, errs[j])
		}
		if counts[j] != 8 {
			t.Fatalf("task %d collected %d want 8", j, counts[j])
		}
		want := EndCompleted
		if j%2 == 1 {
			want = EndLimitPages
		}
		if reasons[j] != want {
			t.Fatalf("task %d reason=%s want %s", j, reasons[j], want)
		}
		// 任务间数据不得串：每个任务的 key 都带自己的前缀（由计数 8 间接保证）。
	}
	t.Logf("并发任务=%d 各自结束原因=%v 各自累计=%d（标记/上限相互隔离）", tasks, reasons, counts)
}
