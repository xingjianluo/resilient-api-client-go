package sdk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
		// MaxItems 是硬上限：页大小为 3，上限 5 落在第二页中间，
		// 只产出 5 条（第一页 3 条 + 第二页前 2 条），第二页剩余 1 条留给下一批。
		Limits: Limits{MaxItems: 5},
		Logger: testLogger(t),
	}
	fetch := dataFetch(&data, 3)
	r := NewWalker(fetch, cfg).Run(context.Background())
	if r.Reason != EndLimitItems || r.Err != nil || r.ItemsCount != 5 {
		t.Fatalf("reason=%s err=%v items=%d want 5 (hard cap)", r.Reason, r.Err, r.ItemsCount)
	}
	// 页内截断：游标不推进，仍停在第二页起点 "3"，供下一批重拉。
	if r.Checkpoint.Cursor != "3" || r.Checkpoint.Done {
		t.Fatalf("checkpoint cursor=%q done=%v want cursor=3", r.Checkpoint.Cursor, r.Checkpoint.Done)
	}
	if r.Checkpoint.ItemsDone != 5 {
		t.Fatalf("items_done=%d want 5", r.Checkpoint.ItemsDone)
	}
	t.Logf("判定依据: 到条数硬上限 -> reason=%s items=%d 游标保持=%s（页内剩余条目留给下一批）",
		r.Reason, r.ItemsCount, r.Checkpoint.Cursor)

	// 续传：新预算重拉第二页，已产出的 5 条由 Seen 过滤，不会重复。
	r2 := ResumeWalker(fetch, r.Checkpoint, cfg).Run(context.Background())
	if r2.Reason != EndLimitItems || r2.ItemsCount != 5 {
		t.Fatalf("run2 reason=%s items=%d want limit_items/5", r2.Reason, r2.ItemsCount)
	}
	for _, it := range r2.Items {
		for _, seen := range r.Items {
			if it.ID == seen.ID {
				t.Fatalf("resume yielded already processed item %s", it.ID)
			}
		}
	}
	// 再续传一次跑完：没有剩余，应判 completed 而非 limit_items。
	cfg3 := cfg
	cfg3.Limits = Limits{}
	r3 := ResumeWalker(fetch, r2.Checkpoint, cfg3).Run(context.Background())
	if r3.Reason != EndCompleted || r3.ItemsCount != 0 {
		t.Fatalf("run3 reason=%s items=%d want completed/0", r3.Reason, r3.ItemsCount)
	}
	t.Logf("续传补齐: 批次2新拿=%d reason=%s；批次3新拿=%d reason=%s；累计去重=%d 零重复",
		r2.ItemsCount, r2.Reason, r3.ItemsCount, r3.Reason, r3.Checkpoint.ItemsDone)
}

// ---- MaxItems 硬上限：页大小、页中截断、终态边界、跨进程续传补齐 ----

// 上限小于单页大小：服务端一页返回 10 条、上限只要 3 条时，
// 实际产出必须恰好 3 条（旧实现会给足整页 10 条），且只发 1 次请求、游标不动。
func TestWalkerLimitItemsHardCapSmallerThanPage(t *testing.T) {
	data := mkItems(25)
	cfg := WalkerConfig[item]{
		Key:    func(i item) string { return i.ID },
		Limits: Limits{MaxItems: 3},
		Logger: testLogger(t),
	}
	var calls int
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		calls++
		return dataFetch(&data, 10)(ctx, cursor)
	}
	r := NewWalker(fetch, cfg).Run(context.Background())
	if r.Reason != EndLimitItems {
		t.Fatalf("reason=%s want limit_items (err=%v)", r.Reason, r.Err)
	}
	if r.ItemsCount != 3 {
		t.Fatalf("items=%d want 3：硬上限不得被整页放大", r.ItemsCount)
	}
	if calls != 1 || r.Pages != 1 {
		t.Fatalf("requests=%d pages=%d want 1/1：到上限不得多发请求", calls, r.Pages)
	}
	if r.Checkpoint.Cursor != "" || r.Checkpoint.Done {
		t.Fatalf("cursor=%q done=%v：页内截断时游标必须停在本页起点 %q",
			r.Checkpoint.Cursor, r.Checkpoint.Done, "")
	}
	wantIDs := []string{"id-0", "id-1", "id-2"}
	for i, it := range r.Items {
		if it.ID != wantIDs[i] {
			t.Fatalf("item[%d]=%s want %s", i, it.ID, wantIDs[i])
		}
	}
	t.Logf("判定依据: 单页返回10条 > 上限3 -> 页内截断，实际产出=%d 请求数=%d 游标保持%q",
		r.ItemsCount, calls, r.Checkpoint.Cursor)
}

// 上限正好落在最后一页（含“上限==总条数”）：只要预算先补满就必须报 limit_items，
// 不能因为 next_cursor 为空就报成 completed；末页中间截断时续传要补齐剩余条目。
func TestWalkerLimitItemsLandsOnLastPage(t *testing.T) {
	cases := []struct {
		name     string
		total    int
		pageSize int
		maxItems int
	}{
		{"上限落在最后一页中间", 10, 6, 8}, // 页：6 + 4，上限 8 落在末页中间，末页剩 2 条
		{"上限恰等于总条数", 9, 3, 9},    // 最后一页 3 条恰好补满 9（整页边界）
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := mkItems(tc.total)
			cfg := WalkerConfig[item]{
				Key:    func(i item) string { return i.ID },
				Limits: Limits{MaxItems: tc.maxItems},
				Logger: testLogger(t),
			}
			r := NewWalker(dataFetch(&data, tc.pageSize), cfg).Run(context.Background())
			if r.Reason != EndLimitItems || r.Err != nil {
				t.Fatalf("reason=%s err=%v want limit_items（上限落在最后一页也不能报 completed）",
					r.Reason, r.Err)
			}
			if r.ItemsCount != tc.maxItems {
				t.Fatalf("items=%d want %d", r.ItemsCount, tc.maxItems)
			}
			t.Logf("判定依据: %s total=%d page=%d max=%d -> reason=%s done=%v cursor=%q",
				tc.name, tc.total, tc.pageSize, tc.maxItems, r.Reason, r.Checkpoint.Done, r.Checkpoint.Cursor)

			// 续传：未消费完的末页要补齐；整页边界补满的情形直接终态。两种都不得重复。
			r2 := ResumeWalker(dataFetch(&data, tc.pageSize), r.Checkpoint, WalkerConfig[item]{
				Key:    cfg.Key,
				Limits: Limits{}, // 新进程不给上限，跑完剩余
				Logger: testLogger(t),
			}).Run(context.Background())
			if r2.Reason != EndCompleted {
				t.Fatalf("resume reason=%s want completed", r2.Reason)
			}
			if r.ItemsCount+r2.ItemsCount != tc.total {
				t.Fatalf("累计=%d+%d want %d：续传补齐后既不遗漏也不重复",
					r.ItemsCount, r2.ItemsCount, tc.total)
			}
			for _, it := range r2.Items {
				for _, seen := range r.Items {
					if it.ID == seen.ID {
						t.Fatalf("resume 重复产出已处理条目 %s", it.ID)
					}
				}
			}
			t.Logf("续传补齐: 批次1=%d reason=%s；批次2新拿=%d reason=%s；累计=%d 零重复",
				r.ItemsCount, r.Reason, r2.ItemsCount, r2.Reason, r.ItemsCount+r2.ItemsCount)
		})
	}
}

// 跨进程续传补齐（经 JSON 落盘/反序列化，模拟新进程）：
// 页大小 10、每批 MaxItems=3，10 条数据要分多批拿完；
// 第一批就停在第一页中间，后续每批重拉当前页、跳过 Seen，直到末页补齐。
// 任何一批产出都不得超过 3，合并后恰好 10 条且零重复。
func TestWalkerLimitItemsHardCapResumeAcrossProcesses(t *testing.T) {
	data := mkItems(10)
	pageFetch := dataFetch(&data, 10) // 服务端每页给 10 条（比每批上限大）
	cfg := WalkerConfig[item]{
		Key:    func(i item) string { return i.ID },
		Limits: Limits{MaxItems: 3},
		Logger: testLogger(t),
	}

	var collected []item
	cp := Checkpoint{Version: CheckpointVersion}
	batch := 0
	for {
		batch++
		// 每个批次都是“新进程”：第一批从头建 Walker，之后从落盘标记恢复，预算每批重新给 3。
		var w *Walker[item]
		if batch == 1 {
			w = NewWalker(pageFetch, cfg)
		} else {
			w = ResumeWalker(pageFetch, cp, cfg)
		}
		r := w.Run(context.Background())
		if r.Err != nil {
			t.Fatalf("batch %d err=%v", batch, r.Err)
		}
		if r.ItemsCount > 3 {
			t.Fatalf("batch %d 产出 %d 条，超过硬上限 3", batch, r.ItemsCount)
		}
		collected = append(collected, r.Items...)
		t.Logf("批次%d reason=%s 本批=%d cursor=%q done=%v",
			batch, r.Reason, r.ItemsCount, r.Checkpoint.Cursor, r.Checkpoint.Done)

		// 模拟落盘 -> 新进程反序列化。
		raw, err := r.Checkpoint.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		cp, err = UnmarshalCheckpoint(raw)
		if err != nil {
			t.Fatal(err)
		}

		if r.Reason == EndCompleted {
			break
		}
		if r.Reason != EndLimitItems {
			t.Fatalf("batch %d reason=%s want limit_items/completed", batch, r.Reason)
		}
		if batch > 10 {
			t.Fatal("超过 10 批仍未补齐，续传没有推进")
		}
	}

	if len(collected) != 10 {
		t.Fatalf("累计产出=%d want 10（剩余条目被遗漏）", len(collected))
	}
	counts := map[string]int{}
	for _, it := range collected {
		counts[it.ID]++
	}
	for i := 0; i < 10; i++ {
		id := "id-" + strconv.Itoa(i)
		if counts[id] != 1 {
			t.Fatalf("%s 出现 %d 次（要求恰好 1 次：不重复不遗漏）", id, counts[id])
		}
	}
	if !cp.Done || cp.ItemsDone != 10 {
		t.Fatalf("终态标记 done=%v items_done=%d want true/10", cp.Done, cp.ItemsDone)
	}
	t.Logf("判定依据: %d 批全部 reason∈{limit_items,completed}，末批 completed；累计去重=%d 零重复",
		batch, len(collected))
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

// 结束原因归类：MaxItems 收敛必须与取消/超时/配额耗尽严格区分。
// 特别地：批次产出恰好到上限的同一时刻，调用方 ctx 也已取消，
// 正常收敛（limit_items）优先——本批已完成、标记可用，不应改报 canceled。
func TestWalkerLimitItemsNotConfusedWithOtherReasons(t *testing.T) {
	key := func(i item) string { return i.ID }

	// 1. 到上限与 ctx 取消同时发生：首屏数据到手后调用方立刻取消，
	//    本批恰好产出到上限 -> limit_items 正常收束（循环顶部不再复查 ctx）。
	data := mkItems(5)
	ctx2, cancel2 := context.WithCancel(context.Background())
	fetchCancelAfterFirst := func(ctx context.Context, cursor string) (Page[item], error) {
		p, err := dataFetch(&data, 10)(ctx, cursor)
		cancel2() // 首屏数据已到手，返回后调用方 ctx 变为 canceled
		return p, err
	}
	r := NewWalker(fetchCancelAfterFirst, WalkerConfig[item]{
		Key: key, Limits: Limits{MaxItems: 5}, Logger: testLogger(t),
	}).Run(ctx2)
	if r.Reason != EndLimitItems || r.Err != nil {
		t.Fatalf("到上限当步: reason=%s err=%v want limit_items/nil", r.Reason, r.Err)
	}
	if r.ItemsCount != 5 {
		t.Fatalf("items=%d want 5", r.ItemsCount)
	}
	t.Logf("到上限当步 ctx 随后取消 -> reason=%s（正常收束优先，不发下一页请求）", r.Reason)

	// 2. 首批之前 ctx 已取消：必须报 canceled，不能因为设了 MaxItems 就报 limit_items。
	data2 := mkItems(100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r = NewWalker(dataFetch(&data2, 10), WalkerConfig[item]{
		Key: key, Limits: Limits{MaxItems: 3}, Logger: testLogger(t),
	}).Run(ctx)
	if r.Reason != EndCanceled {
		t.Fatalf("首批前 ctx 已取消: reason=%s want canceled", r.Reason)
	}
	t.Logf("首批前 ctx 已取消 -> reason=%s（不被上限掩盖）", r.Reason)

	// 3. 配额错误：设了 MaxItems 也不能报成 limit_items。
	quotaFetch := func(ctx context.Context, cursor string) (Page[item], error) {
		return Page[item]{}, &Error{Kind: KindQuotaExhausted, Op: "fetch",
			Err: errors.New("quota not available")}
	}
	r = NewWalker(quotaFetch, WalkerConfig[item]{
		Key: key, Limits: Limits{MaxItems: 3}, Logger: testLogger(t),
	}).Run(context.Background())
	if r.Reason != EndQuotaExhausted || KindOf(r.Err) != KindQuotaExhausted {
		t.Fatalf("quota: reason=%s kind=%s", r.Reason, KindOf(r.Err))
	}
	t.Logf("配额等不到 + 设了 MaxItems -> reason=%s（不被当成到上限）", r.Reason)

	// 4. 没有剩余数据且未到上限：completed，不能因为 MaxItems>0 就报 limit_items。
	data3 := mkItems(2)
	r = NewWalker(dataFetch(&data3, 10), WalkerConfig[item]{
		Key: key, Limits: Limits{MaxItems: 100}, Logger: testLogger(t),
	}).Run(context.Background())
	if r.Reason != EndCompleted || r.ItemsCount != 2 {
		t.Fatalf("数据少于上限: reason=%s items=%d want completed/2", r.Reason, r.ItemsCount)
	}
	t.Logf("数据总量 2 < 上限 100 -> reason=%s（到上限与全部拿完分得清）", r.Reason)
}

// ---- 并发隔离（MaxItems 硬上限版）：多任务共用同一底层抓取函数，
//      各设不同的条数上限（且都小于页大小），产出、标记、结束原因互不串 ----

func TestWalkerConcurrentIsolationDistinctItemCaps(t *testing.T) {
	const tasks = 4
	const total = 9
	const pageSize = 5 // 比所有任务的上限都大，必然触发页内截断

	// 每个任务独立数据集，key 带任务前缀；抓取函数各自带锁，
	// 但同一 dataFetch 闭包被“共享”（模拟共用底层 client/限流器的使用方式）。
	makeData := func(task int) []item {
		out := make([]item, total)
		for i := range out {
			out[i] = item{ID: "task" + strconv.Itoa(task) + "-" + strconv.Itoa(i)}
		}
		return out
	}
	caps := []int{2, 3, 1, 4} // 每个任务不同的硬上限

	var wg sync.WaitGroup
	errs := make([]error, tasks)
	firstReasons := make([]EndReason, tasks)
	totals := make([]int, tasks)
	for j := 0; j < tasks; j++ {
		wg.Add(1)
		go func(task int) {
			defer wg.Done()
			data := makeData(task)
			fetch := dataFetch(&data, pageSize)
			cfg := WalkerConfig[item]{
				Key:    func(i item) string { return i.ID },
				Limits: Limits{MaxItems: caps[task]},
				Logger: testLogger(t),
			}
			r := NewWalker(fetch, cfg).Run(context.Background())
			firstReasons[task] = r.Reason
			if r.Reason != EndLimitItems {
				errs[task] = fmt.Errorf("task %d first reason=%s want limit_items", task, r.Reason)
				return
			}
			if r.ItemsCount != caps[task] {
				errs[task] = fmt.Errorf("task %d first items=%d want hard cap %d",
					task, r.ItemsCount, caps[task])
				return
			}
			got := map[string]bool{}
			for _, it := range r.Items {
				// 串数据检测：每个 key 必须属于本任务前缀。
				if !strings.HasPrefix(it.ID, "task"+strconv.Itoa(task)+"-") {
					errs[task] = fmt.Errorf("task %d 收到别的任务的数据 %s", task, it.ID)
					return
				}
				got[it.ID] = true
			}
			totals[task] += r.ItemsCount

			// 续传批次：反复用自己的标记 + 新预算直到完成。
			cp := r.Checkpoint
			for b := 0; ; b++ {
				r2 := ResumeWalker(fetch, cp, cfg).Run(context.Background())
				if r2.Err != nil {
					errs[task] = fmt.Errorf("task %d resume: %w", task, r2.Err)
					return
				}
				for _, it := range r2.Items {
					if !strings.HasPrefix(it.ID, "task"+strconv.Itoa(task)+"-") {
						errs[task] = fmt.Errorf("task %d 续传收到别的任务的数据 %s", task, it.ID)
						return
					}
					if got[it.ID] {
						errs[task] = fmt.Errorf("task %d 续传重复产出 %s", task, it.ID)
						return
					}
					got[it.ID] = true
				}
				totals[task] += r2.ItemsCount
				cp = r2.Checkpoint
				if r2.Reason == EndCompleted {
					break
				}
				if r2.Reason != EndLimitItems || b > total+1 {
					errs[task] = fmt.Errorf("task %d unexpected resume reason=%s", task, r2.Reason)
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
		if totals[j] != total {
			t.Fatalf("task %d 累计=%d want %d（漏取或串数据）", j, totals[j], total)
		}
		if firstReasons[j] != EndLimitItems {
			t.Fatalf("task %d first reason=%s", j, firstReasons[j])
		}
	}
	t.Logf("并发任务=%d 各自硬上限=%v 首批原因=%v 各自补齐累计=%v（标记/上限互不串）",
		tasks, caps, firstReasons, totals)
}

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
