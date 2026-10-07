package sdk

import (
	"context"
	"errors"
	"time"
)

// defaultMaxEmptyPages 是连续空页（没有任何新元素）的默认收敛阈值。
const defaultMaxEmptyPages = 10

// Limits 是单次遍历运行的工作量上限；零值表示不限该项。
// 上限是每次 Run 的工作量预算，到达上限属于“正常收敛”（不是错误），
// 调用方可凭返回的 Checkpoint 在之后的新进程接着跑。
type Limits struct {
	// MaxDuration 本次运行的墙钟时间上限（按每次 Run 独立计时）。
	MaxDuration time.Duration
	// MaxPages 本次运行最多拉取的页数。
	MaxPages int
	// MaxItems 本次运行最多产出的去重后元素数（硬上限：页内逐条判定，
	// 到上限立即截断，单次 Run 产出绝不会超过该值；本页剩余元素留给续传）。
	MaxItems int
	// MaxEmptyPages 连续多少页没有新元素即判定服务端序列异常并收敛停止；
	// <=0 时取默认值 defaultMaxEmptyPages。
	MaxEmptyPages int
}

// EndReason 是遍历结束的归类原因。上限收敛与各类异常/取消互斥、可明确区分。
type EndReason string

const (
	// EndCompleted 服务端数据已全部遍历完（next_cursor 为空），终态。
	EndCompleted EndReason = "completed"
	// EndLimitTime 到达 MaxDuration（正常收敛，可续传）。
	EndLimitTime EndReason = "limit_time"
	// EndLimitPages 到达 MaxPages（正常收敛，可续传）。
	EndLimitPages EndReason = "limit_pages"
	// EndLimitItems 到达 MaxItems（正常收敛，可续传）。
	EndLimitItems EndReason = "limit_items"
	// EndCanceled 调用方主动取消 context。
	EndCanceled EndReason = "canceled"
	// EndTimeout 调用方设置的 context 超时到期。
	EndTimeout EndReason = "timeout"
	// EndQuotaExhausted 限流配额在调用方时限内迟迟等不到。
	EndQuotaExhausted EndReason = "quota_exhausted"
	// EndFetchError 拉取一页失败（传输/状态/解码等，详见 Err）。
	EndFetchError EndReason = "fetch_error"
	// EndCursorAnomaly 服务端游标序列异常（停滞/绕回）或连续空页，已收敛停止。
	EndCursorAnomaly EndReason = "cursor_anomaly"
)

// ErrCursorLoop 表示服务端游标回到了已访问过的游标（重复/绕回）。
var ErrCursorLoop = errors.New("sdk: pagination cursor looped back to a previously visited cursor")

// ErrPaginationNoProgress 表示连续多页没有任何新元素，遍历无法继续推进。
var ErrPaginationNoProgress = errors.New("sdk: pagination made no progress for too many consecutive pages")

// WalkResult 是一次 Run 的结果：产出元素、结束归类与可续传标记。
//
// 结束判定优先级：终态完成 > 调用方取消/超时 > 工作量上限 > 拉取错误/游标异常。
// 上限收敛（EndLimit*）时 Err 为 nil，与取消/超时/配额耗尽/错误互不混淆。
type WalkResult[T any] struct {
	// Items 是本次运行新产出的去重元素（跨运行的历史元素不会重复出现）。
	Items []T
	// Reason 是结束归类。
	Reason EndReason
	// Checkpoint 是结束时刻的续传标记，可立即 Marshal 落盘。
	Checkpoint Checkpoint
	// Pages 是本次运行实际拉取的页数。
	Pages int
	// ItemsCount == len(Items)，便于直接读取。
	ItemsCount int
	// Err 是 Reason 对应的错误；完成与上限收敛时为 nil。
	Err error
}

// WalkerConfig 配置遍历器。
type WalkerConfig[T any] struct {
	// Key 返回元素的稳定去重键（如元素 ID），必填。
	Key func(T) string
	// Limits 是每次 Run 的工作量上限。
	Limits Limits
	// Logger 可选：记录每次请求参数（游标）、每页判定依据与最终结束归类。
	Logger func(format string, args ...any)
	// Now 可选，注入时钟（默认 time.Now），主要用于测试。
	Now func() time.Time
}

// Walker 是带工作量上限与异常收敛的分页遍历器。
//
// 每个同步任务应独立持有一个 Walker：续传标记、已见集合、页数/条数预算
// 都在实例内，互不串数据；Walker 本身不做内部加锁，也不需要——不同任务
// 各自一个实例，底层 Client / 限流器 / 凭证提供者才是共享且并发安全的。
type Walker[T any] struct {
	fetch   FetchPage[T]
	cfg     WalkerConfig[T]
	cp      Checkpoint
	visited map[string]struct{} // 与 cp.Visited 同步，供 O(1) 绕回检测
	now     func() time.Time
}

// NewWalker 创建从头开始的遍历器。
func NewWalker[T any](fetch FetchPage[T], cfg WalkerConfig[T]) *Walker[T] {
	return ResumeWalker(fetch, Checkpoint{Version: CheckpointVersion}, cfg)
}

// ResumeWalker 从续传标记恢复遍历器。标记缺失的内部集合会被补全，
// 旧版本标记（无 version）也能宽容恢复。
func ResumeWalker[T any](fetch FetchPage[T], cp Checkpoint, cfg WalkerConfig[T]) *Walker[T] {
	if cp.Version == 0 {
		cp.Version = CheckpointVersion
	}
	if cp.Seen == nil {
		cp.Seen = map[string]bool{}
	}
	w := &Walker[T]{
		fetch:   fetch,
		cfg:     cfg,
		cp:      cp,
		visited: make(map[string]struct{}, len(cp.Visited)+1),
		now:     cfg.Now,
	}
	if w.now == nil {
		w.now = time.Now
	}
	for _, c := range cp.Visited {
		w.visited[c] = struct{}{}
	}
	// 兼容缺少 visited 记录的旧标记：至少把即将拉取的游标记为已访问，
	// 防止恢复后立刻绕回起点而不被发现。
	if cp.Cursor != "" {
		if _, ok := w.visited[cp.Cursor]; !ok && len(cp.Visited) == 0 {
			w.visited[cp.Cursor] = struct{}{}
			cp.Visited = append(cp.Visited, cp.Cursor)
		}
	}
	return w
}

// Run 在 cfg.Limits 的工作量预算内运行遍历，直到：服务端完成、到达上限、
// 调用方 context 结束、拉取报错或游标序列异常收敛。
// 同一个 Walker 可以多次调用 Run（每次用新的预算接着上次标记跑），
// 但不得并发调用；多任务请各建一个 Walker。
func (w *Walker[T]) Run(ctx context.Context) WalkResult[T] {
	limits := w.cfg.Limits
	if limits.MaxEmptyPages <= 0 {
		limits.MaxEmptyPages = defaultMaxEmptyPages
	}
	if w.cfg.Key == nil {
		return w.finish(EndFetchError, errors.New("sdk: WalkerConfig.Key is required"), nil, 0)
	}

	start := w.now()
	runPages := 0
	var out []T

	logf := func(format string, args ...any) {
		if w.cfg.Logger != nil {
			w.cfg.Logger(format, args...)
		}
	}

	for {
		// 1. 服务端已到终态。
		if w.cp.Done {
			logf("walk 判定=完成 依据=checkpoint.Done (pages_run=%d items_run=%d)", runPages, len(out))
			return w.finish(EndCompleted, nil, out, runPages)
		}
		// 2. 调用方 context 结束：取消与超时优先且严格区分。
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				logf("walk 判定=超时 依据=context deadline exceeded (pages_run=%d)", runPages)
				return w.finish(EndTimeout, classifyCtxErr("Walker.Run", err), out, runPages)
			}
			logf("walk 判定=取消 依据=context canceled (pages_run=%d)", runPages)
			return w.finish(EndCanceled, classifyCtxErr("Walker.Run", err), out, runPages)
		}
		// 3. 工作量上限：到达即正常收敛（在发起下一页请求之前判定，不放大请求）。
		if limits.MaxDuration > 0 && w.now().Sub(start) >= limits.MaxDuration {
			logf("walk 判定=到时间上限 elapsed=%s budget=%s next_cursor=%q",
				w.now().Sub(start), limits.MaxDuration, w.cp.Cursor)
			return w.finish(EndLimitTime, nil, out, runPages)
		}
		if limits.MaxPages > 0 && runPages >= limits.MaxPages {
			logf("walk 判定=到页数上限 pages=%d next_cursor=%q", runPages, w.cp.Cursor)
			return w.finish(EndLimitPages, nil, out, runPages)
		}
		if limits.MaxItems > 0 && len(out) >= limits.MaxItems {
			logf("walk 判定=到条数上限 items=%d next_cursor=%q", len(out), w.cp.Cursor)
			return w.finish(EndLimitItems, nil, out, runPages)
		}

		// 4. 拉取一页。
		logf("walk request cursor=%q (pages_run=%d items_run=%d elapsed=%s)",
			w.cp.Cursor, runPages, len(out), w.now().Sub(start))
		page, err := w.fetch(ctx, w.cp.Cursor)
		if err != nil {
			reason := EndFetchError
			switch KindOf(err) {
			case KindTimeout:
				reason = EndTimeout
			case KindCanceled:
				reason = EndCanceled
			case KindQuotaExhausted:
				reason = EndQuotaExhausted
			}
			logf("walk 判定=%s 依据=fetch 返回错误 cursor=%q err=%v", reason, w.cp.Cursor, err)
			return w.finish(reason, err, out, runPages)
		}
		runPages++
		w.cp.PagesDone++

		// 5. 按 key 去重并入账：历史 Seen 跨进程保留，已处理内容不会重复产出。
		//    MaxItems 是硬上限：页内逐条判定，到上限立即截断——本页剩余元素
		//    不进 Seen、游标不推进，续传时重拉本页即可补齐，不重不漏。
		added := 0
		truncated := false
		for _, it := range page.Items {
			k := w.cfg.Key(it)
			if w.cp.Seen[k] {
				continue // 续传前已处理，或数据变动导致跨页漂移，跳过
			}
			if limits.MaxItems > 0 && len(out) >= limits.MaxItems {
				truncated = true // 硬上限：本页剩余元素留给下一批
				break
			}
			w.cp.Seen[k] = true
			out = append(out, it)
			added++
			w.cp.ItemsDone++
		}
		if added > 0 {
			w.cp.EmptyStreak = 0
		} else {
			w.cp.EmptyStreak++
		}
		logf("walk response cursor=%q next=%q items=%d new=%d empty_streak=%d",
			w.cp.Cursor, page.NextCursor, len(page.Items), added, w.cp.EmptyStreak)

		// 6. 硬上限截断：到上限即正常收敛。游标仍指向本页（不推进），
		//    已产出元素在 Seen 中，续传重拉本页时跳过已拿部分、补齐剩余；
		//    即使本页 next_cursor 为空，页内仍有未取元素，必须归类为到上限
		//    而非“全部拿完”。
		if truncated {
			logf("walk 判定=到条数上限 依据=硬上限截断 items=%d 上限=%d 游标不推进 cursor=%q（本页剩余留给续传）",
				len(out), limits.MaxItems, w.cp.Cursor)
			return w.finish(EndLimitItems, nil, out, runPages)
		}

		// 7. 序列判定：完成 / 停滞 / 绕回 / 持续空转——保证任何异常游标都能收敛停止。
		if page.NextCursor == "" {
			w.cp.Done = true
			logf("walk 判定=完成 依据=next_cursor 为空 (pages_run=%d items_run=%d)", runPages, len(out))
			return w.finish(EndCompleted, nil, out, runPages)
		}
		if page.NextCursor == w.cp.Cursor {
			err := ErrPaginationStuck
			logf("walk 判定=游标停滞 依据=next_cursor==cursor==%q -> %v", w.cp.Cursor, err)
			return w.finish(EndCursorAnomaly, err, out, runPages)
		}
		if _, loop := w.visited[page.NextCursor]; loop {
			err := ErrCursorLoop
			logf("walk 判定=游标绕回 依据=next_cursor=%q 已在历史游标中 -> %v", page.NextCursor, err)
			return w.finish(EndCursorAnomaly, err, out, runPages)
		}
		if w.cp.EmptyStreak >= limits.MaxEmptyPages {
			err := ErrPaginationNoProgress
			logf("walk 判定=连续空页 依据=empty_streak=%d>=%d -> %v",
				w.cp.EmptyStreak, limits.MaxEmptyPages, err)
			return w.finish(EndCursorAnomaly, err, out, runPages)
		}

		// 8. 本页完整入账后正好补满条数上限：正常收敛，游标已推进可续传。
		if limits.MaxItems > 0 && len(out) >= limits.MaxItems {
			logf("walk 判定=到条数上限 items=%d next_cursor=%q", len(out), page.NextCursor)
			w.advance(page.NextCursor)
			return w.finish(EndLimitItems, nil, out, runPages)
		}

		// 9. 推进游标（页数/时间上限在下一轮顶部判定，且不会多发请求）。
		w.advance(page.NextCursor)
	}
}

// advance 记录“当前游标已访问”并把游标推进到下一页。
func (w *Walker[T]) advance(next string) {
	w.visited[w.cp.Cursor] = struct{}{}
	w.cp.Visited = append(w.cp.Visited, w.cp.Cursor)
	w.cp.Cursor = next
}

// finish 组装结果；返回的是 Checkpoint 的独立副本，调用方可安全落盘，
// 与 Walker 之后继续运行时的内部状态不共享 map。
func (w *Walker[T]) finish(reason EndReason, err error, items []T, pages int) WalkResult[T] {
	return WalkResult[T]{
		Items:      items,
		Reason:     reason,
		Checkpoint: w.snapshot(),
		Pages:      pages,
		ItemsCount: len(items),
		Err:        err,
	}
}

// snapshot 深拷贝续传标记。
func (w *Walker[T]) snapshot() Checkpoint {
	cp := w.cp
	seen := make(map[string]bool, len(w.cp.Seen))
	for k, v := range w.cp.Seen {
		seen[k] = v
	}
	cp.Seen = seen
	if w.cp.Visited != nil {
		cp.Visited = append([]string(nil), w.cp.Visited...)
	}
	return cp
}

// Checkpoint 返回当前续传标记的独立副本（任意时刻可导出、Marshal 落盘）。
func (w *Walker[T]) Checkpoint() Checkpoint {
	return w.snapshot()
}
