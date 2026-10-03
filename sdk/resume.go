// Package sdk 的 resume.go 提供可断点续传的分页遍历：
// 可持久化/跨进程传递的续传标记、工作量上限与可区分的结束原因。
package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrCheckpointVersion 表示续传标记的版本不被当前 SDK 识别。
var ErrCheckpointVersion = errors.New("unsupported checkpoint version")

// CheckpointVersion 是续传标记的当前版本号。
const CheckpointVersion = 1

// Checkpoint 是一次分页遍历的可续传标记，可 JSON 序列化后长期保存、跨进程传递。
type Checkpoint struct {
	// Version 标记结构版本，用于跨版本兼容判定。
	Version int `json:"version"`
	// Cursor 下一次要请求的服务端游标（服务端不透明字符串）。
	Cursor string `json:"cursor"`
	// SeenKeys 已处理元素的去重键全集，恢复后据此保证已处理内容不再出现。
	SeenKeys []string `json:"seen_keys"`
	// RecentCursors 最近使用过的游标（有界），用于跨进程检测游标绕回/成环。
	RecentCursors []string `json:"recent_cursors,omitempty"`
	// Pages 累计已拉取页数。
	Pages int `json:"pages"`
	// Items 累计已产出条数。
	Items int `json:"items"`
}

// EndReason 表示一次遍历运行正常收敛的原因。
type EndReason string

const (
	// EndCompleted 服务端 next_cursor 为空，全部数据遍历完成。
	EndCompleted EndReason = "completed"
	// EndMaxDuration 达到本次运行的最长时间上限。
	EndMaxDuration EndReason = "limit_max_duration"
	// EndMaxPages 达到本次运行的最多页数上限。
	EndMaxPages EndReason = "limit_max_pages"
	// EndMaxItems 达到本次运行的最多条数上限。
	EndMaxItems EndReason = "limit_max_items"
)

// WalkLimits 是单次运行的工作量上限，零值表示不限制该维度。
type WalkLimits struct {
	MaxDuration time.Duration
	MaxPages    int
	MaxItems    int
}

// WalkConfig 配置一次可续传遍历。
type WalkConfig[T any] struct {
	// Fetch 按游标拉取一页（通常基于 Client.Do 构造）。
	Fetch FetchPage[T]
	// Key 返回元素的稳定去重键（如元素 ID）。
	Key func(T) string
	// Limits 本次运行的工作量上限。
	Limits WalkLimits
	// MaxStalePages 连续“零新元素”页数达到该值即判定无进展而终止；<=0 用默认值。
	MaxStalePages int
	// Now 可选，注入时钟（主要用于测试 MaxDuration）；nil 用 time.Now。
	Now func() time.Time
	// Logger 可选：记录每次请求参数、去重与结束判定依据。
	Logger func(format string, args ...any)
}

// WalkResult 是一次运行的结果；即使返回 error，结果仍携带当前进度的续传标记。
type WalkResult[T any] struct {
	// Items 本次运行新产出的元素。
	Items []T
	// Reason 正常收敛时的结束原因；异常返回 error 时为空。
	Reason EndReason
	// Checkpoint 运行结束时刻的续传标记。
	Checkpoint Checkpoint
	// PagesFetched 本次运行实际拉取的页数。
	PagesFetched int
	// ItemsEmitted 本次运行实际产出的条数。
	ItemsEmitted int
}

// 游标序列异常哨兵错误，供 errors.Is 判定；对外错误类别为 KindCursorAnomaly。
var (
	// ErrCursorStalled 服务端返回的 next_cursor 与当前游标相同，游标不推进。
	ErrCursorStalled = errors.New("pagination cursor stalled (next cursor equals current)")
	// ErrCursorLoop 游标回到了本次/上次运行中已经使用过的值（重复成环）。
	ErrCursorLoop = errors.New("pagination cursor loop detected (cursor already used)")
	// ErrNoProgress 连续多页没有任何新元素，遍历无法继续收敛。
	ErrNoProgress = errors.New("pagination made no progress for consecutive pages")
)

// recentCursorLimit 是续传标记中保留的历史游标上限：有界即可检测绕回/成环，
// 不随遍历长度无限增长。
const recentCursorLimit = 64

// defaultMaxStalePages 是连续零新元素页数的默认终止阈值。
const defaultMaxStalePages = 3

// Walker 是单次遍历任务的有状态迭代器，不同任务各自持有、互不共享状态。
type Walker[T any] struct {
	fetch         FetchPage[T]
	key           func(T) string
	limits        WalkLimits
	maxStalePages int
	logf          func(format string, args ...any)

	cursor string
	seen   map[string]struct{}
	// recent 保存最近用过的游标（含当前游标），最新追加在末尾。
	recent []string
	// recentSet 与 recent 同步，O(1) 判定游标是否重复。
	recentSet map[string]struct{}

	pages int
	items int

	now func() time.Time // 便于测试注入
}

// NewWalker 从零开始创建遍历任务。
func NewWalker[T any](cfg WalkConfig[T]) *Walker[T] {
	return newWalker(cfg, Checkpoint{})
}

// ResumeWalker 从续传标记恢复遍历任务。
// 恢复后，已处理过的元素（Checkpoint.SeenKeys）不会再次产出；
// 不兼容的标记版本返回 KindCheckpoint 错误。
func ResumeWalker[T any](cfg WalkConfig[T], cp Checkpoint) (*Walker[T], error) {
	if cp.Version != CheckpointVersion {
		return nil, &Error{
			Kind: KindCheckpoint,
			Op:   "ResumeWalker",
			Err:  fmt.Errorf("%w: got=%d want=%d", ErrCheckpointVersion, cp.Version, CheckpointVersion),
		}
	}
	return newWalker(cfg, cp), nil
}

func newWalker[T any](cfg WalkConfig[T], cp Checkpoint) *Walker[T] {
	maxStale := cfg.MaxStalePages
	if maxStale <= 0 {
		maxStale = defaultMaxStalePages
	}
	logf := cfg.Logger
	if logf == nil {
		logf = func(string, ...any) {} // nil 安全：未配置日志时静默
	}
	w := &Walker[T]{
		fetch:         cfg.Fetch,
		key:           cfg.Key,
		limits:        cfg.Limits,
		maxStalePages: maxStale,
		logf:          logf,
		seen:          make(map[string]struct{}),
		recentSet:     make(map[string]struct{}),
		now:           time.Now,
	}
	if cfg.Now != nil {
		w.now = cfg.Now
	}
	for _, k := range cp.SeenKeys {
		w.seen[k] = struct{}{}
	}
	w.cursor = cp.Cursor
	w.recent = append(w.recent, cp.RecentCursors...)
	if len(w.recent) > recentCursorLimit {
		w.recent = w.recent[len(w.recent)-recentCursorLimit:]
	}
	for _, c := range w.recent {
		w.recentSet[c] = struct{}{}
	}
	// 当前游标必须在历史集合中：恢复后若服务端立刻把游标绕回到当前值，
	// 或首帧返回的 next_cursor 与起点相同，都能被检出。
	if _, ok := w.recentSet[w.cursor]; !ok {
		w.pushRecent(w.cursor)
	}
	w.pages = cp.Pages
	w.items = cp.Items
	return w
}

// pushRecent 记录一个已使用的游标，维持有界历史。
func (w *Walker[T]) pushRecent(c string) {
	if _, ok := w.recentSet[c]; ok {
		return
	}
	w.recent = append(w.recent, c)
	w.recentSet[c] = struct{}{}
	if len(w.recent) > recentCursorLimit {
		delete(w.recentSet, w.recent[0])
		w.recent = w.recent[1:]
	}
}

// Marshal 将续传标记序列化为可长期保存的 JSON。
func (cp Checkpoint) Marshal() ([]byte, error) {
	if cp.Version == 0 {
		cp.Version = CheckpointVersion
	}
	return json.Marshal(cp)
}

// UnmarshalCheckpoint 从 JSON 恢复续传标记，并做版本兼容校验：
// 仅接受当前主版本；版本缺失或更高版本返回 ErrCheckpointVersion，
// 由调用方决定重新全量遍历，避免用不兼容的游标继续拉取。
func UnmarshalCheckpoint(data []byte) (Checkpoint, error) {
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, &Error{
			Kind: KindCheckpoint,
			Op:   "UnmarshalCheckpoint",
			Err:  fmt.Errorf("decode checkpoint: %w", err),
		}
	}
	if cp.Version != CheckpointVersion {
		return Checkpoint{}, &Error{
			Kind: KindCheckpoint,
			Op:   "UnmarshalCheckpoint",
			Err:  fmt.Errorf("%w: got=%d want=%d", ErrCheckpointVersion, cp.Version, CheckpointVersion),
		}
	}
	return cp, nil
}

// Checkpoint 返回当前进度的续传标记副本（拷贝独立，调用方可安全长期保存）。
func (w *Walker[T]) Checkpoint() Checkpoint {
	cp := Checkpoint{
		Version:       CheckpointVersion,
		Cursor:        w.cursor,
		SeenKeys:      make([]string, 0, len(w.seen)),
		RecentCursors: append([]string(nil), w.recent...),
		Pages:         w.pages,
		Items:         w.items,
	}
	for k := range w.seen {
		cp.SeenKeys = append(cp.SeenKeys, k)
	}
	sort.Strings(cp.SeenKeys)
	return cp
}

// Run 执行一次遍历：
//   - 正常收敛（遍历完成或达到工作量上限）返回 *WalkResult 和 nil error，
//     结果中携带可直接保存的 Checkpoint 与明确的 EndReason；
//   - 取消/超时/配额耗尽/游标异常等异常情况返回对应类别的 *Error，
//     同时返回携带当前进度 Checkpoint 的 *WalkResult，调用方仍可续传。
//
// 工作量上限仅约束“本次运行”；Checkpoint 中的累计计数仅用于观测，
// 恢复后不会因历史页数/条数而立即触发上限。
func (w *Walker[T]) Run(ctx context.Context) (*WalkResult[T], error) {
	res := &WalkResult[T]{Checkpoint: w.Checkpoint()}
	start := w.now()
	stalePages := 0

	// hitLimit 在发起下一页请求前判定上限，保证达到上限是正常收敛而非错误。
	hitLimit := func() EndReason {
		if w.limits.MaxPages > 0 && res.PagesFetched >= w.limits.MaxPages {
			return EndMaxPages
		}
		if w.limits.MaxItems > 0 && res.ItemsEmitted >= w.limits.MaxItems {
			return EndMaxItems
		}
		if d := w.limits.MaxDuration; d > 0 && w.now().Sub(start) >= d {
			return EndMaxDuration
		}
		return ""
	}

	for {
		if reason := hitLimit(); reason != "" {
			w.finish(res, reason)
			w.logf("walk end reason=%s pages=%d items=%d cursor=%q 判定依据: 达到工作量上限",
				reason, res.PagesFetched, res.ItemsEmitted, w.cursor)
			return res, nil
		}
		if err := ctx.Err(); err != nil {
			w.finish(res, "")
			return res, classifyCtxErr("Walker.Run", err)
		}

		w.logf("walk request page=%d cursor=%q limits(pages=%d items=%d dur=%s) emitted=%d/%d fetched=%d/%d",
			res.PagesFetched+1, w.cursor,
			w.limits.MaxPages, w.limits.MaxItems, w.limits.MaxDuration,
			res.ItemsEmitted, w.limits.MaxItems, res.PagesFetched, w.limits.MaxPages)

		page, err := w.fetch(ctx, w.cursor)
		if err != nil {
			// 请求失败（含重试耗尽、超时、取消、配额耗尽）：错误类别保持底层归类，
			// 不把任何一种结束原因伪装成另一种。
			w.finish(res, "")
			w.logf("walk end pages=%d items=%d 判定依据: fetch 返回错误 %v",
				res.PagesFetched, res.ItemsEmitted, err)
			return res, err
		}
		res.PagesFetched++
		w.pages++

		newInPage := 0
		for _, item := range page.Items {
			k := w.key(item)
			if _, dup := w.seen[k]; dup {
				// 数据增删导致元素跨页漂移，或同页重复：跳过，已处理内容不重复产出。
				w.logf("walk dedup key=%q cursor=%q 判定依据: 已处理过的元素，跳过", k, w.cursor)
				continue
			}
			w.seen[k] = struct{}{}
			w.items++
			newInPage++
			res.ItemsEmitted++
			res.Items = append(res.Items, item)
			// 条数上限在页内达到：本页剩余元素丢弃，但游标保持在本页起点，
			// 下次续传重拉本页，由 seen 集合去重，不丢不重。
			if w.limits.MaxItems > 0 && res.ItemsEmitted >= w.limits.MaxItems {
				w.finish(res, EndMaxItems)
				w.logf("walk end reason=%s pages=%d items=%d cursor=%q 判定依据: 页内达到条数上限，游标保持待续传",
					EndMaxItems, res.PagesFetched, res.ItemsEmitted, w.cursor)
				return res, nil
			}
		}

		if page.NextCursor == "" {
			// 服务端明确没有下一页：正常完成。游标保持在本页起点无意义，
			// 标记为空游标，续传时调用方应视为已完成。
			w.finish(res, EndCompleted)
			w.logf("walk end reason=%s pages=%d items=%d 判定依据: next_cursor 为空",
				EndCompleted, res.PagesFetched, res.ItemsEmitted)
			return res, nil
		}

		advance := true
		if newInPage == 0 {
			stalePages++
		} else {
			stalePages = 0
		}

		switch {
		case page.NextCursor == w.cursor:
			// 停滞：next_cursor 等于当前游标，继续请求只会得到同一页。
			advance = false
			w.finish(res, "")
			err := &Error{Kind: KindCursorAnomaly, Op: "Walker.Run",
				Err: fmt.Errorf("%w: cursor=%q", ErrCursorStalled, w.cursor)}
			w.logf("walk end 判定依据: 游标停滞 next=%q current=%q -> %v",
				page.NextCursor, w.cursor, err)
			return res, err
		default:
			if _, seen := w.recentSet[page.NextCursor]; seen {
				// 成环/绕回：下一个游标在最近历史中出现过（含跨进程恢复的历史）。
				advance = false
				w.finish(res, "")
				err := &Error{Kind: KindCursorAnomaly, Op: "Walker.Run",
					Err: fmt.Errorf("%w: cursor=%q", ErrCursorLoop, page.NextCursor)}
				w.logf("walk end 判定依据: 游标重复/绕回 next=%q 已在最近 %d 个游标中 -> %v",
					page.NextCursor, len(w.recent), err)
				return res, err
			}
		}

		if stalePages >= w.maxStalePages && newInPage == 0 {
			// 连续多页全部是旧元素（或空页）：游标虽在变，但遍历不再有进展，
			// 作为异常收敛，避免在服务端数据反复抖动时无限跑下去。
			w.finish(res, "")
			err := &Error{Kind: KindCursorAnomaly, Op: "Walker.Run",
				Err: fmt.Errorf("%w: consecutive_stale_pages=%d", ErrNoProgress, stalePages)}
			w.logf("walk end 判定依据: 连续 %d 页零新元素 -> %v", stalePages, err)
			return res, err
		}

		if advance {
			w.cursor = page.NextCursor
			w.pushRecent(w.cursor)
		}
	}
}

// finish 用当前进度填充结果中的续传标记。
func (w *Walker[T]) finish(res *WalkResult[T], reason EndReason) {
	res.Reason = reason
	res.Checkpoint = w.Checkpoint()
}
