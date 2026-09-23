package sdk

import (
	"context"
	"errors"
)

// Page 是一页结果。
type Page[T any] struct {
	Items []T
	// NextCursor 为空表示没有下一页。
	NextCursor string
}

// FetchPage 按游标拉取一页。
type FetchPage[T any] func(ctx context.Context, cursor string) (Page[T], error)

// ErrPaginationStuck 表示游标没有推进（服务端异常），用于保证遍历必然终止。
var ErrPaginationStuck = errors.New("sdk: pagination cursor did not advance")

// Iterator 遍历分页结果：
//   - 以游标严格推进保证必然终止（游标不前进则报错）；
//   - 以 seen 集合按 key 去重，保证遍历期间数据变动（插入/删除导致元素跨页漂移）时结果不重复。
type Iterator[T any] struct {
	fetch  FetchPage[T]
	key    func(T) string // 元素去重键
	seen   map[string]struct{}
	cursor string
}

// NewIterator 创建分页迭代器；key 用于去重（如元素 ID）。
func NewIterator[T any](fetch FetchPage[T], key func(T) string) *Iterator[T] {
	return &Iterator[T]{fetch: fetch, key: key, seen: make(map[string]struct{})}
}

// Collect 收集全部去重后的元素；游标不推进时返回 ErrPaginationStuck。
func (it *Iterator[T]) Collect(ctx context.Context) ([]T, error) {
	var out []T
	for {
		page, err := it.fetch(ctx, it.cursor)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			k := it.key(item)
			if _, dup := it.seen[k]; dup {
				continue // 数据变动导致跨页重复，跳过
			}
			it.seen[k] = struct{}{}
			out = append(out, item)
		}
		if page.NextCursor == "" {
			return out, nil // 没有下一页，终止
		}
		if page.NextCursor == it.cursor {
			// 游标未推进：若不报错将死循环，必须终止。
			return nil, ErrPaginationStuck
		}
		it.cursor = page.NextCursor
	}
}
