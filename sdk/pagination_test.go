package sdk

import (
	"context"
	"errors"
	"strconv"
	"testing"
)

type item struct{ ID string }

// fetchFrom 基于快照切片的偏移分页（模拟服务端）。
func fetchFrom(data []item, limit int) FetchPage[item] {
	return func(ctx context.Context, cursor string) (Page[item], error) {
		off, _ := strconv.Atoi(cursor)
		end := off + limit
		if end > len(data) {
			end = len(data)
		}
		next := ""
		if end < len(data) {
			next = strconv.Itoa(end)
		}
		return Page[item]{Items: data[off:end], NextCursor: next}, nil
	}
}

func TestPaginationCollectAll(t *testing.T) {
	data := []item{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}}
	it := NewIterator(fetchFrom(data, 2), func(i item) string { return i.ID })
	got, err := it.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d items want 5", len(got))
	}
	t.Logf("collected=%v pages=3 (limit=2)", got)
}

func TestPaginationDedupUnderMutation(t *testing.T) {
	// 模拟遍历中首元素被删除导致后续元素跨页漂移。
	data := []item{{"a"}, {"b"}, {"c"}, {"d"}}
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		p, err := fetchFrom(data, 2)(ctx, cursor)
		if cursor == "" {
			// 第一页返回后删除 "a"，"c" 将漂移到第二页重复出现。
			data = data[1:]
			t.Logf("mutation: removed first item, remaining=%v", data)
		}
		return p, err
	}
	it := NewIterator(fetch, func(i item) string { return i.ID })
	got, err := it.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, i := range got {
		seen[i.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("item %s appeared %d times", id, n)
		}
	}
	t.Logf("dedup ok: %v", got)
}

func TestPaginationStuckTerminates(t *testing.T) {
	fetch := func(ctx context.Context, cursor string) (Page[item], error) {
		return Page[item]{Items: []item{{"x"}}, NextCursor: "same"}, nil
	}
	it := NewIterator(fetch, func(i item) string { return i.ID })
	_, err := it.Collect(context.Background())
	if !errors.Is(err, ErrPaginationStuck) {
		t.Fatalf("want ErrPaginationStuck, got %v", err)
	}
	t.Logf("判定依据: next_cursor 未推进 -> %v", err)
}
