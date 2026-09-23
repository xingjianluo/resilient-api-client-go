package resilientclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	resilient "github.com/xingjianluo/resilient-api-client-go"
	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

func collect(t *testing.T, it *resilient.Iterator) []string {
	t.Helper()
	var ids []string
	for {
		item, ok, err := it.Next(context.Background())
		if err != nil {
			t.Fatalf("分页遍历出错: %v", err)
		}
		if !ok {
			break
		}
		ids = append(ids, item.ID)
	}
	return ids
}

// 基本遍历：5 条数据、每页 2 条，顺序完整无遗漏。
func TestPaginationBasic(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(5)...))
	c := newTestClient(t, srv)
	ids := collect(t, c.IterItems())
	want := []string{"a", "b", "c", "d", "e"}
	if len(ids) != len(want) {
		t.Fatalf("判定依据：应取到 5 条，实际 %v", ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("判定依据：顺序不符，got=%v want=%v", ids, want)
		}
	}
	t.Logf("请求参数: GET /v1/items（游标分页, size=2），重试次数=0，判定依据: 5 条顺序完整=%v", ids)
}

// 空数据集：首页即结束，必然终止。
func TestPaginationEmpty(t *testing.T) {
	srv := newServer(t)
	c := newTestClient(t, srv)
	ids := collect(t, c.IterItems())
	if len(ids) != 0 {
		t.Fatalf("判定依据：空数据集应立即结束，实际 %v", ids)
	}
}

// 遍历期间数据增删：结果不重复、必然终止。
func TestPaginationNoDuplicatesUnderMutation(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(6)...)) // a..f
	c := newTestClient(t, srv)
	it := c.IterItems()

	var ids []string
	steps := 0
	for {
		item, ok, err := it.Next(context.Background())
		if err != nil {
			t.Fatalf("遍历出错: %v", err)
		}
		if !ok {
			break
		}
		ids = append(ids, item.ID)
		steps++
		switch steps {
		case 2: // 第一页后追加（落在游标之后，可能可见）
			srv.AddItem(mockapi.Item{ID: "g", Name: "g"})
		case 4: // 中途删除一个尚未到达的条目并追加重复 ID 不可能（ID 唯一），改为再追加
			srv.RemoveItem("e")
			srv.AddItem(mockapi.Item{ID: "h", Name: "h"})
		}
	}
	seen := map[string]int{}
	for _, id := range ids {
		seen[id]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Fatalf("判定依据：条目 %s 重复产出 %d 次，全部结果=%v", id, n, ids)
		}
	}
	t.Logf("请求参数: 游标分页并在中途 AddItem/RemoveItem，判定依据: 无重复，结果=%v（新增可能因游标快照不可见）", ids)
}

// 游标成环：模拟服务端返回之前访问过的游标，必须以 KindPagination 终止，不得死循环。
func TestPaginationCursorCycle(t *testing.T) {
	mux := http.NewServeMux()
	var calls int64
	mux.HandleFunc("/v1/items", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			_, _ = w.Write([]byte(`{"items":[{"id":"a","name":"a"}],"next_cursor":"c1"}`))
		case 2:
			_, _ = w.Write([]byte(`{"items":[{"id":"b","name":"b"}],"next_cursor":"c2"}`))
		default:
			// 故意让游标回到 c1，形成环 c1 -> c2 -> c1 ...
			_, _ = w.Write([]byte(`{"items":[{"id":"c","name":"c"}],"next_cursor":"c1"}`))
		}
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	c := resilient.New(ts.URL,
		resilient.WithLogger(func(f string, a ...any) { t.Logf("[client] "+f, a...) }))
	it := c.IterItems()

	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		for {
			_, ok, e := it.Next(context.Background())
			if e != nil {
				err = e
				return
			}
			if !ok {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("判定依据：游标成环时迭代器必须终止，实际 2s 内未结束（疑似死循环）")
	}
	ae := assertKind(t, err, resilient.KindPagination)
	t.Logf("判定依据: 检测到重复游标 c1，终止迭代，request_id=%s，服务端被请求次数=%d",
		ae.RequestID, atomic.LoadInt64(&calls))
}

// 分页请求遇到可恢复 500：应透明重试并继续完成遍历。
func TestPaginationRetriesTransientFailure(t *testing.T) {
	srv := newServer(t, mockapi.WithInitialItems(mustItems(3)...))
	c := newTestClient(t, srv, resilient.WithRetryPolicy(resilient.RetryPolicy{
		MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond,
	}))
	srv.Fail("/v1/items", 1, http.StatusInternalServerError, 0) // 首页第一次 500
	ids := collect(t, c.IterItems())
	if len(ids) != 3 {
		t.Fatalf("判定依据：首页瞬时 500 重试后应取全 3 条，实际 %v", ids)
	}
	t.Logf("请求参数: 首页 500 一次，重试次数=1，判定依据: 遍历完成且无重复=%v", ids)
}
