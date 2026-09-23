package resilientclient

import (
	"context"
	"fmt"
	"net/http"
)

// Item 是分页遍历的元素。
type Item struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// listPage 对应模拟服务端的分页信封。
type listPage struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// Iterator 遍历游标分页列表。
//
// 终止性保证：每次前进的游标要么是服务端首次给出的新游标，要么为空；
// 一旦遇到此前见过的游标（游标成环）或非法游标，立即以 KindPagination 终止，
// 因此即使服务端有缺陷也不会死循环。
//
// 不重复保证：遍历期间数据可能增删，同一 ID 只会被产出一次；
// 已遍历位置上的新增数据可能不可见（游标快照语义），删除的数据则安全跳过。
type Iterator struct {
	client    *Client
	pagePath  string
	seen      map[string]struct{} // 已见过的游标
	seenItems map[string]struct{} // 已产出过的条目 ID
	buf       []Item
	cursor    string
	done      bool
	page      int // 已请求的页码，仅用于日志
}

// IterItems 创建条目迭代器，从 /v1/items 首页开始。
func (c *Client) IterItems() *Iterator {
	return &Iterator{
		client:    c,
		pagePath:  "/v1/items",
		seen:      map[string]struct{}{},
		seenItems: map[string]struct{}{},
	}
}

// Next 返回下一个条目。
// 遍历正常结束时 ok=false、err=nil；发生错误时 ok=false、err 为 *APIError。
func (it *Iterator) Next(ctx context.Context) (item Item, ok bool, err error) {
	for {
		// 先消费当前页缓冲（跨页去重）。
		for len(it.buf) > 0 {
			itm := it.buf[0]
			it.buf = it.buf[1:]
			if _, dup := it.seenItems[itm.ID]; dup {
				it.client.log("pagination: skipping duplicate item id=%s page=%d cursor=%s",
					itm.ID, it.page, it.cursor)
				continue
			}
			it.seenItems[itm.ID] = struct{}{}
			it.client.log("pagination: yield item id=%s name=%q page=%d cursor=%s",
				itm.ID, itm.Name, it.page, it.cursor)
			return itm, true, nil
		}
		if it.done {
			it.client.log("pagination: finished, total_unique_items=%d pages=%d",
				len(it.seenItems), it.page)
			return Item{}, false, nil
		}

		path := it.pagePath
		if it.cursor != "" {
			path = fmt.Sprintf("%s?cursor=%s", it.pagePath, it.cursor)
		}
		it.page++
		it.client.log("pagination: fetching page=%d path=%s (判定: 缓冲耗尽，请求下一页)", it.page, path)

		var page listPage
		reqID, gerr := it.client.GetJSON(ctx, path, &page)
		if gerr != nil {
			ae := asAPIError(gerr, http.MethodGet+" "+path)
			ae.Op = "pagination " + ae.Op
			return Item{}, false, ae
		}

		if page.NextCursor == "" {
			// 空游标：服务端明确告知没有下一页，正常终止。
			it.buf = page.Items
			it.done = true
			continue
		}
		if page.NextCursor == it.cursor {
			// 服务端返回了与当前完全相同的非空游标：原地踏步，必然成环。
			return Item{}, false, newPaginationError(path, reqID,
				fmt.Sprintf("server returned the same non-empty cursor %q again", page.NextCursor))
		}
		if _, seen := it.seen[page.NextCursor]; seen {
			return Item{}, false, newPaginationError(path, reqID,
				fmt.Sprintf("cursor cycle detected: cursor %q was already visited", page.NextCursor))
		}

		it.buf = page.Items
		it.seen[page.NextCursor] = struct{}{}
		it.cursor = page.NextCursor
	}
}

func newPaginationError(path, reqID, msg string) *APIError {
	e := newAPIError(KindPagination, "GET "+path, nil, msg)
	e.RequestID = reqID
	return e
}
