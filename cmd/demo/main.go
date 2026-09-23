// Command demo 启动进程内模拟服务端，演示：正常调用、脚本化失败+退避重试、
// 游标分页遍历（中途增删数据）、凭证过期自动刷新。
//
// 运行：go run ./cmd/demo
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	resilient "github.com/xingjianluo/resilient-api-client-go"
	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

func main() {
	srv := mockapi.New(
		mockapi.WithTokenTTL(800*time.Millisecond),
		mockapi.WithInitialItems(
			mockapi.Item{ID: "a", Name: "alpha"},
			mockapi.Item{ID: "b", Name: "bravo"},
			mockapi.Item{ID: "c", Name: "charlie"},
			mockapi.Item{ID: "d", Name: "delta"},
			mockapi.Item{ID: "e", Name: "echo"},
		),
	)
	defer srv.Close()
	fmt.Println("mock server:", srv.URL())

	// TokenSource：真实项目里换成授权服务器地址；这里打到模拟服务端。
	tokenSrc := func(ctx context.Context) (resilient.AccessToken, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL()+"/v1/token", bytes.NewReader([]byte(`{}`)))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return resilient.AccessToken{}, err
		}
		defer resp.Body.Close()
		var tr struct {
			AccessToken string `json:"access_token"`
			ExpiresIn   int64  `json:"expires_in"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
			return resilient.AccessToken{}, err
		}
		at := resilient.AccessToken{Value: tr.AccessToken}
		if tr.ExpiresIn > 0 {
			at.ExpiresAt = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
		}
		return at, nil
	}

	client := resilient.New(srv.URL(),
		resilient.WithTokenSource(tokenSrc),
		resilient.WithRetryPolicy(resilient.RetryPolicy{
			MaxAttempts: 4, BaseDelay: 50 * time.Millisecond,
			MaxDelay: time.Second, RetryAfterCap: 2 * time.Second,
		}),
		resilient.WithLimiter(resilient.NewTokenBucket(50, 10, 2*time.Second)),
		resilient.WithAttemptTimeout(3*time.Second),
		resilient.WithLogger(func(format string, args ...any) { log.Printf("  "+format, args...) }),
	)
	ctx := context.Background()

	// 1) 前两次调用模拟 503（可恢复），第三次成功：演示幂等重试与退避。
	srv.Fail("/v1/items/a", 2, http.StatusServiceUnavailable, 0)
	var item resilient.Item
	if _, err := client.GetJSON(ctx, "/v1/items/a", &item); err != nil {
		log.Fatalf("get item: %v", err)
	}
	fmt.Printf("✓ 经过重试后取到条目: %+v，服务端命中次数=%d\n", item, srv.Hits("/v1/items/a"))

	// 2) 分页遍历；遍历期间数据集发生增删。
	srv.AddItem(mockapi.Item{ID: "f", Name: "foxtrot"})
	it := client.IterItems()
	var ids []string
	go func() {
		time.Sleep(10 * time.Millisecond)
		srv.AddItem(mockapi.Item{ID: "g", Name: "golf"})
		srv.RemoveItem("c")
	}()
	for {
		itm, ok, err := it.Next(ctx)
		if err != nil {
			log.Fatalf("iterate: %v", err)
		}
		if !ok {
			break
		}
		ids = append(ids, itm.ID)
	}
	fmt.Printf("✓ 分页遍历（含中途增删）共得到 %d 个不重复条目: %v\n", len(ids), ids)

	// 3) 凭证过期：下一次调用应透明刷新（single-flight 由单测验证）。
	time.Sleep(900 * time.Millisecond)
	var echo map[string]any
	if _, err := client.GetJSON(ctx, "/v1/echo", &echo); err != nil {
		log.Fatalf("echo after expiry: %v", err)
	}
	fmt.Printf("✓ 凭证过期后自动刷新并调用成功，令牌端点总命中=%d\n", srv.TokenHits())
}
