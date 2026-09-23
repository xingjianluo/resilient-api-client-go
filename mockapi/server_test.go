package mockapi_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

func TestMockServerAuthFlow(t *testing.T) {
	srv := mockapi.New(mockapi.WithTokenTTL(50*time.Millisecond),
		mockapi.WithInitialItems(mockapi.Item{ID: "x", Name: "X"}))
	defer srv.Close()

	// 无令牌 -> 401。
	resp, err := http.Get(srv.URL() + "/v1/items/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("X-Request-Id") == "" {
		t.Fatalf("无凭证应 401 且带请求标识，status=%d", resp.StatusCode)
	}

	// 取令牌。
	tr, err := http.Post(srv.URL()+"/v1/token", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(tr.Body).Decode(&tok); err != nil {
		t.Fatal(err)
	}
	tr.Body.Close()

	// 带令牌 -> 200。
	req, _ := http.NewRequest(http.MethodGet, srv.URL()+"/v1/items/x", nil)
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	okResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	okResp.Body.Close()
	if okResp.StatusCode != http.StatusOK {
		t.Fatalf("带凭证应 200，实际 %d", okResp.StatusCode)
	}
	if srv.TokenHits() != 1 || srv.Hits("/v1/items/x") != 2 {
		t.Fatalf("观测计数异常: tokenHits=%d itemHits=%d", srv.TokenHits(), srv.Hits("/v1/items/x"))
	}
}

func TestMockServerScriptedFailures(t *testing.T) {
	srv := mockapi.New(mockapi.WithNoAuth())
	defer srv.Close()
	srv.Fail("/v1/badjson", 1, http.StatusTooManyRequests, 250*time.Millisecond)

	resp, err := http.Get(srv.URL() + "/v1/badjson")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("第一次应被脚本化为 429，实际 %d", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "1" {
		t.Fatalf("250ms 应按秒向上取整为 Retry-After: 1，实际 %q", ra)
	}

	resp2, err := http.Get(srv.URL() + "/v1/badjson")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	// 脚本化失败耗尽后恢复为该路径原本行为（非法 JSON，但状态 200）。
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("失败次数耗尽后应恢复，实际 %d", resp2.StatusCode)
	}
}
