package resilientclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	resilient "github.com/xingjianluo/resilient-api-client-go"
	"github.com/xingjianluo/resilient-api-client-go/mockapi"
)

// newTestClient 构造一个连接到模拟服务端、带凭证与日志的客户端。
func newTestClient(t *testing.T, srv *mockapi.Server, opts ...resilient.Option) *resilient.Client {
	t.Helper()
	defaults := []resilient.Option{
		resilient.WithTokenSource(tokenSource(srv)),
		resilient.WithLogger(func(format string, args ...any) {
			t.Logf("[client] "+format, args...)
		}),
	}
	return resilient.New(srv.URL(), append(defaults, opts...)...)
}

// tokenSource 返回一个调用模拟服务端 /v1/token 的凭证换取函数。
func tokenSource(srv *mockapi.Server) resilient.TokenSource {
	return func(ctx context.Context) (resilient.AccessToken, error) {
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
}

func newServer(t *testing.T, opts ...mockapi.Option) *mockapi.Server {
	t.Helper()
	srv := mockapi.New(opts...)
	t.Cleanup(srv.Close)
	return srv
}

// assertKind 断言 err 是 *APIError 且 Kind 符合预期，并记录判定依据。
func assertKind(t *testing.T, err error, want resilient.ErrorKind) *resilient.APIError {
	t.Helper()
	var ae *resilient.APIError
	if !errors.As(err, &ae) {
		t.Fatalf("期望错误归一化为 *APIError(kind=%s)，实际类型=%T err=%v", want, err, err)
	}
	if ae.Kind != want {
		t.Fatalf("错误类别判定：want=%s got=%s（判定依据：%v，request_id=%s）",
			want, ae.Kind, err, ae.RequestID)
	}
	t.Logf("错误类别判定通过：kind=%s，原因=%s，request_id=%s", ae.Kind, ae.Message, ae.RequestID)
	return ae
}

func mustItems(n int) []mockapi.Item {
	items := make([]mockapi.Item, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, mockapi.Item{
			ID:   string(rune('a' + i)),
			Name: "item-" + string(rune('a'+i)),
		})
	}
	return items
}
