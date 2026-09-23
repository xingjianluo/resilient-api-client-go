package sdk

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTokenProviderSingleflight(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	refresh := func(ctx context.Context) (Token, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(50 * time.Millisecond) // 模拟慢刷新，放大并发窗口
		return Token{Value: "tok", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	p := NewTokenProvider(refresh, 10*time.Second)

	const n = 16
	var wg sync.WaitGroup
	results := make([]Token, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = p.Token(context.Background())
		}(i)
	}
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("caller %d err=%v", i, errs[i])
		}
		if results[i].Value != "tok" {
			t.Fatalf("caller %d got %q", i, results[i].Value)
		}
	}
	if calls != 1 {
		t.Fatalf("refresh executed %d times, want exactly 1", calls)
	}
	if p.RefreshCount != 1 {
		t.Fatalf("RefreshCount=%d want 1", p.RefreshCount)
	}
	t.Logf("并发=%d 实际刷新=%d 判定依据: singleflight 共享同一次刷新结果", n, calls)
}

func TestTokenProviderCachesValidToken(t *testing.T) {
	calls := 0
	refresh := func(ctx context.Context) (Token, error) {
		calls++
		return Token{Value: "tok", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	p := NewTokenProvider(refresh, 10*time.Second)
	for i := 0; i < 5; i++ {
		if _, err := p.Token(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("cached token should avoid refresh, calls=%d", calls)
	}
}

func TestTokenProviderRefreshesExpired(t *testing.T) {
	calls := 0
	now := time.Now()
	p := NewTokenProvider(func(ctx context.Context) (Token, error) {
		calls++
		// 每次签发的凭证 1 秒后过期（skew=1s => 立即视为过期）。
		return Token{Value: "tok", ExpiresAt: now.Add(time.Second)}, nil
	}, time.Second)
	p.now = func() time.Time { return now }
	if _, err := p.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expired token must trigger refresh, calls=%d", calls)
	}
	t.Logf("判定依据: skew=1s 时 1s 有效期的凭证立即过期 -> 再次刷新")
}
