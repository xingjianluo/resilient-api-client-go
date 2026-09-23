package sdk

import (
	"context"
	"sync"
	"time"
)

// Token 表示一份访问凭证。
type Token struct {
	Value     string
	ExpiresAt time.Time
}

// valid 判断凭证在 now 时刻是否仍可用（预留 skew 提前量，避免临界过期）。
func (t Token) valid(now time.Time, skew time.Duration) bool {
	return t.Value != "" && now.Add(skew).Before(t.ExpiresAt)
}

// RefreshFunc 实际执行凭证刷新的函数。
type RefreshFunc func(ctx context.Context) (Token, error)

// TokenProvider 缓存凭证，过期时并发触发的一次刷新只实际执行一次（singleflight）：
// 等待中的调用方共享同一次刷新结果，不互相阻塞、不放大请求。
type TokenProvider struct {
	mu      sync.Mutex
	refresh RefreshFunc
	skew    time.Duration
	token   Token
	// inflight 非空表示有刷新正在进行，其它调用方等待其结果。
	inflight chan struct{}
	result   Token
	err      error
	now      func() time.Time
	// RefreshCount 记录实际执行的刷新次数（用于测试与观测）。
	RefreshCount int
}

// NewTokenProvider 创建凭证提供者；skew 为过期提前量。
func NewTokenProvider(refresh RefreshFunc, skew time.Duration) *TokenProvider {
	return &TokenProvider{refresh: refresh, skew: skew, now: time.Now}
}

// Token 返回有效凭证，必要时触发或等待刷新。
func (p *TokenProvider) Token(ctx context.Context) (Token, error) {
	p.mu.Lock()
	if p.token.valid(p.now(), p.skew) {
		t := p.token
		p.mu.Unlock()
		return t, nil
	}
	if p.inflight != nil {
		// 已有刷新在进行：等待其完成并共享结果，不再发起新请求。
		done := p.inflight
		p.mu.Unlock()
		select {
		case <-done:
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.result, p.err
		case <-ctx.Done():
			return Token{}, classifyCtxErr("TokenProvider.Token", ctx.Err())
		}
	}
	// 自己成为刷新者。
	done := make(chan struct{})
	p.inflight = done
	p.RefreshCount++
	p.mu.Unlock()

	tok, err := p.refresh(ctx)

	p.mu.Lock()
	p.result, p.err = tok, err
	if err == nil {
		p.token = tok
	}
	p.inflight = nil
	p.mu.Unlock()
	close(done) // 唤醒所有等待者

	if err != nil {
		return Token{}, err
	}
	return tok, nil
}
