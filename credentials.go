package resilientclient

import (
	"context"
	"sync"
	"time"
)

// AccessToken 是带过期时刻的访问凭证。
type AccessToken struct {
	Value     string
	ExpiresAt time.Time
}

// TokenSource 负责实际向授权方换取凭证（真实项目中通常是一次 HTTP 调用）。
// 刷新过程使用独立于任何单次请求的 context 发起，因此某个等待方超时或取消
// 不会连累其他等待方；实现方应自行控制换取超时。
type TokenSource func(ctx context.Context) (AccessToken, error)

type refreshCall struct {
	done chan struct{}
	tok  AccessToken
	err  error
}

// CachedTokenProvider 缓存凭证，并保证：凭证过期/缺失时并发触发的多次刷新
// 只有一次真正执行（single-flight）；其余调用方共享同一次刷新结果。
// 某个等待方提前超时或取消只会让自己返回，不会取消刷新、不会阻塞其他等待方。
type CachedTokenProvider struct {
	src  TokenSource
	skew time.Duration
	now  func() time.Time

	mu       sync.Mutex
	cached   AccessToken
	hasToken bool
	epoch    int64 // Invalidate 自增，用于丢弃失效期间产生的旧刷新结果
	inFlight *refreshCall
}

// NewCachedTokenProvider 创建带缓存的凭证提供者。
// earlyRefreshSkew 表示提前量：令牌剩余寿命小于该值即视为过期，避免边界过期。
func NewCachedTokenProvider(src TokenSource, earlyRefreshSkew time.Duration) *CachedTokenProvider {
	return &CachedTokenProvider{src: src, skew: earlyRefreshSkew, now: time.Now}
}

func (p *CachedTokenProvider) validLocked(t AccessToken) bool {
	if t.Value == "" {
		return false
	}
	if t.ExpiresAt.IsZero() {
		return true // 无过期时间的令牌视为长期有效
	}
	return p.now().Add(p.skew).Before(t.ExpiresAt)
}

// Token 返回有效的访问令牌；过期时并发调用仅触发一次实际刷新。
func (p *CachedTokenProvider) Token(ctx context.Context) (string, error) {
	const op = "credential refresh"

	p.mu.Lock()
	if p.hasToken && p.validLocked(p.cached) {
		v := p.cached.Value
		p.mu.Unlock()
		return v, nil
	}

	call := p.inFlight
	if call == nil {
		// 本调用成为唯一的实际刷新发起者。
		call = &refreshCall{done: make(chan struct{})}
		p.inFlight = call
		epoch := p.epoch
		go p.runRefresh(call, epoch)
	}
	p.mu.Unlock()

	// 等待共享刷新结果，但不屏蔽本调用方自身的超时/取消。
	select {
	case <-ctx.Done():
		return "", newAPIError(classifyContextErr(ctx.Err()), op, ctx.Err(),
			"context ended while waiting for shared token refresh")
	case <-call.done:
	}
	if call.err != nil {
		return "", newAPIError(KindCredential, op, call.err, "token source returned an error")
	}
	return call.tok.Value, nil
}

func (p *CachedTokenProvider) runRefresh(call *refreshCall, epoch int64) {
	// 独立于触发请求的 context：单个调用方取消不影响整批等待方。
	tok, err := p.src(context.Background())
	call.tok, call.err = tok, err

	p.mu.Lock()
	if p.inFlight == call {
		p.inFlight = nil
	}
	// 仅当刷新期间没有发生过 Invalidate，且令牌有效时才写入缓存。
	if err == nil && epoch == p.epoch && p.validLocked(tok) {
		p.cached = tok
		p.hasToken = true
	}
	p.mu.Unlock()
	close(call.done)
}

// Invalidate 使缓存的凭证失效，下一次 Token 调用会强制发起新刷新。
// 不会取消正在进行的刷新，但其结果若晚于本次失效完成则被丢弃。
func (p *CachedTokenProvider) Invalidate() {
	p.mu.Lock()
	p.epoch++
	p.hasToken = false
	p.cached = AccessToken{}
	p.mu.Unlock()
}
