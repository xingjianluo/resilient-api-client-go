package resilientclient

import (
	"context"
	"sync"
	"time"
)

// Limiter 限制发往服务端的请求速率。
type Limiter interface {
	// Wait 阻塞直到获得一个令牌：
	//   - 预计等待超过容忍上限时，立即返回 KindQuota（配额耗尽）；
	//   - 等待期间整体截止到期，返回 KindTimeout；
	//   - 等待期间调用方取消，返回 KindCanceled。
	// 三种原因彼此可区分，不会互相掩盖。
	Wait(ctx context.Context) error
}

// TokenBucket 是本地令牌桶限流器：
// 以固定速率 ratePerSec 补充令牌，桶容量为 burst。
type TokenBucket struct {
	rate    float64 // 每秒补充的令牌数
	burst   float64 // 桶容量
	maxWait time.Duration

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// NewTokenBucket 创建令牌桶。
// ratePerSec<=0 表示不限流（Wait 立即返回）；maxWait<=0 表示无限等待。
func NewTokenBucket(ratePerSec float64, burst int, maxWait time.Duration) *TokenBucket {
	l := &TokenBucket{rate: ratePerSec, burst: float64(burst), maxWait: maxWait}
	if burst > 0 {
		l.tokens = float64(burst) // 初始满桶，允许启动瞬时突发
	}
	return l
}

func (l *TokenBucket) refillLocked(now time.Time) {
	if l.last.IsZero() {
		l.last = now
		return
	}
	elapsed := now.Sub(l.last).Seconds()
	l.last = now
	l.tokens += elapsed * l.rate
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
}

// Wait 获取一个令牌。
func (l *TokenBucket) Wait(ctx context.Context) error {
	const op = "rate limiter"
	if l.rate <= 0 {
		return ctx.Err() // 不限流；仍尊重已经结束的 context
	}
	if err := ctx.Err(); err != nil {
		return newAPIError(classifyContextErr(err), op, err, "context already done before acquiring token")
	}

	for {
		now := time.Now()
		l.mu.Lock()
		l.refillLocked(now)
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		// 距离下一个令牌可用还需多久。
		wait := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()

		if wait <= 0 {
			continue
		}
		// 配额耗尽：需要的等待超出调用方/客户端声明的容忍上限，立即拒绝。
		if l.maxWait > 0 && wait > l.maxWait {
			return newAPIError(KindQuota, op, ErrQuota,
				"next token would be ready in "+wait.String()+", exceeding maxWait "+l.maxWait.String())
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return newAPIError(classifyContextErr(ctx.Err()), op, ctx.Err(),
				"context ended while waiting for rate-limit token")
		case <-timer.C:
		}
	}
}
