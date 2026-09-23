package sdk

import (
	"context"
	"errors"
	"sync"
	"time"
)

// RateLimiter 是简单的令牌桶限流器。
type RateLimiter struct {
	mu       sync.Mutex
	rate     float64 // 每秒补充的令牌数
	burst    float64 // 桶容量
	tokens   float64
	lastFill time.Time
	now      func() time.Time // 便于测试注入
}

// NewRateLimiter 创建限流器；rate<=0 表示不限流（Wait 直接放行）。
func NewRateLimiter(rate, burst float64) *RateLimiter {
	if burst < 1 {
		burst = 1
	}
	return &RateLimiter{
		rate:     rate,
		burst:    burst,
		tokens:   burst,
		lastFill: time.Now(),
		now:      time.Now,
	}
}

// refill 按流逝时间补充令牌（调用方需持锁）。
func (r *RateLimiter) refill(now time.Time) {
	elapsed := now.Sub(r.lastFill).Seconds()
	if elapsed > 0 {
		r.tokens += elapsed * r.rate
		if r.tokens > r.burst {
			r.tokens = r.burst
		}
		r.lastFill = now
	}
}

// Wait 阻塞等待一个令牌；等待期间上下文结束返回 KindQuotaExhausted 错误，
// 与超时/取消区分：这里表示“限流配额在调用方时限内不可得”。
func (r *RateLimiter) Wait(ctx context.Context) error {
	if r == nil || r.rate <= 0 {
		return nil // 不限流
	}
	if err := ctx.Err(); err != nil {
		return classifyCtxErr("RateLimiter.Wait", err)
	}
	for {
		r.mu.Lock()
		now := r.now()
		r.refill(now)
		if r.tokens >= 1 {
			r.tokens--
			r.mu.Unlock()
			return nil
		}
		// 计算还需等待多久才能凑够一个令牌。
		missing := 1 - r.tokens
		wait := time.Duration(missing / r.rate * float64(time.Second))
		r.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &Error{
				Kind: KindQuotaExhausted,
				Op:   "RateLimiter.Wait",
				Err:  errors.New("rate limit quota not available before context ended"),
			}
		case <-timer.C:
		}
	}
}

// classifyCtxErr 把上下文结束映射为可区分的 SDK 错误：超时与取消不互相掩盖。
func classifyCtxErr(op string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: KindTimeout, Op: op, Err: err}
	}
	return &Error{Kind: KindCanceled, Op: op, Err: err}
}
