package sdk

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiterQuotaExhausted(t *testing.T) {
	// 速率极低：第一个令牌立即可得，第二个在调用方时限内不可得。
	rl := NewRateLimiter(0.2, 1)
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("first token should be immediate: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := rl.Wait(ctx)
	if KindOf(err) != KindQuotaExhausted {
		t.Fatalf("want KindQuotaExhausted, got %v (kind=%q)", err, KindOf(err))
	}
	t.Logf("判定依据: rate=0.2/s burst=1, 第二令牌需等 5s > 调用方 100ms 时限 -> kind=%s", KindOf(err))
}

func TestRateLimiterDisabled(t *testing.T) {
	rl := NewRateLimiter(0, 0)
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("disabled limiter must pass through: %v", err)
	}
	var nilRL *RateLimiter
	if err := nilRL.Wait(context.Background()); err != nil {
		t.Fatalf("nil limiter must pass through: %v", err)
	}
}

func TestClassifyCtxErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if k := KindOf(classifyCtxErr("op", ctx.Err())); k != KindCanceled {
		t.Fatalf("canceled want %q got %q", KindCanceled, k)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel2()
	time.Sleep(time.Millisecond)
	if k := KindOf(classifyCtxErr("op", ctx2.Err())); k != KindTimeout {
		t.Fatalf("timeout want %q got %q", KindTimeout, k)
	}
	t.Logf("判定依据: context.Canceled->%s, context.DeadlineExceeded->%s", KindCanceled, KindTimeout)
}
