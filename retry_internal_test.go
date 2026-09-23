package resilientclient

import (
	"net/http"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	h := http.Header{}
	if _, ok := parseRetryAfter(h); ok {
		t.Fatal("无 Retry-After 时应返回 ok=false")
	}
	h.Set("Retry-After", "2")
	if d, ok := parseRetryAfter(h); !ok || d != 2*time.Second {
		t.Fatalf("delta-seconds 解析错误: %v %v", d, ok)
	}
	h.Set("Retry-After", "not-a-date")
	if _, ok := parseRetryAfter(h); ok {
		t.Fatal("非法值应返回 ok=false，而非 panic/负数")
	}
	// HTTP-date 形式：指向 1 秒后，解析出的等待应接近 1s 且非负。
	future := time.Now().Add(1500 * time.Millisecond).UTC().Format(http.TimeFormat)
	h.Set("Retry-After", future)
	d, ok := parseRetryAfter(h)
	if !ok {
		t.Fatal("HTTP-date 形式应可解析")
	}
	if d < 500*time.Millisecond || d > 3*time.Second {
		t.Fatalf("HTTP-date 换算的等待时长异常: %v", d)
	}
	t.Logf("判定依据: delta-seconds 与 HTTP-date 两种形式均可解析，非法值安全忽略，date->%v", d)
}

func TestIdempotencyClassification(t *testing.T) {
	idempotent := []string{"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"}
	for _, m := range idempotent {
		if !isIdempotentMethod(m) {
			t.Fatalf("%s 应判定为幂等", m)
		}
	}
	for _, m := range []string{"POST", "PATCH", ""} {
		if isIdempotentMethod(m) {
			t.Fatalf("%q 不应判定为幂等", m)
		}
	}
}

func TestBackoffMonotonicAndCapped(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 6, BaseDelay: 10 * time.Millisecond,
		MaxDelay: 100 * time.Millisecond, RetryAfterCap: time.Second}
	var prev time.Duration
	// 统计意义上退避应增长（无 Retry-After 时中位数翻倍），且永不超过 MaxDelay。
	for attempt := 1; attempt <= 5; attempt++ {
		var sum time.Duration
		for range 200 {
			d := p.backoff(attempt, 0)
			if d > p.MaxDelay {
				t.Fatalf("退避 %v 超过 MaxDelay %v", d, p.MaxDelay)
			}
			sum += d
		}
		avg := sum / 200
		if attempt > 1 && avg < prev {
			t.Fatalf("判定依据：平均退避应随次数增长，attempt=%d avg=%v prev=%v", attempt, avg, prev)
		}
		prev = avg
	}
	// Retry-After 超过 Cap 时必须被封顶。
	if d := p.backoff(1, time.Hour); d > p.MaxDelay {
		t.Fatalf("超大 Retry-After 应先受 RetryAfterCap 再受 MaxDelay 封顶，实际 %v", d)
	}
}
