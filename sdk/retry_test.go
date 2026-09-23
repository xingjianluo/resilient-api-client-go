package sdk

import (
	"net/http"
	"testing"
	"time"
)

func TestBackoffRespectsRetryAfter(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second, Multiplier: 2}

	// 指数退避：100ms, 200ms, 400ms
	for i, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond} {
		if got := p.backoff(i+1, 0); got != want {
			t.Errorf("attempt=%d backoff=%v want=%v", i+1, got, want)
		}
	}
	// 服务端提示更长时采用提示。
	if got := p.backoff(1, 3*time.Second); got != 3*time.Second {
		t.Errorf("retryAfter not respected: got=%v", got)
	}
	// 封顶 MaxDelay。
	if got := p.backoff(1, 10*time.Second); got != 5*time.Second {
		t.Errorf("max delay cap: got=%v", got)
	}
	t.Logf("判定依据: base=100ms mult=2 max=5s, retryAfter 取较大值并封顶")
}

func TestRetryAfterFromHeader(t *testing.T) {
	h := http.Header{"Retry-After": []string{"2"}}
	if got := retryAfterFrom(h); got != 2*time.Second {
		t.Errorf("got=%v want=2s", got)
	}
	if got := retryAfterFrom(http.Header{}); got != 0 {
		t.Errorf("empty header got=%v", got)
	}
}

func TestIdempotentClassification(t *testing.T) {
	for _, m := range []string{"GET", "HEAD", "PUT", "DELETE", "OPTIONS"} {
		if !isIdempotent(m) {
			t.Errorf("%s should be idempotent", m)
		}
	}
	if isIdempotent("POST") {
		t.Error("POST must not be idempotent (no retry)")
	}
	if !retryableStatus(429) || !retryableStatus(503) || retryableStatus(400) {
		t.Error("retryableStatus: 429/5xx retryable, 4xx not")
	}
}
