package resilientclient

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// RetryPolicy 描述重试与退避策略。
//
// 重试前提（硬性规则）：仅幂等方法（GET/HEAD/OPTIONS/PUT/DELETE/TRACE）
// 才允许在可恢复失败后重试；POST/PATCH 等非幂等请求绝不重试，避免重复副作用。
//
// 可恢复失败指：传输层错误、尝试级超时，以及 429/500/502/503/504 响应。
// 调用方取消（KindCanceled）永远不重试。
type RetryPolicy struct {
	MaxAttempts   int           // 含首次请求在内的最大尝试次数，<=1 表示不重试
	BaseDelay     time.Duration // 首次退避基准
	MaxDelay      time.Duration // 指数退避上限
	RetryAfterCap time.Duration // 尊重服务端 Retry-After 的上限，防止恶意/异常长提示
}

// DefaultRetryPolicy 返回默认重试策略：最多 4 次尝试，100ms 基准，上限 2s。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:   4,
		BaseDelay:     100 * time.Millisecond,
		MaxDelay:      2 * time.Second,
		RetryAfterCap: 5 * time.Second,
	}
}

// NoRetry 返回禁用重试的策略。
func NoRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1}
}

// isIdempotentMethod 判断 HTTP 方法是否幂等（RFC 9110 语义）。
func isIdempotentMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions,
		http.MethodPut, http.MethodDelete, http.MethodTrace:
		return true
	default:
		return false
	}
}

// retryableStatus 判断非成功状态码是否属于可恢复类别。
func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// backoff 计算第 attempt 次失败后到下一次尝试的等待时长（attempt 从 1 开始）。
//
// 规则：指数退避 BaseDelay*2^(attempt-1)，叠加 [0.5,1) 的全抖动；
// 若服务端给出合法 Retry-After（retryAfter>0）且其值大于计算值，则尊重服务端提示；
// 服务端提示受 RetryAfterCap 封顶，最终等待受 MaxDelay 封顶。
func (p RetryPolicy) backoff(attempt int, retryAfter time.Duration) time.Duration {
	exp := time.Duration(1)
	for i := 0; i < attempt-1 && exp < p.MaxDelay; i++ {
		exp *= 2
	}
	d := p.BaseDelay * exp
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	// 全抖动，避免大量客户端同时重试形成惊群。
	d = time.Duration(float64(d) * (0.5 + 0.5*rand.Float64()))

	if retryAfter > 0 {
		ra := retryAfter
		if p.RetryAfterCap > 0 && ra > p.RetryAfterCap {
			ra = p.RetryAfterCap
		}
		if ra > d { // 服务端明确要求等更久时，以服务端为准
			d = ra
		}
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	return d
}

// parseRetryAfter 解析 Retry-After 头：支持 delta-seconds 与 HTTP-date 两种形式。
func parseRetryAfter(header http.Header) (time.Duration, bool) {
	v := header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
