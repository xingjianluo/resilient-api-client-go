package sdk

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryPolicy 控制重试行为：仅幂等请求在可恢复失败后重试。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次），<=1 表示不重试。
	MaxAttempts int
	// BaseDelay 首次退避基数。
	BaseDelay time.Duration
	// MaxDelay 退避上限。
	MaxDelay time.Duration
	// Multiplier 指数退避倍率。
	Multiplier float64
	// Logger 可选，记录每次重试的判定依据。
	Logger func(format string, args ...any)
}

// DefaultRetryPolicy 返回默认策略。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 4,
		BaseDelay:   50 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		Multiplier:  2.0,
	}
}

// isIdempotent 判断 HTTP 方法是否幂等（GET/HEAD/PUT/DELETE/OPTIONS）。
// POST 等非幂等方法默认不重试，避免重复副作用。
func isIdempotent(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodPut,
		http.MethodDelete, http.MethodOptions:
		return true
	}
	return false
}

// retryableStatus 判断状态码是否可恢复（值得重试）：
// 429（限流）与 5xx（服务端临时故障）可重试，其余 4xx 为调用方问题不重试。
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

// backoff 计算第 attempt 次重试前的等待时长（attempt 从 1 开始），
// 尊重服务端给出的 retryAfter 提示（取两者较大值），并封顶 MaxDelay。
func (p RetryPolicy) backoff(attempt int, retryAfter time.Duration) time.Duration {
	d := p.BaseDelay
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * p.Multiplier)
	}
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	// 服务端提示优先：提示更长则用提示（同样封顶）。
	if retryAfter > d {
		d = retryAfter
		if d > p.MaxDelay {
			d = p.MaxDelay
		}
	}
	return d
}

// retryAfterFrom 解析响应头中的 Retry-After 提示（秒数或 HTTP 日期）。
func retryAfterFrom(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}
