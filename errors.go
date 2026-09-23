package resilientclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// ErrorKind 是对外暴露的、可区分的错误类别。
//
// 底层 net/http、json 等标准库异常不会直接透出，一律被包成 *APIError，
// 调用方可以用 errors.As 取出 *APIError 后按 Kind 分支处理，
// 也可以用 errors.Is(err, ErrTimeout) 等判定常见原因。
type ErrorKind string

const (
	KindTransport  ErrorKind = "transport"  // 传输层失败（连接被拒、连接重置等）
	KindStatus     ErrorKind = "status"     // 服务端返回非成功状态码
	KindDecode     ErrorKind = "decode"     // 响应体无法解析为期望格式
	KindTimeout    ErrorKind = "timeout"    // 超时：整体截止或单次尝试超时
	KindCanceled   ErrorKind = "canceled"   // 调用方主动取消
	KindQuota      ErrorKind = "quota"      // 本地配额耗尽，无法在容忍范围内获取令牌
	KindCredential ErrorKind = "credential" // 凭证获取/刷新失败
	KindPagination ErrorKind = "pagination" // 分页遍历异常（如游标成环）
)

// 哨兵错误，调用方可通过 errors.Is 区分原因，彼此不会互相掩盖。
var (
	ErrTimeout    = errors.New("resilientclient: request timeout")
	ErrCanceled   = errors.New("resilientclient: request canceled")
	ErrQuota      = errors.New("resilientclient: local quota exhausted")
	ErrCredential = errors.New("resilientclient: credential refresh failed")
)

// APIError 是归一化后对外暴露的唯一错误形态。
type APIError struct {
	Kind       ErrorKind
	Op         string        // 逻辑操作描述，如 "GET /v1/items/1"
	StatusCode int           // 仅 KindStatus 时有意义
	RequestID  string        // 服务端返回的请求标识（X-Request-Id）
	Message    string        // 人类可读说明
	Cause      error         // 被包装的底层错误（不直接透出，但可 errors.Is/As）
	retryAfter time.Duration // 服务端 Retry-After 提示（仅可重试的状态错误时有值）
}

// retryHint 返回服务端给出的重试等待提示。
func (e *APIError) retryHint() (time.Duration, bool) {
	if e.retryAfter > 0 {
		return e.retryAfter, true
	}
	return 0, false
}

func (e *APIError) Error() string {
	s := fmt.Sprintf("%s: %s failed", e.Kind, e.Op)
	if e.StatusCode != 0 {
		s += fmt.Sprintf(" (status=%d)", e.StatusCode)
	}
	if e.RequestID != "" {
		s += fmt.Sprintf(" [request_id=%s]", e.RequestID)
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

func (e *APIError) Unwrap() error { return e.Cause }

// Is 让 errors.Is(err, ErrTimeout/ErrCanceled/ErrQuota/ErrCredential) 按 Kind 命中。
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrTimeout:
		return e.Kind == KindTimeout
	case ErrCanceled:
		return e.Kind == KindCanceled
	case ErrQuota:
		return e.Kind == KindQuota
	case ErrCredential:
		return e.Kind == KindCredential
	}
	return false
}

// newAPIError 构造归一化错误。
func newAPIError(kind ErrorKind, op string, cause error, msg string) *APIError {
	return &APIError{Kind: kind, Op: op, Message: msg, Cause: cause}
}

// classifyContextErr 把 context 的结束原因映射为可区分错误：
// context.DeadlineExceeded -> KindTimeout；context.Canceled -> KindCanceled。
func classifyContextErr(err error) ErrorKind {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout
	case errors.Is(err, context.Canceled):
		return KindCanceled
	default:
		return ""
	}
}

// isNetTimeout 判断底层网络错误是否带超时标记。
func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// asAPIError 把可能为 *APIError 的错误原样取出，否则包成传输错误。
func asAPIError(err error, op string) *APIError {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae
	}
	return newAPIError(KindTransport, op, err, "unexpected error")
}
