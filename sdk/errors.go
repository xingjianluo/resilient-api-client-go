// Package sdk 提供可复用的远程 API 客户端：限流、重试、分页与凭证刷新。
package sdk

import (
	"errors"
	"fmt"
	"time"
)

// ErrorKind 对外可区分的错误类别。
type ErrorKind string

const (
	// KindTransport 传输层失败（连接失败、DNS 等）。
	KindTransport ErrorKind = "transport"
	// KindStatus 服务端返回非成功状态码。
	KindStatus ErrorKind = "status"
	// KindDecode 响应体不合法（无法解析）。
	KindDecode ErrorKind = "decode"
	// KindTimeout 调用方设置的超时到期。
	KindTimeout ErrorKind = "timeout"
	// KindCanceled 调用方主动取消。
	KindCanceled ErrorKind = "canceled"
	// KindQuotaExhausted 限流配额耗尽（等待令牌时上下文结束）。
	KindQuotaExhausted ErrorKind = "quota_exhausted"
)

// Error 是 SDK 对外暴露的统一错误类型，底层库异常不会直接透出。
type Error struct {
	Kind      ErrorKind
	Op        string // 发生错误的操作，如 "Do"
	RequestID string // 服务端返回的请求标识（若有）
	// StatusCode 仅 KindStatus 时有意义。
	StatusCode int
	// RetryAfter 服务端建议的重试等待（429/503 时可能有值）。
	RetryAfter time.Duration
	Err        error // 归一化后的底层原因，不直接透出原始库类型
}

func (e *Error) Error() string {
	return fmt.Sprintf("sdk: %s failed: kind=%s request_id=%q: %v", e.Op, e.Kind, e.RequestID, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// KindOf 返回错误的类别；非 SDK 错误返回空串。
func KindOf(err error) ErrorKind {
	var se *Error
	if errors.As(err, &se) {
		return se.Kind
	}
	return ""
}

// RequestIDOf 提取错误中保留的服务端请求标识。
func RequestIDOf(err error) string {
	var se *Error
	if errors.As(err, &se) {
		return se.RequestID
	}
	return ""
}
