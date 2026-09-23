package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client 是可复用的远程 API 客户端。
type Client struct {
	HTTP      *http.Client
	BaseURL   string
	Limiter   *RateLimiter // nil 表示不限流
	Retry     RetryPolicy
	Tokens    *TokenProvider // nil 表示不附带凭证
	UserAgent string
}

// NewClient 以默认配置创建客户端。
func NewClient(baseURL string) *Client {
	return &Client{
		HTTP:      &http.Client{Timeout: 30 * time.Second},
		BaseURL:   baseURL,
		Retry:     DefaultRetryPolicy(),
		UserAgent: "resilient-api-client-go/1.0",
	}
}

// Request 描述一次调用。
type Request struct {
	Method string
	Path   string
	Body   any // 非 nil 时编码为 JSON
}

// Do 执行请求：限流 -> 附凭证 -> 发送 -> 错误归一化 -> 按需重试。
// 成功时将响应体解码到 out（out 可为 nil）。
func (c *Client) Do(ctx context.Context, req Request, out any) error {
	if err := ctx.Err(); err != nil {
		return classifyCtxErr("Do", err)
	}
	// 1. 限流：配额在调用方时限内不可得时返回 KindQuotaExhausted。
	if err := c.Limiter.Wait(ctx); err != nil {
		return err
	}
	// 2. 预编码请求体，保证重试时可重放。
	bodyReader, bodyBytes, err := encodeBody(req.Body)
	if err != nil {
		return &Error{Kind: KindDecode, Op: "Do", Err: fmt.Errorf("encode request body: %w", err)}
	}
	_ = bodyReader

	idempotent := isIdempotent(req.Method)
	maxAttempts := c.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if !idempotent && maxAttempts > 1 {
		c.logf("request method=%s path=%s 非幂等，禁止重试（attempts=1）", req.Method, req.Path)
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return classifyCtxErr("Do", err)
		}
		c.logf("request attempt=%d/%d method=%s path=%s body=%s",
			attempt, maxAttempts, req.Method, req.Path, string(bodyBytes))

		lastErr = c.doOnce(ctx, req, bodyBytes, out)
		if lastErr == nil {
			if attempt > 1 {
				c.logf("request attempt=%d 成功 path=%s", attempt, req.Path)
			}
			return nil
		}

		var se *Error
		if !errors.As(lastErr, &se) {
			return lastErr
		}
		// 判定是否可重试：仅幂等请求 + 可恢复错误类别。
		retryable := idempotent &&
			(se.Kind == KindTransport || (se.Kind == KindStatus && retryableStatus(se.StatusCode)))
		if !retryable || attempt == maxAttempts {
			c.logf("request attempt=%d 失败 kind=%s retryable=%v 判定=放弃 path=%s",
				attempt, se.Kind, retryable, req.Path)
			return lastErr
		}
		delay := c.Retry.backoff(attempt, se.RetryAfter)
		c.logf("request attempt=%d 失败 kind=%s status=%d 判定=重试 delay=%s (retry_after=%s) path=%s",
			attempt, se.Kind, se.StatusCode, delay, se.RetryAfter, req.Path)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return classifyCtxErr("Do", ctx.Err())
		case <-timer.C:
		}
	}
	return lastErr
}

// doOnce 执行单次尝试，返回归一化错误（nil 表示成功）。
func (c *Client) doOnce(ctx context.Context, req Request, body []byte, out any) error {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, c.BaseURL+req.Path, bytes.NewReader(body))
	if err != nil {
		return &Error{Kind: KindTransport, Op: "Do", Err: fmt.Errorf("build request: %w", err)}
	}
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	if c.UserAgent != "" {
		httpReq.Header.Set("User-Agent", c.UserAgent)
	}
	// 附带有效凭证（必要时触发/等待刷新）。
	if c.Tokens != nil {
		tok, err := c.Tokens.Token(ctx)
		if err != nil {
			return err
		}
		httpReq.Header.Set("Authorization", "Bearer "+tok.Value)
	}

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		// 上下文结束优先归类为超时/取消，不与传输层错误混淆。
		if ctxErr := ctx.Err(); ctxErr != nil {
			return classifyCtxErr("Do", ctxErr)
		}
		// 底层库异常归一化为 KindTransport，不直接透出。
		return &Error{Kind: KindTransport, Op: "Do", Err: errors.New("transport failure: " + err.Error())}
	}
	defer resp.Body.Close()

	requestID := resp.Header.Get("X-Request-Id")
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// 非成功响应：保留请求标识与 Retry-After 提示。
		return &Error{
			Kind:       KindStatus,
			Op:         "Do",
			RequestID:  requestID,
			StatusCode: resp.StatusCode,
			RetryAfter: retryAfterFrom(resp.Header),
			Err:        fmt.Errorf("unexpected status %d", resp.StatusCode),
		}
	}
	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return &Error{Kind: KindTransport, Op: "Do", RequestID: requestID, Err: errors.New("read body: " + err.Error())}
	}
	if err := json.Unmarshal(data, out); err != nil {
		// 响应体不合法。
		return &Error{Kind: KindDecode, Op: "Do", RequestID: requestID, Err: fmt.Errorf("decode response: %w", err)}
	}
	return nil
}

func (c *Client) logf(format string, args ...any) {
	if c.Retry.Logger != nil {
		c.Retry.Logger(format, args...)
	}
}

// encodeBody 将请求体编码为 JSON 字节流。
func encodeBody(v any) (io.Reader, []byte, error) {
	if v == nil {
		return nil, nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, nil, err
	}
	return bytes.NewReader(b), b, nil
}
