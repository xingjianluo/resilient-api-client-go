package resilientclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"
)

// logf 输出带前缀的调试日志（默认丢弃；测试中通过 WithLogger 接入 t.Logf）。
type logFunc func(format string, args ...any)

// Client 是可复用的远程 API 客户端：在同一个执行路径内统一处理
// 限流获取、凭证注入与失效刷新、幂等重试退避、错误归一化与请求标识保留。
type Client struct {
	baseURL    string
	httpClient *http.Client
	limiter    Limiter
	retry      RetryPolicy
	tokens     *CachedTokenProvider
	attemptTO  time.Duration
	log        logFunc
	userAgent  string
}

// Option 配置 Client。
type Option func(*Client)

// WithHTTPClient 注入自定义 *http.Client（如自定义 Transport）。
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }

// WithLimiter 注入限流器。
func WithLimiter(l Limiter) Option { return func(c *Client) { c.limiter = l } }

// WithRetryPolicy 设置重试/退避策略。
func WithRetryPolicy(p RetryPolicy) Option { return func(c *Client) { c.retry = p } }

// WithTokenProvider 注入已构造的凭证缓存提供者。
func WithTokenProvider(p *CachedTokenProvider) Option { return func(c *Client) { c.tokens = p } }

// WithTokenSource 直接注入令牌换取函数，客户端会自动包一层带缓存的提供者。
func WithTokenSource(src TokenSource) Option {
	return func(c *Client) { c.tokens = NewCachedTokenProvider(src, 5*time.Second) }
}

// WithAttemptTimeout 设置单次尝试超时（整体截止仍由调用方 context 决定）。
func WithAttemptTimeout(d time.Duration) Option { return func(c *Client) { c.attemptTO = d } }

// WithLogger 注入日志函数，用于打印请求参数、重试次数与判定依据。
func WithLogger(f func(string, ...any)) Option { return func(c *Client) { c.log = logFunc(f) } }

// WithUserAgent 设置 User-Agent 头。
func WithUserAgent(ua string) Option { return func(c *Client) { c.userAgent = ua } }

// New 创建客户端。
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{},
		retry:      DefaultRetryPolicy(),
		log:        func(string, ...any) {},
		userAgent:  "resilient-api-client-go/1.0",
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) op(method, path string) string { return method + " " + path }

// Do 发送一次请求，返回状态码、响应体与服务端请求标识（X-Request-Id）。
//
// 成功路径：HTTP 状态码为 2xx 时返回 (status, body, requestID, nil)。
// 失败路径：error 恒为 *APIError，Kind 可区分；详见包内 Kind 常量。
//
// 重试规则：仅幂等方法、且失败类别可恢复时重试；退避指数+全抖动，
// 并尊重服务端 Retry-After（有上限）。调用方取消立即返回、绝不重试。
func (c *Client) Do(ctx context.Context, method, path string, body io.Reader) (int, []byte, string, error) {
	op := c.op(method, path)

	var bodyBytes []byte
	if body != nil {
		b, err := io.ReadAll(body)
		if err != nil {
			return 0, nil, "", newAPIError(KindTransport, op, err, "failed to read request body")
		}
		bodyBytes = b
	}

	idempotent := isIdempotentMethod(method)
	maxAttempts := 1
	if idempotent {
		maxAttempts = c.retry.MaxAttempts
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr *APIError
	authRefreshed := false // 本轮是否已因 401 强制刷新过凭证
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			// 调用方主动取消 / 整体截止：立即返回，且原因互不掩盖。
			return 0, nil, "", newAPIError(classifyContextErr(err), op, err,
				fmt.Sprintf("context ended before attempt %d", attempt))
		}

		// 限流获取放在每次尝试之前：被限流拒绝不消耗服务端调用。
		if c.limiter != nil {
			if err := c.limiter.Wait(ctx); err != nil {
				return 0, nil, "", err
			}
		}

		status, respBody, reqID, retryAfter, err := c.doOnce(ctx, method, path, bodyBytes, attempt)
		if err == nil {
			if status >= 200 && status < 300 {
				c.log("attempt=%d/%d %s -> success status=%d request_id=%s",
					attempt, maxAttempts, op, status, reqID)
				return status, respBody, reqID, nil
			}
			// 非成功响应：归一化为 KindStatus，保留请求标识。
			apiErr := newAPIError(KindStatus, op, nil,
				fmt.Sprintf("server returned non-success status %d, body=%s", status, truncate(respBody, 256)))
			apiErr.StatusCode = status
			apiErr.RequestID = reqID
			apiErr.retryAfter = retryAfter
			lastErr = apiErr
			if status == http.StatusUnauthorized && c.tokens != nil {
				// 凭证过期/失效：丢弃缓存，强制下一次尝试刷新（仅幂等方法会走到重试）。
				c.tokens.Invalidate()
				c.log("attempt=%d/%d %s -> 401, credential invalidated, will refresh request_id=%s",
					attempt, maxAttempts, op, reqID)
			} else if !retryableStatus(status) {
				c.log("attempt=%d/%d %s -> non-retryable status=%d request_id=%s",
					attempt, maxAttempts, op, status, reqID)
				return status, respBody, reqID, apiErr
			}
		} else {
			lastErr = err
			// 凭证刷新失败：不重试，避免对授权方造成放大。
			if err.Kind == KindCredential {
				return 0, nil, err.RequestID, err
			}
			// 取消类原因（取消/超时）：超时允许重试，取消不重试。
			if err.Kind == KindCanceled {
				c.log("attempt=%d/%d %s -> canceled, stop retrying: %v", attempt, maxAttempts, op, err)
				return 0, nil, err.RequestID, err
			}
		}

		if attempt == maxAttempts {
			c.log("attempt=%d/%d %s -> exhausted retries: %v", attempt, maxAttempts, op, lastErr)
			break
		}
		// 401 后的凭证刷新重试不等待退避（每轮仅一次，避免无限循环）。
		if lastErr.Kind == KindStatus && lastErr.StatusCode == http.StatusUnauthorized && !authRefreshed {
			authRefreshed = true
			c.log("attempt=%d/%d %s -> refreshing credential, immediate retry request_id=%s",
				attempt, maxAttempts, op, lastErr.RequestID)
			continue
		}

		// 退避：指数+全抖动；若失败响应带有 Retry-After 则服务端提示优先（有上限）。
		var hint time.Duration
		if h, ok := lastErr.retryHint(); ok {
			hint = h
		}
		wait := c.retry.backoff(attempt, hint)
		c.log("attempt=%d/%d %s -> recoverable failure (%v), retry=%d backing off %s request_id=%s",
			attempt, maxAttempts, op, lastErr.Kind, attempt+1, wait, lastErr.RequestID)

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, nil, lastErr.RequestID, newAPIError(classifyContextErr(ctx.Err()), op, ctx.Err(),
				fmt.Sprintf("context ended during backoff after attempt %d", attempt))
		case <-timer.C:
		}
	}
	if lastErr.Kind == KindStatus {
		return lastErr.StatusCode, nil, lastErr.RequestID, lastErr
	}
	return 0, nil, lastErr.RequestID, lastErr
}

// doOnce 执行单次尝试：构造请求、取凭证、发送、读取并归一化结果。
// 返回值依次为：状态码、响应体、请求标识、Retry-After 提示、归一化错误。
func (c *Client) doOnce(ctx context.Context, method, path string, body []byte, attempt int) (int, []byte, string, time.Duration, *APIError) {
	op := c.op(method, path)

	attemptCtx := ctx
	var cancel context.CancelFunc
	if c.attemptTO > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, c.attemptTO)
		defer cancel()
	}

	url := c.baseURL + path
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(attemptCtx, method, url, bodyReader)
	if err != nil {
		return 0, nil, "", 0, newAPIError(KindTransport, op, err, "failed to build request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("X-Request-Id", newClientRequestID(attempt))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if c.tokens != nil {
		token, tokErr := c.tokens.Token(ctx) // 取凭证使用调用方 context，不受尝试超时裁剪
		if tokErr != nil {
			return 0, nil, "", 0, asAPIError(tokErr, op)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	c.log("sending attempt=%d method=%s path=%s body_bytes=%d", attempt, method, path, len(body))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, "", 0, normalizeTransportErr(err, op)
	}
	defer resp.Body.Close()

	reqID := resp.Header.Get("X-Request-Id")
	retryAfter, _ := parseRetryAfter(resp.Header)
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return resp.StatusCode, nil, reqID, retryAfter, newAPIError(KindTransport, op, readErr, "failed to read response body")
	}
	return resp.StatusCode, respBody, reqID, retryAfter, nil
}

// GetJSON 是便捷封装：GET 请求并将成功响应解码到 out。
// 响应体不是合法 JSON 时返回 KindDecode（即使状态码为 2xx）。
func (c *Client) GetJSON(ctx context.Context, path string, out any) (string, error) {
	status, body, reqID, err := c.Do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return reqID, err
	}
	if err := json.Unmarshal(body, out); err != nil {
		apiErr := newAPIError(KindDecode, c.op(http.MethodGet, path), err,
			"response body is not valid JSON: "+truncate(body, 128))
		apiErr.StatusCode = status
		apiErr.RequestID = reqID
		return reqID, apiErr
	}
	return reqID, nil
}

// normalizeTransportErr 将 net/http 与 context 的底层错误映射为可区分的归一化错误。
func normalizeTransportErr(err error, op string) *APIError {
	switch {
	case errors.Is(err, context.Canceled):
		return newAPIError(KindCanceled, op, err, "request canceled while in flight")
	case errors.Is(err, context.DeadlineExceeded):
		return newAPIError(KindTimeout, op, err, "request deadline exceeded while in flight")
	case isNetTimeout(err):
		return newAPIError(KindTimeout, op, err, "network timeout")
	default:
		return newAPIError(KindTransport, op, err, "transport error")
	}
}

func truncate(b []byte, n int) string {
	s := string(b)
	if len(s) > n {
		return s[:n] + "...(truncated)"
	}
	return s
}

func newClientRequestID(attempt int) string {
	return fmt.Sprintf("cli-%d-%d", time.Now().UnixNano(), rand.IntN(1<<30))
}
