// Package mockapi 提供进程内模拟服务端（net/http/httptest），
// 用于在本地复现真实远程 API：令牌签发与过期、脚本化失败、Retry-After
// 限流提示、慢响应以及数据可变的游标分页。
package mockapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Item 是模拟资源。
type Item struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// RequestRecord 记录服务端收到的一次请求，供测试断言与日志核对。
type RequestRecord struct {
	Time      time.Time
	Method    string
	Path      string
	Auth      string
	RequestID string
}

type failSpec struct {
	remaining  int
	status     int
	retryAfter time.Duration // >0 时写入 Retry-After
}

type issuedToken struct {
	value     string
	expiresAt time.Time
}

// Server 是进程内模拟服务端。
type Server struct {
	ts *httptest.Server

	mu          sync.Mutex
	items       []Item
	failures    map[string]*failSpec // 按精确路径配置脚本化失败
	records     []RequestRecord
	tokens      map[string]issuedToken
	tokenTTL    time.Duration
	requireAuth bool

	tokenHits  int64
	requestSeq int64
}

// Option 配置模拟服务端。
type Option func(*Server)

// WithInitialItems 设置初始数据集。
func WithInitialItems(items ...Item) Option {
	return func(s *Server) { s.items = append([]Item(nil), items...) }
}

// WithTokenTTL 设置签发令牌的有效期；0 表示长期有效。
func WithTokenTTL(d time.Duration) Option {
	return func(s *Server) { s.tokenTTL = d }
}

// WithNoAuth 关闭鉴权校验。
func WithNoAuth() Option {
	return func(s *Server) { s.requireAuth = false }
}

// New 创建并启动模拟服务端。
func New(opts ...Option) *Server {
	s := &Server{
		failures:    map[string]*failSpec{},
		tokens:      map[string]issuedToken{},
		tokenTTL:    time.Hour,
		requireAuth: true,
	}
	for _, o := range opts {
		o(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/token", s.handleToken)
	mux.HandleFunc("/v1/items", s.handleItems)
	mux.HandleFunc("/v1/items/", s.handleItemByID)
	mux.HandleFunc("/v1/echo", s.handleEcho)
	mux.HandleFunc("/v1/slow", s.handleSlow)
	mux.HandleFunc("/v1/badjson", s.handleBadJSON)
	s.ts = httptest.NewServer(mux)
	return s
}

// URL 返回模拟服务端的基础地址。
func (s *Server) URL() string { return s.ts.URL }

// Close 关闭模拟服务端。
func (s *Server) Close() { s.ts.Close() }

// Fail 令指定路径接下来 n 次请求返回 status；retryAfter>0 时附带 Retry-After。
func (s *Server) Fail(path string, n, status int, retryAfter time.Duration) {
	s.mu.Lock()
	s.failures[path] = &failSpec{remaining: n, status: status, retryAfter: retryAfter}
	s.mu.Unlock()
}

// AddItem 在数据集中追加一个条目（模拟遍历期间数据变动）。
func (s *Server) AddItem(it Item) {
	s.mu.Lock()
	s.items = append(s.items, it)
	s.mu.Unlock()
}

// RemoveItem 按 ID 删除条目（不存在则无操作）。
func (s *Server) RemoveItem(id string) {
	s.mu.Lock()
	for i, it := range s.items {
		if it.ID == id {
			s.items = append(s.items[:i], s.items[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

// TokenHits 返回令牌端点被调用次数（用于验证 single-flight）。
func (s *Server) TokenHits() int { return int(atomic.LoadInt64(&s.tokenHits)) }

// Hits 返回指定路径被命中的次数。
func (s *Server) Hits(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.records {
		if r.Path == path {
			n++
		}
	}
	return n
}

// Requests 返回请求记录快照。
func (s *Server) Requests() []RequestRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RequestRecord, len(s.records))
	copy(out, s.records)
	return out
}

func (s *Server) nextRequestID() string {
	n := atomic.AddInt64(&s.requestSeq, 1)
	return fmt.Sprintf("mock-%d-%d", time.Now().UnixNano(), n)
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, code, msg, reqID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":       code,
			"message":    msg,
			"request_id": reqID,
		},
	})
}

// record 记录请求并补上响应头与脚本化失败；返回 false 表示已写出失败响应。
func (s *Server) record(w http.ResponseWriter, r *http.Request) (string, bool) {
	reqID := r.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = s.nextRequestID()
	}
	w.Header().Set("X-Request-Id", reqID)

	auth := r.Header.Get("Authorization")
	s.mu.Lock()
	s.records = append(s.records, RequestRecord{
		Time: time.Now(), Method: r.Method, Path: r.URL.Path,
		Auth: auth, RequestID: reqID,
	})
	f := s.failures[r.URL.Path]
	if f != nil && f.remaining > 0 {
		f.remaining--
		status, ra := f.status, f.retryAfter
		if f.remaining == 0 {
			delete(s.failures, r.URL.Path)
		}
		s.mu.Unlock()
		if ra > 0 {
			// delta-seconds 形式按秒向上取整，保证短于 1s 的提示不被截断为 0。
			w.Header().Set("Retry-After", fmt.Sprintf("%d", (ra+time.Second-1)/time.Second))
		}
		s.writeError(w, r, status, http.StatusText(status),
			"scripted failure for "+r.URL.Path, reqID)
		return reqID, false
	}
	s.mu.Unlock()
	return reqID, true
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "POST required", s.nextRequestID())
		return
	}
	atomic.AddInt64(&s.tokenHits, 1)
	reqID, ok := s.record(w, r)
	if !ok {
		return
	}
	n := atomic.LoadInt64(&s.tokenHits)
	value := fmt.Sprintf("tok-%d", n)
	resp := map[string]any{
		"access_token": value,
		"token_type":   "Bearer",
	}
	s.mu.Lock()
	ttl := s.tokenTTL
	if ttl > 0 {
		resp["expires_in"] = int64(ttl.Seconds())
	}
	s.tokens[value] = issuedToken{value: value, expiresAt: time.Now().Add(ttl)}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
	_ = reqID
}

func (s *Server) checkAuth(w http.ResponseWriter, r *http.Request, reqID string) bool {
	if !s.requireAuth {
		return true
	}
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		s.writeError(w, r, http.StatusUnauthorized, "missing_token", "authorization required", reqID)
		return false
	}
	value := strings.TrimPrefix(h, prefix)
	s.mu.Lock()
	t, ok := s.tokens[value]
	s.mu.Unlock()
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "invalid_token", "token not recognized", reqID)
		return false
	}
	if !t.expiresAt.IsZero() && time.Now().After(t.expiresAt) {
		s.writeError(w, r, http.StatusUnauthorized, "token_expired", "token has expired", reqID)
		return false
	}
	return true
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("off:%d", offset)))
}

func decodeCursor(c string) (int, bool) {
	if c == "" {
		return 0, true
	}
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, false
	}
	var off int
	if _, err := fmt.Sscanf(string(b), "off:%d", &off); err != nil {
		return 0, false
	}
	return off, true
}

// listResponse 是分页列表信封。
type listResponse struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func (s *Server) handleItems(w http.ResponseWriter, r *http.Request) {
	reqID, ok := s.record(w, r)
	if !ok {
		return
	}
	if !s.checkAuth(w, r, reqID) {
		return
	}
	limit := 2
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := fmtAtoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	offset, valid := decodeCursor(r.URL.Query().Get("cursor"))
	if !valid || offset < 0 {
		s.writeError(w, r, http.StatusBadRequest, "invalid_cursor", "cursor cannot be decoded", reqID)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	resp := listResponse{Items: []Item{}}
	if offset < len(s.items) {
		end := offset + limit
		if end > len(s.items) {
			end = len(s.items)
		}
		resp.Items = append(resp.Items, s.items[offset:end]...)
		if end < len(s.items) {
			resp.NextCursor = encodeCursor(end)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleItemByID(w http.ResponseWriter, r *http.Request) {
	reqID, ok := s.record(w, r)
	if !ok {
		return
	}
	if !s.checkAuth(w, r, reqID) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/items/")
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range s.items {
		if it.ID == id {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(it)
			return
		}
	}
	s.writeError(w, r, http.StatusNotFound, "not_found", "item "+id+" not found", reqID)
}

func (s *Server) handleEcho(w http.ResponseWriter, r *http.Request) {
	reqID, ok := s.record(w, r)
	if !ok {
		return
	}
	if !s.checkAuth(w, r, reqID) {
		return
	}
	var body map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"received": body})
}

func (s *Server) handleSlow(w http.ResponseWriter, r *http.Request) {
	reqID, ok := s.record(w, r)
	if !ok {
		return
	}
	delay := 200 * time.Millisecond
	if v := r.URL.Query().Get("ms"); v != "" {
		if n, err := fmtAtoi(v); err == nil {
			delay = time.Duration(n) * time.Millisecond
		}
	}
	select {
	case <-time.After(delay):
	case <-r.Context().Done(): // 客户端取消/超时，请求上下文结束
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "request_id": reqID})
}

func (s *Server) handleBadJSON(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.record(w, r); !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte("{not-valid-json"))
}

func fmtAtoi(v string) (int, error) {
	var n int
	_, err := fmt.Sscan(v, &n)
	return n, err
}
