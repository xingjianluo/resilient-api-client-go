// Package mockserver 在本地进程内模拟真实远程 API 服务端，
// 复现限流（429+Retry-After）、可恢复故障（503）、分页、
// 凭证签发与校验、请求标识（X-Request-Id）等行为。
package mockserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Server 是进程内模拟服务端。
type Server struct {
	mu sync.Mutex

	items        []Item
	tokenTTL     int64 // 秒
	validTokens  map[string]int64
	tokenCounter int
	refreshCount int
	// failNextN 让接下来 N 次数据请求返回 503（模拟可恢复故障）。
	failNextN int
	// RateLimit 为每秒允许的请求数，超限返回 429 + Retry-After。
	RateLimit float64
	tokens    float64
	lastFill  time.Time

	reqCounter atomic.Int64
	// Delay 让数据端点在处理前睡眠（模拟慢响应，用于超时测试）。
	Delay time.Duration
	srv        *httptest.Server
}

// Item 是服务端数据元素。
type Item struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// New 创建并启动模拟服务端。
func New(items []Item, tokenTTL int64) *Server {
	s := &Server{
		items:       items,
		tokenTTL:    tokenTTL,
		validTokens: map[string]int64{},
		RateLimit:   0, // 默认不限流
		lastFill:    time.Now(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", s.handleToken)
	mux.HandleFunc("GET /items", s.handleItems)
	mux.HandleFunc("GET /bad", s.handleBad)
	s.srv = httptest.NewServer(mux)
	return s
}

// URL 返回服务端地址。
func (s *Server) URL() string { return s.srv.URL }

// Close 关闭服务端。
func (s *Server) Close() { s.srv.Close() }

// RefreshCount 返回实际执行的凭证刷新次数。
func (s *Server) RefreshCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshCount
}

// TotalRequests 返回数据端点（/items）收到的请求总数，
// 用于验证重试/并发场景下请求没有被凭空放大。
func (s *Server) TotalRequests() int64 {
	return s.reqCounter.Load()
}

// FailNext 设置接下来 n 次数据请求返回 503。
func (s *Server) FailNext(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNextN = n
}

// AddItem 在遍历期间追加数据（模拟数据变动）。
func (s *Server) AddItem(it Item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, it)
}

// RemoveFirst 删除首个元素（模拟数据变动）。
func (s *Server) RemoveFirst() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) > 0 {
		s.items = s.items[1:]
	}
}

// handleToken 签发凭证：POST /token -> {token, expires_at}。
func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.tokenCounter++
	s.refreshCount++
	tok := "token-" + strconv.Itoa(s.tokenCounter)
	exp := time.Now().Unix() + s.tokenTTL
	s.validTokens[tok] = exp
	s.mu.Unlock()

	s.writeJSON(w, http.StatusOK, map[string]any{
		"token":      tok,
		"expires_at": exp,
	})
}

// handleItems 分页返回数据：GET /items?cursor=<offset>&limit=<n>。
// 需要有效 Bearer 凭证，否则 401；可能按配置返回 503/429。
func (s *Server) handleItems(w http.ResponseWriter, r *http.Request) {
	if s.Delay > 0 {
		time.Sleep(s.Delay)
	}
	reqID := "req-" + strconv.FormatInt(s.reqCounter.Add(1), 10)
	w.Header().Set("X-Request-Id", reqID)

	// 凭证校验。
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(auth) <= len(prefix) || auth[:len(prefix)] != prefix {
		s.writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "missing token"})
		return
	}
	tok := auth[len(prefix):]
	s.mu.Lock()
	exp, ok := s.validTokens[tok]
	valid := ok && time.Now().Unix() < exp
	s.mu.Unlock()
	if !valid {
		s.writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid or expired token"})
		return
	}

	// 模拟可恢复故障。
	s.mu.Lock()
	if s.failNextN > 0 {
		s.failNextN--
		s.mu.Unlock()
		w.Header().Set("Retry-After", "1")
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "temporary failure"})
		return
	}
	s.mu.Unlock()

	// 服务端限流：每秒 RateLimit 个请求，超限 429 + Retry-After。
	if !s.allow() {
		w.Header().Set("Retry-After", "1")
		s.writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "rate limited"})
		return
	}

	// 分页：cursor 为偏移量。
	q := r.URL.Query()
	cursor, _ := strconv.Atoi(q.Get("cursor"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 10
	}
	s.mu.Lock()
	total := len(s.items)
	end := cursor + limit
	if end > total {
		end = total
	}
	var page []Item
	if cursor < total {
		page = append(page, s.items[cursor:end]...)
	}
	s.mu.Unlock()

	next := ""
	if end < total {
		next = strconv.Itoa(end)
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"items":       page,
		"next_cursor": next,
	})
}

// allow 是服务端侧的简单令牌桶。
func (s *Server) allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.RateLimit <= 0 {
		return true
	}
	now := time.Now()
	elapsed := now.Sub(s.lastFill).Seconds()
	s.tokens += elapsed * s.RateLimit
	if s.tokens > s.RateLimit {
		s.tokens = s.RateLimit
	}
	s.lastFill = now
	if s.tokens < 1 {
		return false
	}
	s.tokens--
	return true
}

// handleBad 始终返回 200 但响应体不是合法 JSON（用于解码错误测试）。
func (s *Server) handleBad(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Request-Id", "req-bad")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{not-json"))
}

func (s *Server) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
