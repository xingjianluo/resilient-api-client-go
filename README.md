# resilient-api-client-go

一个可复用的远程 API 客户端 SDK。配套的 `mockapi` 子包提供**进程内模拟服务端**
（`net/http/httptest`，无需外部依赖），用于在本地真实地复现限流、重试、分页与
凭证刷新行为，使这些能力在 SDK 的各个入口（`Do` / `GetJSON` / `Iterator`）下
行为一致、错误原因可区分、判定过程可解释。

## 快速开始

```go
srv := mockapi.New(
    mockapi.WithTokenTTL(800*time.Millisecond),
    mockapi.WithInitialItems(mockapi.Item{ID: "a", Name: "alpha"} /* ... */),
)
defer srv.Close()

client := resilient.New(srv.URL(),
    resilient.WithTokenSource(myTokenSource),
    resilient.WithRetryPolicy(resilient.DefaultRetryPolicy()),
    resilient.WithLimiter(resilient.NewTokenBucket(50, 10, 2*time.Second)),
    resilient.WithAttemptTimeout(3*time.Second),
    resilient.WithLogger(func(format string, args ...any) { /* 接 t.Logf/zap/log */ }),
)

var item resilient.Item
requestID, err := client.GetJSON(ctx, "/v1/items/a", &item)
```

完整可运行示例见 `cmd/demo`：

```bash
go run ./cmd/demo
```

## 错误分类

底层 `net/http`、`encoding/json`、`context` 的异常**不会直接透出**，一律归一化为
`*resilient.APIError`，包含字段：

| 字段 | 含义 |
| --- | --- |
| `Kind` | 可区分的错误类别（见下表） |
| `Op` | 逻辑操作，如 `GET /v1/items/1` |
| `StatusCode` | 非成功响应的 HTTP 状态码（仅 `KindStatus`） |
| `RequestID` | 服务端返回的请求标识（`X-Request-Id`），便于排查 |
| `Message` / `Cause` | 可读说明与被包装的底层错误（可继续 `errors.Is/As`） |

| `ErrorKind` | 触发场景 | 哨兵判定 |
| --- | --- | --- |
| `KindTransport` | 连接被拒、连接重置、读响应体失败等传输层错误 | — |
| `KindStatus` | 服务端返回非 2xx 状态码（含 429、5xx、4xx、401） | — |
| `KindDecode` | 响应体不是合法 JSON（**即使状态码为 2xx**） | — |
| `KindTimeout` | 整体 `context` 截止、单次尝试超时、网络超时 | `errors.Is(err, ErrTimeout)` |
| `KindCanceled` | 调用方主动 `cancel()` | `errors.Is(err, ErrCanceled)` |
| `KindQuota` | 本地限流令牌的预计等待超出容忍上限，被立即拒绝 | `errors.Is(err, ErrQuota)` |
| `KindCredential` | 凭证换取/刷新失败（结果不缓存，后续会重试） | `errors.Is(err, ErrCredential)` |
| `KindPagination` | 游标成环或服务端返回已访问过的游标 | — |

**三种“结束类”原因互不掩盖**：超时（`ErrTimeout`）、调用方取消（`ErrCanceled`）、
配额耗尽（`ErrQuota`）分别由不同分支产生，`errors.Is` 可精确区分；
取消永远立即返回，不会被包装成超时或传输错误。

## 重试与退避规则

- **仅幂等方法重试**：`GET / HEAD / OPTIONS / PUT / DELETE / TRACE` 才允许在
  可恢复失败后重试；`POST / PATCH` 等非幂等请求即使遇到 503 也**绝不重试**，
  避免重复副作用（对应 `isIdempotentMethod`）。
- **可恢复失败**：传输层错误、尝试级超时，以及 `429 / 500 / 502 / 503 / 504`。
  `404` 等其他状态码、调用方取消、凭证刷新失败均不重试。
- **退避**：指数退避 `BaseDelay * 2^(attempt-1)` 并叠加 `[0.5, 1)` 全抖动防惊群，
  以 `MaxDelay` 封顶（`retry.go: RetryPolicy.backoff`）。
- **尊重服务端提示**：响应带 `Retry-After`（支持 `delta-seconds` 与 `HTTP-date`）
  且其值大于本地计算值时，以服务端为准；服务端提示受 `RetryAfterCap` 封顶，
  防止异常/恶意的超长提示拖死客户端。
- **401 凭证失效**：幂等请求丢弃缓存凭证后**立即重试一次**（不走退避，且每轮仅一次，
  避免刷新风暴）；非幂等请求直接返回 401。
- **取消语义**：退避等待期间 `ctx` 结束会立即返回对应的 `KindCanceled/KindTimeout`，
  不会继续重试。
- **限流前置**：每次尝试（含重试）前先取令牌；被限流拒绝的请求不会打到服务端。

默认策略（`DefaultRetryPolicy`）：最多 4 次尝试、基准 100ms、退避上限 2s、
Retry-After 上限 5s。可用 `NoRetry()` 关闭重试。

### 限流器

`NewTokenBucket(ratePerSec, burst, maxWait)` 为本地令牌桶：

- 初始满桶，允许启动突发；按固定速率补充，桶容量为 `burst`。
- 令牌不足时按精确时长等待；预计等待超过 `maxWait` 立即返回 `KindQuota`，
  不会长时间占住调用方。
- 等待期间 `ctx` 截止/取消，分别返回 `KindTimeout` / `KindCanceled`。
- `ratePerSec <= 0` 表示不限流。

## 凭证刷新语义

`CachedTokenProvider`（`credentials.go`）提供带提前量（`earlyRefreshSkew`）的
凭证缓存，并保证：

1. **single-flight**：凭证缺失或过期的瞬间，无论多少调用方并发触发，
   实际向授权方发起的刷新只有一次，其余调用方共享同一次结果。
2. **等待方互不阻塞、不放大请求**：刷新使用独立 `context`，某个等待方提前超时
   或取消只让自己返回，既不会取消刷新，也不会触发新的刷新（有测试覆盖）。
3. **失败不缓存**：刷新失败归为 `KindCredential`，下一次调用会重新尝试。
4. **失效丢弃旧结果**：`Invalidate()` 自增版本号；刷新期间发生失效时，
   晚到的旧结果不会写回缓存。401 响应会自动触发该路径。

## 分页遍历语义

`Client.IterItems()` 返回游标分页迭代器（`pagination.go`）：

- **必然终止**：空 `next_cursor` 表示结束；一旦服务端返回与当前相同、
  或此前已访问过的非空游标（游标成环），立即以 `KindPagination` 终止，
  即使服务端有缺陷也不会死循环。
- **结果不重复**：按条目 ID 跨页去重。遍历期间数据增删是安全的——
  已见 ID 只产出一次；游标快照语义下，已遍历位置之后新增的数据可能不可见，
  被删除的数据安全跳过。
- 分页请求本身同样享受幂等重试（如首页瞬时 500 会透明重试）与错误归一化。

## 进程内模拟服务端 `mockapi`

| 端点 | 用途 |
| --- | --- |
| `POST /v1/token` | 签发不透明 Bearer 令牌，支持可配置 TTL（过期后受保护端点返回 401） |
| `GET /v1/items?cursor=&limit=` | 游标分页列表（默认每页 2 条），支持中途 `AddItem/RemoveItem` |
| `GET /v1/items/{id}` | 单资源，不存在返回 404 |
| `POST /v1/echo` | 非幂等回显，用于验证 POST 不重试 |
| `GET /v1/slow?ms=` | 慢响应，尊重客户端取消，用于超时/取消测试 |
| `GET /v1/badjson` | 返回非法 JSON 体，用于 `KindDecode` 测试 |

控制与观测方法：`Fail(path, n, status, retryAfter)`（脚本化前 n 次失败并可附带
`Retry-After`）、`AddItem/RemoveItem`、`TokenHits()`、`Hits(path)`、`Requests()`。
所有响应都带 `X-Request-Id`（请求未自带时服务端生成）。

## 本地验证方法

```bash
# 1. 构建
go build ./...

# 2. 全部单元测试（-race 检测数据竞争）
go test ./... -race -count=1 -v

# 3. 运行端到端演示（模拟：503 重试退避、分页中途增删、凭证过期刷新）
go run ./cmd/demo
```

测试使用 `WithLogger(t.Logf)` 接入日志，`go test -v` 时会打印每个请求的
**请求参数、尝试/重试次数与判定依据**（如“幂等 GET + 503 可恢复 → 退避重试”、
“POST 非幂等 → 不重试”、“ctx 截止 → KindTimeout”）。

### 测试覆盖一览

| 测试 | 覆盖点 |
| --- | --- |
| `TestDoSuccess` | 2xx 成功、JSON 解码、请求标识保留 |
| `TestRetryIdempotentRecovers` / `TestRetryExhausted` | 幂等重试、次数耗尽仍为可区分错误 |
| `TestRetryRespectsRetryAfter` | 退避尊重服务端 `Retry-After` |
| `TestNoRetryForNonIdempotent` / `TestNonRetryableStatus` | POST 不重试、404 不重试 |
| `TestTransportFailure` / `TestDecodeFailure` | 传输错误、非法响应体归一化 |
| `TestTimeoutDistinctFromCancel` / `TestCallerCanceled` / `TestAttemptTimeout` | 超时与取消可区分、取消不重试 |
| `TestQuotaExhaustedRejected` / `TestLimiterWaitCanceled` | 配额耗尽立即拒绝、等待中取消 |
| `TestPagination*` | 正常/空集/变动去重/游标成环/分页瞬时失败重试 |
| `TestCredentialSingleFlight` 等 | 并发单次刷新、缓存、过期自动刷新、等待方独立、失败可再试、POST 401 不重试 |
| `TestConcurrent*` | 并发压力下结果一致、请求不放大、独立截止时间 |

## 目录结构

```
.
├── client.go               # 核心客户端：限流→取凭证→请求→重试退避→错误归一化
├── errors.go               # *APIError、ErrorKind、哨兵错误、context 原因分类
├── ratelimit.go            # 令牌桶 Limiter
├── retry.go                # 幂等判定、可恢复状态、指数退避+全抖动、Retry-After
├── credentials.go          # 带缓存的 single-flight 凭证提供者
├── pagination.go           # 游标分页迭代器（去重 + 成环检测）
├── mockapi/server.go       # 进程内模拟服务端
├── cmd/demo/main.go        # 端到端本地演示
└── *_test.go               # 单元/并发/边界测试
```
