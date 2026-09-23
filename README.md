# resilient-api-client-go

可复用的远程 API 客户端 SDK，附带本地进程内模拟服务端（`mockserver`），
用于复现真实调用并验证限流、重试、分页与凭证刷新在各入口下行为一致且可解释。

## 结构

- `sdk/` — 客户端 SDK
  - `client.go` — 客户端入口 `Client.Do`：限流 → 附凭证 → 发送 → 错误归一化 → 按需重试
  - `errors.go` — 统一错误类型 `*sdk.Error` 与错误类别 `ErrorKind`
  - `retry.go` — 重试策略与指数退避
  - `ratelimit.go` — 客户端侧令牌桶限流器
  - `pagination.go` — 分页迭代器（去重 + 必然终止）
  - `credentials.go` — 凭证缓存与 singleflight 刷新
- `mockserver/` — 进程内模拟服务端（httptest），复现 429+Retry-After、503、
  分页、凭证签发/校验、`X-Request-Id` 等行为
- `integration/` — 基于 mockserver 的端到端测试

## 错误分类

所有错误统一为 `*sdk.Error`，底层库异常（如 `*url.Error`）不会直接透出。
用 `sdk.KindOf(err)` 区分类别，`sdk.RequestIDOf(err)` 提取服务端请求标识：

| Kind | 含义 | 可重试 |
|---|---|---|
| `transport` | 传输层失败（连接拒绝、DNS、读写中断） | 是（仅幂等请求） |
| `status` | 非 2xx 响应，保留 `StatusCode` 与 `RetryAfter` | 仅 429 / 5xx |
| `decode` | 响应体不是合法 JSON | 否 |
| `timeout` | 调用方 `context` 超时到期 | 否 |
| `canceled` | 调用方主动取消 | 否 |
| `quota_exhausted` | 限流配额在调用方时限内不可得 | 否 |

超时、取消与配额耗尽分别映射为 `timeout` / `canceled` / `quota_exhausted`，
互不掩盖；非成功响应始终保留服务端 `X-Request-Id` 以便排查。

## 重试与退避规则

1. **仅幂等请求可重试**：GET/HEAD/PUT/DELETE/OPTIONS；POST 等非幂等方法
   `MaxAttempts` 强制为 1，避免重复副作用。
2. **仅可恢复失败可重试**：`transport` 错误，以及 `status` 中的 429 与 5xx；
   其余 4xx 视为调用方问题，立即放弃。
3. **退避**：`delay = min(BaseDelay * Multiplier^(attempt-1), MaxDelay)`；
   服务端 `Retry-After`（秒数或 HTTP 日期）优先——取两者较大值后再封顶
   `MaxDelay`。
4. 退避等待期间上下文结束，立即返回 `timeout` / `canceled`，不再发起尝试。
5. 设置 `RetryPolicy.Logger` 后，每次尝试都会打印请求参数（方法、路径、
   请求体）、当前第几次重试与判定依据（重试/放弃及原因）。

## 凭证刷新语义

- `TokenProvider` 缓存凭证，`skew` 提前量内即将过期的凭证视为无效。
- 过期时并发触发的刷新是 **singleflight**：第一个调用方执行刷新，其余调用方
  等待同一结果——实际刷新只执行一次，不放大请求，等待者互不阻塞。
- 等待刷新的调用方上下文结束时独立返回 `timeout` / `canceled`，不影响刷新者。
- `RefreshCount` 记录实际刷新次数，便于测试与观测。

## 分页遍历语义

- 以游标（`next_cursor`）推进，`next_cursor` 为空即终止；
  游标不推进时返回 `ErrPaginationStuck`，**保证必然终止**（不死循环）。
- 按调用方提供的 `key` 函数去重：遍历期间数据变动（插入/删除导致元素跨页
  漂移）时**结果不重复**。注意：偏移式分页在删除场景下可能跳过元素，
  这是服务端分页模型的固有特性，SDK 保证的是不重复与终止性。

## 本地验证方法

```bash
go build ./...        # 编译
go test ./...         # 全部测试
go test -race ./...   # 竞态检测
go test ./integration/ -v   # 查看请求参数、重试次数与判定依据日志
```

覆盖场景：正常调用与分页遍历、503 重试成功、非幂等请求不重试、
超时/取消/配额耗尽三者可区分、解码错误与请求标识保留、
服务端 429+Retry-After 退避重试、并发凭证刷新仅执行一次、
传输层错误归一化、分页数据变动去重与游标停滞终止。
