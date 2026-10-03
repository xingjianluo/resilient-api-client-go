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
  - `resume.go` — 可断点续传的遍历：可持久化 `Checkpoint`、工作量上限 `Walker`、
    可区分的结束原因与异常游标收敛
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
| `checkpoint` | 续传标记无法解析或版本不兼容 | 否 |
| `cursor_anomaly` | 游标序列异常（停滞 / 重复成环 / 绕回 / 连续无进展） | 否 |

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

## 断点续传遍历（Walker / Checkpoint）

面向批量同步/导出这类“一次跑不完、进程可能重启”的任务，`resume.go` 在
`Iterator` 之上提供 `Walker`：

```go
cfg := sdk.WalkConfig[YourItem]{
    Fetch:  fetchPage,                 // func(ctx, cursor) (Page[T], error)
    Key:    func(i YourItem) string { return i.ID },
    Limits: sdk.WalkLimits{MaxDuration: 5 * time.Minute, MaxPages: 1000, MaxItems: 100000},
    Logger: func(format string, args ...any) { log.Printf(format, args...) },
}
w := sdk.NewWalker(cfg)
res, err := w.Run(ctx)
// 任意时刻（包括出错时）都能从 res.Checkpoint 拿到当前进度：
raw, _ := res.Checkpoint.Marshal()    // JSON，可落盘、可投递到队列/传给新进程
```

下一个进程从标记恢复，已处理过的内容不会再产出：

```go
cp, err := sdk.UnmarshalCheckpoint(raw)
w, err := sdk.ResumeWalker(cfg, cp)
res, err = w.Run(ctx)                 // 接着拉剩下的数据
```

### 续传语义（不丢、不重、必然停）

- **不重**：`Checkpoint.SeenKeys` 保存已处理元素去重键的**全集**。恢复后，
  因服务端增删导致跨页漂移、或页内达到条数上限而重拉同一页时，旧元素都会被
  seen 集合过滤，只产出新元素。
- **不丢（页内截断）**：`MaxItems` 在一页内部达到时，游标**保持在本页起点**，
  下次续传重拉该页并靠 seen 去重，本页剩余元素不会丢。
- **游标异常自动收敛**，统一归类为 `cursor_anomaly`，可用 `errors.Is` 细分：
  - `ErrCursorStalled`：`next_cursor` 与当前游标相同（停滞）；
  - `ErrCursorLoop`：下一个游标在最近用过的游标中出现（重复成环 / 绕回）。
    最近 64 个游标（`RecentCursors`）会随标记持久化，因此**跨进程恢复后**
    服务端把游标绕回也能检出；
  - `ErrNoProgress`：连续 `MaxStalePages`（默认 3）页没有任何新元素
    （游标虽在变但不再有进展，例如服务端数据反复抖动），主动停下，不无端空跑。
- 服务端数据**新增**的元素在后续遍历中正常产出（如游标允许到达）；
  **删除**已处理元素不会造成重复。偏移式分页固有的“跳过未读元素”风险仍在，
  对完整性要求极高的任务应在服务端使用稳定游标点（而非裸 offset）。

### 续传标记里有什么、什么能长期保存

`Checkpoint` 是带 `version` 的普通 JSON：

| 字段 | 含义 | 长期保存 |
|---|---|---|
| `version` | 标记结构版本（当前 `1`） | 是 |
| `cursor` | 下一次要请求的**服务端不透明游标** | 是，但语义由服务端保证；服务端游标若会过期需评估有效期 |
| `seen_keys` | 已处理元素去重键全集，去重的正确性依据 | 是；体积随已处理元素数线性增长，可按业务键裁剪/分桶 |
| `recent_cursors` | 最近最多 64 个游标，用于跨进程检测绕回/成环 | 是；缺失只会降低跨进程绕回检出能力，不影响基本续传 |
| `pages` / `items` | 累计页数 / 条数，仅用于观测与告警 | 是，不参与上限判定 |

标记**不含**凭证、请求头或连接信息——这些由 `Client`/`TokenProvider` 在恢复后
重新提供，因此可以安全落盘或跨进程传递（仍建议按业务敏感度加密/控制访问）。

### 版本兼容

- `UnmarshalCheckpoint` / `ResumeWalker` 只接受当前主版本；版本缺失或版本号
  更高（新版本写入、旧 SDK 读取）时返回 `checkpoint` 错误且
  `errors.Is(err, sdk.ErrCheckpointVersion)` 为真。
- 遇到不兼容版本，调用方应放弃该标记并**从头全量遍历**（seen 为空，不会产生
  重复之外的副作用），或由应用层做迁移；不要用不兼容的游标盲目续传。
- 未来新增可选字段会在同一主版本内向后兼容；不兼容变更会提升 `version`。

### 工作量上限与结束原因怎么区分

`WalkLimits` 的三个上限**只约束单次 `Run`**（历史累计计数不会让恢复后的运行
立即触顶）；正常收敛时 `err == nil` 且 `res.Reason` 明确：

| `EndReason` | 触发条件 |
|---|---|
| `completed` | 服务端 `next_cursor` 为空，全部拉完 |
| `limit_max_duration` | 本次运行时长达到 `MaxDuration`（在发起下一页请求前判定） |
| `limit_max_pages` | 本次运行拉取页数达到 `MaxPages` |
| `limit_max_items` | 本次运行产出条数达到 `MaxItems`（含页内命中，游标保留） |

以下情况**不是**正常收敛，返回非 nil 的 `*sdk.Error`，`res.Reason` 为空，
四类彼此不会被互相冒充；即使出错，返回的 `res.Checkpoint` 仍可用于续传：

| 场景 | `KindOf(err)` |
|---|---|
| 调用方主动取消 `context` | `canceled` |
| 请求 / 运行超时 | `timeout` |
| 限流配额在时限内始终不可得 | `quota_exhausted` |
| 游标停滞 / 成环 / 绕回 / 连续无进展 | `cursor_anomaly` |
| 传输失败、非成功状态码、解码失败、坏标记 | `transport` / `status` / `decode` / `checkpoint` |

### 并发任务

`Walker` 是**单任务有状态对象**：多个同步任务各自 `NewWalker`、各自保存
`Checkpoint`、各自设置上限，彼此不共享 seen / 游标 / 结束判定，可以共用同一个
`*Client`（连接池、限流器、`TokenProvider` 全部共享）。凭证过期时的刷新仍是
singleflight——并发任务增多不会触发重复刷新，限流也按共享令牌桶统一约束，
不会凭空放大请求。

## 本地验证方法

```bash
go build ./...        # 编译
go test ./...         # 全部测试
go test -race ./...   # 竞态检测
go test ./sdk/ -run 'TestWalker|TestCheckpoint' -v          # 续传/上限/异常游标单测
go test ./integration/ -run 'TestResumable|TestConcurrentShared|TestResumeUnder|TestWalkerQuota' -v
go test ./integration/ -v   # 查看请求参数、重试次数与判定依据日志
```

带日志运行时，Walker 会逐页打印请求参数（页码、游标、各项上限与当前进度）、
去重命中（哪个 key、依据什么跳过）、以及结束判定依据（completed / 命中哪项
上限 / fetch 返回的错误类别），最终归类可直接在日志中对照。

覆盖场景：正常调用与分页遍历、503 重试成功、非幂等请求不重试、
超时/取消/配额耗尽三者可区分、解码错误与请求标识保留、
服务端 429+Retry-After 退避重试、并发凭证刷新仅执行一次、
传输层错误归一化、分页数据变动去重与游标停滞终止；
以及续传标记 JSON 往返与跨进程恢复、已处理内容不重复、
MaxDuration/MaxPages/MaxItems 分批正常收敛与续传、
上限收敛与取消/超时/配额耗尽互不混淆、
游标停滞/成环/跨进程绕回/连续无进展自动终止、标记版本不兼容拒绝、
并发任务共用客户端时数据与结束原因隔离且凭证不重复刷新、
分批导出期间服务端增删数据后续传不重不丢。
