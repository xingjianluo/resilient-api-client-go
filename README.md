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
  - `checkpoint.go` — 可落盘、可跨进程传递的续传标记（带版本号）
  - `walker.go` — 批量遍历器：断点续传 + 工作量上限 + 游标异常收敛
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

## 断点续传与工作量上限（批量同步/导出）

面向大批量任务（一次跑不完、进程会被重启）的入口是 `sdk.Walker`
（`walker.go`）。旧的 `Iterator.Collect` 仍然保留，语义不变。

### 基本用法

```go
cfg := sdk.WalkerConfig[Item]{
    Key:    func(i Item) string { return i.ID },
    Limits: sdk.Limits{MaxDuration: 5 * time.Minute, MaxPages: 2000, MaxItems: 100000},
    Logger: logf, // 可选：每次请求参数、判定依据、结束归类
}
w := sdk.NewWalker(fetchPage, cfg)          // fetchPage 与 Iterator 的签名一致
for {
    r := w.Run(ctx)                          // 每个批次处理 r.Items
    save(r.Checkpoint.Marshal())             // 结束标记落盘
    if r.Reason != sdk.EndLimitTime && r.Reason != sdk.EndLimitPages && r.Reason != sdk.EndLimitItems {
        break                                // 完成或出错，停止调度
    }
    // 否则下一进程/下一批次从标记恢复：
    cp, _ := sdk.UnmarshalCheckpoint(saved)
    w = sdk.ResumeWalker(fetchPage, cp, cfgWithFreshBudget)
}
```

### 续传语义

1. `Run` 返回的 `WalkResult.Checkpoint` 是**任意结束时刻**（包括到上限、
   取消、超时、报错）都可导出的独立副本，`Marshal()` 成 JSON 后可落盘、
   传消息队列、跨进程/跨机器恢复；`Walker.Checkpoint()` 也可在运行中随时取快照。
2. **已处理内容不会重复出现**：标记里保存全部已处理元素的去重键（`seen`）。
   续传期间服务端数据增删导致元素跨页漂移时，历史元素靠 `seen` 过滤；
   偏移式分页模型下头部新增/删除可能使个别新增元素被跳过（与旧 `Iterator`
   的保证一致），SDK 保证的是**不重复 + 可终止**，不是强一致快照。
3. 游标以“已访问序列”（`visited_cursors`）记账。游标回到任何历史游标
   （重复、绕回，包括跨进程绕回）立即报 `ErrCursorLoop` 收敛停止；
   `next_cursor == cursor` 停滞报 `ErrPaginationStuck`；游标在前进但连续
   `MaxEmptyPages`（默认 10）页没有任何新元素，报 `ErrPaginationNoProgress`。
   三类异常都归为 `EndCursorAnomaly`——**不会卡死，也不会没完没了地跑**。
4. 完成是终态：`next_cursor` 为空后标记 `done=true`，之后再 `Run` 立即
   返回 `completed` 且不发任何请求。

### 续传标记里有什么、能存多久

`Checkpoint` 只含遍历协议状态，**不含凭证、连接或任何进程内对象**，
可长期保存：

| 字段 | 含义 | 备注 |
|---|---|---|
| `version` | 格式版本（当前 `1`） | 版本兼容的依据 |
| `cursor` | 下一页游标 | 恢复后的起点 |
| `done` | 是否到终态 | 终态幂等 |
| `seen` | 已处理元素键集合 | 不重复的保证，规模≈已处理元素数 |
| `visited_cursors` | 已访问游标序列 | 绕回检测，规模≈已拉页数 |
| `empty_streak` | 连续空页计数 | 跨进程保留的空转状态 |
| `pages_done` / `items_done` | 跨进程累计页数/去重条数 | 观测与进度展示 |

版本兼容规则：`UnmarshalCheckpoint` 遇到**更高**版本号返回
`ErrCheckpointUnsupportedVersion`（包装为 `KindDecode`），拒绝静默错续；
缺 `version` 字段的旧标记按当前版本宽容接受；未知字段一律忽略
（后续版本只新增可选字段即可向后/向前兼容）。注意 `seen` 与
`visited_cursors` 随已处理规模线性增长，属预期开销，请整体压缩/加密存储。

### 工作量上限与结束原因

`Limits` 是**每次 `Run` 独立计算**的预算（多次 `Run` 会重新计时、重新计页）：

- `MaxDuration`：墙钟时间预算；`MaxPages`：本次最多页数；
  `MaxItems`：本次最多产出条数（**硬上限**：页内逐条判定，到上限立即
  截断，单次 `Run` 产出绝不超限，哪怕服务端一页返回的条数比上限还多）；
  `MaxEmptyPages`：空转收敛阈值。
- 上限检查都在发起下一页请求之前，到上限**不发多余请求**。

**`MaxItems` 硬上限与续传补齐**：到上限的那一页如果在页中间被截断，
游标**不推进**（仍指向本页），已产出元素的 key 已记入标记的 `seen`。
凭该标记续传（可跨进程，经 `Checkpoint.Marshal` 落盘）时会重拉本页、
自动跳过已拿部分，把页内剩余和后续内容一起补齐——剩余一条不少、
已拿的一条不重。即使被截断的页 `next_cursor` 为空（最后一页），
只要页内还有未取元素，结束原因仍是 `limit_items` 而非 `completed`；
仅当上限恰好等于剩余全集且末页已完整取尽时，才归类 `completed`
（数据确已拿完，无需续传）。

结束原因 `WalkResult.Reason` 互斥且可程序判定，**上限收敛不是错误**
（`Err` 为 nil，且标记可直接续传）：

| Reason | 含义 | 是错误？ | 可否直接续传 |
|---|---|---|---|
| `completed` | 服务端遍历完成（终态） | 否 | 无需续传 |
| `limit_time` / `limit_pages` / `limit_items` | 到达工作量上限，正常收敛 | 否 | 是 |
| `canceled` | 调用方主动取消（`context` canceled） | 是（`KindCanceled`） | 是，可重试本批 |
| `timeout` | 调用方 deadline 到期（请求超时） | 是（`KindTimeout`） | 是 |
| `quota_exhausted` | 限流配额在时限内不可得 | 是（`KindQuotaExhausted`） | 是 |
| `fetch_error` | 拉取失败（传输/状态/解码，见 `Err`/`KindOf`） | 是 | 是（游标未推进） |
| `cursor_anomaly` | 游标停滞/绕回/连续空页，已收敛 | 是 | 否，需人工排查服务端 |

到上限、取消、超时、配额耗尽四类**严格区分、互不掩盖**：时间上限用
`Walker` 自己的墙钟预算判定；`context` 结束优先按 deadline/cancel 归类；
fetch 返回的错误沿用 SDK 既有的错误归一化（`KindOf`）。`limit_items`
与 `completed` 同样互斥：判定依据是“是否还有没取到的元素”——页内截断
（含最后一页内截断）一律 `limit_items`；只有服务端 `next_cursor` 为空
且末页元素全部入账才是 `completed`。

### 并发任务共用一个客户端

- **每个同步任务独立 `NewWalker`**：续传标记、`seen`、上限预算都是实例内
  状态，任务之间不串数据、不共享结束原因。同一个 `Walker` 不得并发 `Run`
  （多个任务请各建一个）。
- **`Client` / 限流器 / `TokenProvider` 是共享且并发安全的**：多任务并发
  不会导致凭证重复刷新（singleflight + 缓存复用），也不会凭空放大请求
  （上限判定在发请求前；仅真实数据请求计入）。

## 本地验证方法

```bash
go build ./...        # 编译
go test ./...         # 全部测试
go test -race ./...   # 竞态检测
go test ./sdk/ -run 'TestCheckpoint|TestWalker' -v          # 续传/上限/异常游标/并发单测
go test ./integration/ -run TestWalker -v                  # 端到端：请求参数、重试、判定依据、结束归类
go test ./integration/ -v   # 查看请求参数、重试次数与判定依据日志
```

覆盖场景：正常调用与分页遍历、503 重试成功、非幂等请求不重试、
超时/取消/配额耗尽三者可区分、解码错误与请求标识保留、
服务端 429+Retry-After 退避重试、并发凭证刷新仅执行一次、
传输层错误归一化、分页数据变动去重与游标停滞终止；
以及续传标记序列化与版本兼容、跨进程续传零重复、三类工作量上限正常收敛、
`MaxItems` 硬上限（上限小于单页、上限落在最后一页中间、到上限后跨进程
续传补齐且零重复、多任务共用客户端各设不同条数上限互不串）、
到上限/取消/超时/配额耗尽四类严格区分、游标停滞/绕回/连续空页收敛停止、
多任务并发共用客户端时标记与结束原因隔离、凭证只刷新一次且请求不放大。
