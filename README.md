# grpc-stream-reset-retry-planner
规划 gRPC 流重置、重试窗口和消息幂等边界

## 概述

gRPC 双向流在连接重置（RST_STREAM / GOAWAY）后，客户端只能观察到部分消息的结果。
直接重放整个流会重复执行已提交的业务操作；只从最后收到的消息继续又会漏掉断线前
已发送但未确认的数据。本规划器根据流 ID、消息序号、累计确认、幂等键、deadline、
缓冲窗口、连接事件、重试策略和服务端提交证据，计算可安全重放的区间，并对每条
消息给出 `continue` / `replay` / `wait` / `fail` 决定。

## 运行

```sh
node --test   # 需要 Node.js >= 24.16.0
```

## 消息状态机

每条消息在断线时刻处于四种状态之一（`src/states.js`）：

- `unsent`：从未发送，可在新连接直接继续（`continue`）。
- `sent-unacked`：已发送但未确认，旧连接排空后可按原幂等键重放（`replay`）。
- `processed-response-lost`：服务端已处理但响应丢失，先等待提交证据或对账，
  不盲目重放（`wait`）。
- `committed`：已提交（累计确认或提交证据覆盖），跳过，继续下一条（`continue`）。

判定优先级：累计确认 > 提交证据（committed）> 处理证据（processed）> 是否已发送。

## 幂等边界假设

- 每条消息携带唯一幂等键，服务端对同一幂等键的去重窗口不短于客户端的重试窗口。
- 新连接不得在旧连接仍可能提交时并行重放同一幂等键：只要
  `now < oldConnection.mayStillCommitUntil`，相关消息一律 `wait`，
  待旧连接排空（RST 确认、GOAWAY drain 完成或证据到达）后才允许 `replay`。
- GOAWAY 的 `lastStreamId` 是服务端处理边界：流 ID 大于它的流从未被处理，
  可立即在新连接重放；小于等于它的流必须等待排空。
- `processed-response-lost` 的消息不允许直接重放，必须先通过提交证据或
  对账接口确认终态，避免依赖"重放返回缓存结果"这一非必需的服务端行为。

## 重试窗口假设

- 客户端重放缓冲窗口为 `bufferWindow` 条；序号低于 `maxSeq - bufferWindow + 1`
  的未确认消息已被驱逐，无法重放，判定为 `fail`（需上层对账补偿）。
- 重试次数达到 `retryPolicy.maxAttempts` 的消息判定为 `fail`。
- `now >= deadline` 时所有未提交消息判定为 `fail`（`deadline-exceeded`），
  已提交消息不受影响。
- 不可重试的 trailers 状态（如 CANCELLED、RESOURCE_EXHAUSTED、RST_STREAM CANCEL）
  使所有未提交消息立即 `fail`；UNAVAILABLE / INTERNAL 等可重试状态进入正常重放流程。

## 输出结构

`planRecovery(input)` 返回：

- `decisions`：每条消息的 `state`、`decision` 与 `reason`。
- `trailers`：每个连接事件解析出的 gRPC 状态、可重试性与动作。
- `deadline`：剩余预算与是否已超时。
- `migration`：GOAWAY 迁移决策（`wait-for-drain` 或 `replay-on-new-connection`）。
- `safeReplayWindow`：可安全重放的序号区间 `{ from, to, seqs }`。
