# ADR-0003：lease fencing 与 WS 持久化边界

- 状态：已接受
- 日期：2026-08-05

## 决策

### Lease fencing

每个群 turn 由 SQLite lease 唯一认领。工具调用、OneBot 出站和完成 ack 前都检查 lease owner、lease id、状态和过期时间。非幂等出站前先持久化 `outbound_started` marker；marker 之后旧 turn 失去 lease 时不得继续访问 OneBot API，结果未知的 lease 进入 `uncertain`，不自动重放。

只有 SQLite 明确完成 ack/release/uncertain 状态转换，dispatcher 才会清理活动 turn 并尝试下一轮。这样插件崩溃、heartbeat 失败或两个进程同时恢复时，旧 turn 不能凭内存状态继续产生副作用。

### WS 可靠性边界

反向 WS 收到的事件只有在进入有界接收队列后才被本进程视为已接纳；队列满、事件处理异常或任务取消时允许关闭连接，让 OneBot 框架重连重放。SQLite 队列只保证事件完成规范化并成功持久化之后的去重和恢复。

本插件不增加原始 WS spool，因此持久化前的网络重连重放行为只作为实际框架的 best-effort 观察，不升级为 OneBot 11 协议保证，也不宣称输入 exactly-once。

确认写操作的 token 仍是 adapter 进程内存状态，重启会使 token 失效；管理动作的 `started/unknown/retry_armed/discarded` 状态则持久化在同一个队列 SQLite operation ledger 中。持久化台账不记录 token，审计也不记录完整参数或媒体 URL。

## 原因

OneBot 11 没有可供插件依赖的非幂等请求幂等键。网络断开时无法区分请求未到达、已执行但响应丢失或仍在执行；同样，进程在 SQLite 入队前崩溃时无法从本插件恢复原始事件。将可证明的状态范围写清楚，比伪造更强的可靠性承诺更安全，也避免引入独立 spool 的额外存储和恢复系统。

## 影响

输入消息是至少一次语义；非幂等出站是 unknown-safe 语义。管理员必须通过 `/onebot resolve retry|discard` 处理队列消息，或通过 `/onebot resolve action retry|discard OPERATION_ID` 处理管理动作台账。默认队列、审计和媒体目录写入 Hermes home，测试或部署可用显式路径隔离。
