# ADR-0005：同实例 reconnect、管理动作台账与验收边界

- 状态：已接受
- 日期：2026-08-06

## 决策

### 同实例 reconnect

Hermes 可能复用同一个 OneBot adapter 实例调用 `connect(is_reconnect=True)`。因此：

- `QueueStore` 在断开后可以 reopen 同一路径 SQLite，并更换 owner id；
- disconnect 先取消背景任务和 heartbeat，再把当前 owner 的 lease 原子结算；
- 尚未开始非幂等出站的 lease 回到 `pending`，已开始或阶段未知的 lease 进入 `uncertain`；
- 正常主动断开不增加失败次数；只有过期且明确处于 `agent_running` 的 lease 才按 2/4/8 秒退避消耗最多 3 次恢复预算，达到上限进入 `failed`；
- dispatcher 的活动 lease、debounce、judging 和 engaged 状态只存在内存，reconnect 后清空并回到 idle；
- SQLite 消息、滚动摘要和显式 durable trigger request 保留并重新恢复；
- 旧 task 即使延迟取消，也必须被 lease fencing 拒绝新的工具和出站请求。

这样不会把短生命周期的活跃窗口伪装成可跨进程恢复的持久事实，也不会因为 adapter 复用而把 SQLite 或 dispatcher 留在 closed 状态。

### 管理动作 operation ledger

撤回、禁言、踢人和全员禁言等非幂等动作在访问 OneBot 前写入同一个队列 SQLite 的 operation ledger：

```text
started -> succeeded/known_failed
started --崩溃或响应未知--> unknown
unknown --管理员 retry--> retry_armed
unknown --管理员 discard--> discarded
retry_armed --新的预览+确认--> started
```

遗留的 `started` 在进程恢复时一律变成 `unknown`。同一 fingerprint 的 `unknown` 动作禁止直接再次调用。`retry` 只解除阻断，不自动重放；管理员仍需新的预览和确认。审计只记录 operation id、fingerprint 摘要、工具、目标、状态和脱敏参数摘要，不记录确认 token、完整参数或媒体 URL。

### CI 与本地 Hermes 验收边界

CI 只保证插件自身可以在干净环境安装、`onebot11/` 纯协议和状态机测试通过、Ruff 无错误。CI 不嵌入完整 Hermes 源码、真实 session store 或 provider。

adapter 注册、hooks、工具并集、shared session key、strict auxiliary、reconnect 和临时 `HERMES_HOME` 由 `scripts/verify_hermes_integration.ps1` 在本地可复现运行；真实 LLBot/NapCat 联调另行记录实际行为，不把 WS 重放升级为 OneBot 11 协议保证。

## 原因

OneBot 11 没有非幂等请求的通用幂等键。断线后无法区分请求未到达、已经执行但响应丢失或仍在执行。把 lease 阶段和管理动作状态落到同一个 SQLite，比内存集合更能在重启后保持安全默认，同时不引入第二套数据库和后台服务。

同理，CI 中复制完整 Hermes 会让插件发布门禁变重且难以稳定；纯插件门禁和可复现 Hermes 集成验收分层，能明确知道每个绿灯到底证明了什么。

## 影响

- reconnect 后可能需要重新仲裁候选消息，连续对话的短期状态会丢失，这是有意取舍；
- `unknown` 管理动作需要管理员人工判断目标端是否已经执行；
- 本地 Hermes 集成需要调用者提供 Hermes 源码和其依赖环境；
- 原始 WS 进入 SQLite 前仍不承诺可靠不丢失，输入整体保持至少一次语义。
