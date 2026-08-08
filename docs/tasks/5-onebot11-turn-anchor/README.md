# Task 5：OneBot 11 TurnAnchor、authority 与 followup

关联：[Issue #9](https://github.com/notnotype/hermes-plugin-onebot11/issues/9)

## 目标

把群队列从“一个 trigger 消费整批 pending”改为“每个锚点一个独立 followup”，确保不同用户的任务不共享 authority，同时保留群 shared Hermes session。

## 已实现

- QueueStore schema v9：anchor seq/kind/batch 边界、anchor 级失败退避、queued/processing reaction。
- 精确触发在入队事务创建 message anchor；自动 evaluator 只选择一个现存 seq。
- 每 anchor claim 固定消息范围；后续消息不扩大旧 batch；blocking anchor 阻止越序。
- message/operator/legacy authority 合同；v8 无法证明的旧状态进入 legacy hold。
- 当前 batch 使用结构化 JSONL，包含消息 ID、发送者、role、reply 和媒体/segment 标记。
- turn-start immutable 权限快照和 authority reminder；写工具按锚点角色直接执行。
- `⏳ -> 👀 -> cleared` reaction 生命周期，恢复只重试 unset。
- `/onebot flush` 创建 operator anchor；resolve 保留原 anchor，缺失 authority 的旧记录只释放为未锚定 pending。
- dispatcher 持久转换失败时保留 heartbeat；单轮恢复异常不终止恢复循环。

## 与原计划的差异

- 旧 `require_mention=false`/`always` 不再直接创建用户 authority；它们只能调度自动 selector。未配置 selector 时消息留在 SQLite。
- cooldown 不再直接创建 trigger；只延迟 selector。精确触发绕过 cooldown。
- 删除 confirmation token。角色、目标、lease 和工具门禁通过后，写工具在当前 turn 直接执行；unknown 仍进入人工 resolve。
- 当前 Hermes 的 `pre_llm_call` 已是 user-request sidecar，并保留 wire bytes 用于历史重放；无需修改 Hermes 或使用不存在的 provider hook实现 authority reminder。

## 验证

- 本地完整 Hermes 环境：`242 passed`。
- 临时 `HERMES_HOME` smoke 已完成：插件版本 `0.4.0`、12 个工具、4 个 hooks、`onebot11_trigger` auxiliary、schema v9 和 shared session 配置均已真实注册。
- `ruff check .`：通过。
- 覆盖多锚点串行、batch 边界、immutable authority、自动选择器严格输出、v8/v9 迁移、reaction 阶段、shutdown fencing、unknown 写操作和群 slash。

## 外部验收边界

Arch 联调只允许群 `1072992996` 与私聊用户 `2056963663`。2026-08-08 已在 `192.168.1.18` 部署 commit `f8c14ac` / v0.4.0，并验证 schema v9 迁移、两个独立 @ anchor、普通用户权限拒绝、允许 DM、非白名单拒绝和 reaction recovery。两个 anchor 使用真实反向 WS 的合成 payload；未执行禁言、踢人、撤回、全员禁言或 unknown 管理动作。

## 后续

- Docker 子代理及传递性权限另行设计。
- OneBot 12、语义摘要、原始 WS spool 和复杂管理后台不属于本 Task。
