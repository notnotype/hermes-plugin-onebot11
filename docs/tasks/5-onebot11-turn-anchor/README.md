# Task 5：OneBot 11 TurnAnchor、authority 与 followup

关联：[Issue #9](https://github.com/notnotype/hermes-plugin-onebot11/issues/9)

## 目标

把群队列从“一个 trigger 消费整批 pending”改为“每个锚点一个独立 followup”，确保不同用户的任务不共享 authority，同时保留群 shared Hermes session。

## 已实现

- QueueStore schema v9：anchor seq/kind/batch 边界、anchor 级失败退避、queued/processing reaction。
- 精确触发在入队事务创建 message anchor；自动 evaluator 只选择一个现存 seq。
- 每 anchor claim 固定消息范围；后续消息不扩大旧 batch；blocking anchor 阻止越序。
- message/operator/legacy authority 合同；v8 无法证明的旧状态进入 legacy hold。
- 当前 batch 使用结构化 JSONL，包含 `message_id`、稳定 `message_key`、发送者、role、reply 和媒体/segment 标记；没有真实 OneBot ID 时不伪造 `message_id`。
- turn-start immutable 权限快照和 authority reminder；写工具按锚点角色直接执行。
- `⏳ -> 👀 -> cleared` reaction 生命周期，恢复只重试 unset。
- `/onebot flush` 创建 operator anchor；`/onebot resolve retry` 为可验证的 message/operator anchor 创建新的 request id，保留原 authority、消息范围和 reaction 状态；legacy hold 不能 retry。
- dispatcher 持久转换失败时保留 heartbeat；单轮恢复异常不终止恢复循环。
- 过期 agent-running lease 会增加失败次数并按 2/4/8 秒退避，达到上限后进入 `failed`；出站阶段未知进入 `uncertain`，不自动重放。
- 每个回复块都重新执行 adapter、白名单和 lease fencing；块间失效时不发送后续块，部分成功进入 `uncertain`。
- 注册插件时要求 Hermes 提供 `pre_gateway_dispatch`、`pre_llm_call`、`pre_tool_call`；缺少关键 hook 直接拒绝启用。
- role catalog 只帮助模型理解 `user`、`trusted_user`、`super_admin` 的工具集合，真正授权仍来自 anchor binding、lease 和 handler 硬校验。

## 与原计划的差异

- 旧 `require_mention=false`/`always` 不再直接创建用户 authority；它们只能调度自动 selector。未配置 selector 时消息留在 SQLite。
- cooldown 不再直接创建 trigger；只延迟 selector。精确触发绕过 cooldown。
- 删除 confirmation token。角色、目标、lease 和工具门禁通过后，写工具在当前 turn 直接执行；unknown 仍进入人工 resolve。
- 当前 Hermes 的 `pre_llm_call` 是 user-request sidecar，并保留 wire bytes 用于历史重放；动态时间等 request-only 内容仍等待 Hermes 的 `pre_provider_request`。未来 Hermes 若为系统错误通知提供 `hermes_system_error_notice=true`，插件会避免把该通知计入业务 outbound marker；在此之前保持保守的 `uncertain`。

## 验证

- 当前本地完整环境：`256 passed`，使用 Hermes 源码和 site-packages 导入真实 gateway；没有因缺少 Hermes 依赖跳过 adapter 测试。
- `ruff check .`：通过；`compileall` 和 `git diff --check` 通过。
- 真实 Hermes `PluginManager` 临时注册 smoke：通过，OneBot 平台、12 个工具、4 个 hooks 和 `onebot11_trigger` auxiliary 均注册成功。
- 覆盖崩溃恢复退避上限、phase fencing、resolve 新 anchor、reaction 状态迁移、分块出站 fencing、binding mismatch、hash message key、HTTP redirect，以及 selector 实际观察游标和失败竞态。
- Arch 当前 HEAD `87b8dbd` 已部署到独立 worktree，并完成白名单范围内合成 WS/LLBot smoke；未执行破坏性群管理写操作。

## 外部验收边界

Arch 联调只允许群 `1072992996` 与私聊用户 `2056963663`。当前 `87b8dbd` 已通过真实反向 WS 的合成 payload 验证：普通用户和超级管理员各创建一个独立 message anchor，分别得到 `TASK5_ANCHOR_A/B`；允许私聊得到 `TASK5_DM`；收尾后 queue、trigger、reaction 均为空，gateway 保持 active。合成 payload 的伪造 message ID 不能验证 reaction；另用真实群消息 ID `2119419776` 完成 `set=true -> set=false`。未执行禁言、踢人、撤回、全员禁言、unknown 管理动作或非白名单目标事件；合成 payload 不等同于真人 QQ 客户端协议保证。

## 后续

- Docker 子代理及传递性权限另行设计。
- OneBot 12、语义摘要、原始 WS spool 和复杂管理后台不属于本 Task。
