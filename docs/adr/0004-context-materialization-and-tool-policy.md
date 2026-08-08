# ADR-0004：当前 batch 上下文物化与精确工具权限

- 状态：Accepted
- 日期：2026-08-06
- 关联：Task 4 / Issue #6

## 背景

OneBot 群消息先进入持久队列，再由共享 Hermes session 处理。旧实现把已确认消息追加到 SQLite 滚动摘要，并在下一轮把摘要和新消息一起注入 synthetic user turn。这样旧消息既存在 Hermes session 历史，又可能从 SQLite 摘要再次出现。与此同时，Hermes 的默认工具 schema 对普通 OneBot 用户过宽，单独只限制 `qq_*` 工具不能阻断网页、终端、文件、MCP 和 delegation。

## 决策

1. Hermes session 历史是跨轮上下文的唯一主载体。当前 queue batch 的早期消息摘要和最近原文组成一个 user turn，并在成功路径进入历史；ack 不再更新跨轮 SQLite 摘要。
2. 队列消息允许至少一次处理；不尝试在 SQLite ack 与 Hermes transcript 之间实现 exactly-once。崩溃窗口的极端重复由后续人工或上下文压缩处理，正常成功路径不得重复注入。
3. 权限角色固定为 `user`、`trusted_user`、`super_admin`。角色工具集合是精确工具名列表，schema 使用所有角色许可工具并集，执行时使用 TurnAnchor 首次 claim 时持久化的不可变权限快照；白名单、目标、lease 和 adapter 关闭仍可立即 fencing。QueueStore schema v10 之前的 pending anchor 可以首次 claim 后绑定，已有 active/uncertain/failed 且缺少快照的记录进入 hold。
4. OneBot 角色暂时禁止 `delegate_task`、`tool_search`、`tool_describe`、`tool_call`；在 Hermes 提供 per-turn `allowed_tool_names`、tool search 过滤和子 Agent 权限继承前，不伪造传递性授权。tool-search 目录可见不等于当前 turn 获得工具执行权限。
5. authority reminder 通过 `pre_llm_call` 追加到当前 user request；Hermes 保存 wire sidecar 并保持 system prompt 稳定。动态时间等易变信息仍应使用 request-only provider hook，插件不修改 Hermes 安装目录。
6. 群 `/context` 等只读命令在 adapter 入站层旁路处理；会话重置、模型切换和压缩命令拒绝进入群 Agent。

## 后果

- provider 前缀缓存可以稳定复用 Hermes session 历史；每轮只新增当前 batch user turn。
- SQLite `summary` 列保留用于旧 schema 兼容和诊断，但不再作为新 Agent turn 的历史来源。
- 新增或收紧权限不改变正在运行 turn 的 immutable snapshot；统一从下一 TurnAnchor 生效。
- 旧 Hermes 版本无法完成动态 request-only 上下文和宿主级 per-turn 工具过滤。该缺口已显式记录为上游接口任务；缺少 `session_id`/`turn_id` 的桥接调用必须拒绝，不能靠插件 hook 返回值猜测宿主行为。
- 子代理暂不从 OneBot caller 继承权限；Docker 子代理的隔离、共享目录和结果边界另开任务。

## 未选方案

- 不把所有历史消息复制进每次 user turn：会破坏缓存并快速消耗输入预算。
- 不继续维护一个会跨轮增长的摘要：它和 session transcript 有重复所有权。
- 不把时间等易变字段放进 `pre_llm_call`；authority reminder 刻意属于该 turn 的可重放 wire sidecar。
- 不在本 Task 引入 Docker 子代理或原始 WS spool；它们分别属于后续隔离任务和既有持久化边界。
