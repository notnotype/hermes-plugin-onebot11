# OneBot 11 上下文物化与细粒度权限

- 关联 issue：[#6](https://github.com/notnotype/hermes-plugin-onebot11/issues/6)
- 状态：本地实现、Arch + LLBot 指定白名单联调完成，待 PR 收口
- 开始日期：2026-08-06

## 用户需求

群共享 session 要能吃到稳定的历史缓存，但不能反复注入已经进入 session 的队列摘要；普通用户不能默认使用网页、浏览器、终端、文件和 MCP 等高风险工具；群聊需要能旁路执行 `/context` 一类只读命令。受信用户按精确工具名配置，权限配置可以由 Hermes 管理员工具修改。

## 目标

- 当前群 queue batch 由早期消息摘要 + 最近原文组成一个 synthetic user turn，并进入 Hermes session 历史。
- ack 后不再把同一 batch 写进跨轮 SQLite 摘要，正常路径不重复注入。
- 角色固定为 `user`、`trusted_user`、`super_admin`，精确工具名在 `pre_tool_call` 和 handler 两层校验。
- 权限配置只修改 `platforms.onebot11.extra.roles`，写入预览后由 `/onebot confirm` 完成。
- 群 `/context`、`/status`、`/whoami`、`/help`、`/commands` 不进入 queue/session；危险 slash command 直接拒绝。
- Docker 子代理和 Hermes provider/tool policy 上游接口有明确后续记录，不在本 Task 中伪造完成。

## 执行过程

1. 从本地 `master` `a45e244` 创建 `feat/i6-t4-context-permissions` worktree，避免混入 trigger spike 和 Task 2 worktree。
2. 将 `QueueLease.summary` 改为当前 lease 早期消息摘要；`QueueStore.ack()` 不再更新跨轮 `onebot_queue_chat.summary`。
3. 新增确定性 batch context 和 request-only dynamic context helper，使用 UTF-8 字节预算与最近原文保留策略。
4. 扩展角色解析、trusted QQ 白名单、精确工具名校验和全量 `pre_tool_call` 门禁；权限收紧在下一次工具调用生效。
5. 新增 `onebot_get_permissions`、`onebot_set_role_tools`、`onebot_set_trusted_users`；写配置通过 Hermes 原子 YAML writer 只修改 roles 子树。
6. 在 adapter 入站层增加群只读 slash command 和危险命令拒绝。
7. 记录 Hermes 当前版本没有 `pre_provider_request`；插件仅在宿主公开该 hook 时注册动态 request copy 适配器。
8. 在 Arch `192.168.1.18` 部署 `0.3.0`，用真实反向 WS/HTTP 链路完成指定群、指定私聊、shared session、slash、reaction、权限拒绝和白名单负向验证。

## 变更文件

- `onebot11/context.py`：batch 摘要、最近原文和动态 request context。
- `onebot11/permissions.py`：三角色、精确工具名、trusted 用户和权限合同。
- `onebot11/queue.py`：当前 batch 摘要物化，停止 ack 后滚动摘要追加。
- `onebot11/tools.py`：权限配置工具 schema。
- `onebot11/__init__.py`：导出新增纯协议 API。
- `adapter.py`：角色/配置工具、全量工具 hook、provider hook 兼容点和群 slash command。
- `tests/test_context.py`、`tests/test_permissions.py`、`tests/test_queue.py`、`tests/test_adapter.py`、`tests/test_tools.py`：回归矩阵。
- `docs/permissions.md`、`README.md`、`docs/adr/0004-context-materialization-and-tool-policy.md`：合同同步。

## 验证结果

- 纯插件环境：`pytest -q` -> `101 passed, 1 skipped`；跳过项是没有 Hermes `gateway` 依赖的 adapter 集成测试。
- 纯插件环境：`ruff check .` 通过。
- 接入本机 Hermes 源码和依赖：`pytest -q` -> `149 passed`，adapter 集成测试不再因缺少 Hermes gateway 而跳过。
- 已覆盖 batch 重复注入、UTF-8 字节预算、最近原文、动态文本标记、角色优先级、wildcard 拒绝、通用 Hermes 工具门禁、权限配置写入保护、群 slash 旁路和危险命令拒绝。
- Hermes 临时 `HERMES_HOME` smoke 已完成：平台、12 个 OneBot 工具、4 个现有安全 hooks 和 `onebot11_trigger` auxiliary 均注册；当前 Hermes 没有 `pre_provider_request`，因此动态上下文仍是上游接口待办。
- Arch + LLBot 外部联调已完成：配置只允许群 `1072992996` 和私聊用户 `2056963663`；共享 key 为 `agent:main:onebot11:group:1072992996`，两个群用户计算结果一致。
- 外部链路已验证：`/context` 不进队列；`/status` 标注 `chat_type=group` 且不回传旧 summary；普通用户 `/whoami` 只有当前范围只读工具；普通用户调用 `terminal` 被 hook/handler 拒绝；群 turn 的 reaction 按 `set=true -> 回复 -> set=false` 完成；允许私聊正常回复；非白名单群和私聊只有 `access_denied` 审计且无出站。
- 外部消息使用真实反向 WS 的合成 OneBot payload，未冒充真人 QQ 客户端输入；群管理写操作预览/确认、unknown 出站人工 resolve 和 Hermes 上游接口仍未在本 Task 外部验证。

## 后续 TODO

- Hermes 上游：增加真正的 `pre_provider_request`，每次 provider request/retry 接收 request copy 并允许返回替换副本；增加 per-turn exact `allowed_tool_names`，并贯穿 tool search、`execute_code` 和 delegation 子 Agent。
- Docker 子代理：单独任务实现容器、共享目录、资源/网络限制、凭据隔离和结果大小限制。
- 真实联调：严格只使用群 `1072992996` 和私聊用户 `2056963663`，验证两角色共享 session、配置确认、群 slash 和权限拒绝。
