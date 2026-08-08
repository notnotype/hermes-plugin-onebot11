# PROJECT-STATUS

仓库级现状报告。实现变更通过分支和 PR 收口；本文只记录当前代码合同、验证证据和仍需外部联调的事项。

## 当前状态

- **阶段**：v0.4.0 TurnAnchor 实现位于 Issue #9 stacked 分支；PR #10 仍为 draft，当前收口代码尚未合并 master。当前 commit `87b8dbd` 已部署到 Arch 独立 worktree 并完成白名单范围内合成 WS/LLBot smoke。仍未执行破坏性群管理写操作。
- **核心合同**：群固定 shared session；每条锚点消息对应一个 authority 和一个串行 followup；自动 selector 只选 seq；非幂等 unknown 不自动重放。
- **本地验证**：当前环境 `256 passed`；使用 Hermes 源码和 site-packages 完成真实 adapter 集成测试，无跳过项。Ruff、compileall、diff 检查和临时 Hermes 注册 smoke 已通过；本次 Arch 重部署仍待完成。

## 模块状态

| 模块 | 状态 | 当前合同 |
|---|---|---|
| `onebot11/message.py` | 完成 | array/CQ 字符串解析，保留 text、媒体、reply、文件/语音/视频/转发/未知段标记 |
| `onebot11/events.py` | 完成 | message 事件归一化；自身回传过滤；notice/request/lifecycle 只做限长统计摘要 |
| `onebot11/ws_server.py` | 完成 | token、loopback 默认、有界接收队列、同 chat 顺序、全局 inflight、失败关闭连接促使上游重放 |
| `onebot11/http_api.py` | 完成 | 查询有限重试；发送/管理/reaction 写永不自动重试；有符号 message_id；超时、429/5xx、非 JSON、超大响应分类；媒体 SSRF/类型/大小限制 |
| `onebot11/queue.py` | 完成 | schema v9、TurnAnchor/batch 边界、过期 lease 退避/上限、anchor retry 新 request、queued/processing reaction、legacy hold |
| `onebot11/dispatch.py` | 完成 | 每群一个活动 anchor，串行 followup、heartbeat、恢复异常隔离和 fencing |
| `onebot11/triggers.py` | 完成 | 精确触发纯函数；自动 selector 严格选择一个现存 seq，不读取权限配置 |
| `onebot11/permissions.py` | 完成 | `CallerContext`、`ChatTarget`、精确 `(session_id, turn_id)` binding、角色工具并集、fail-closed |
| `onebot11/tools.py` | 完成 | 当前范围查询；真实写请求前 fencing；写工具按 anchor authority 直接执行，unknown 不自动重放 |
| `adapter.py` | 完成 | anchor/caller 绑定、role catalog、结构化上下文、authority reminder、⏳/👀、逐块 fencing、unknown-safe 出站 |
| 文档/ADR | 进行中 | ADR-0002/0005、Task 5 和项目状态已同步；PR #10 仍待 CI/审查与合并 |

## 验证证据

- 使用 Hermes 源码和 site-packages 加载真实 gateway，并在仓库 `.venv` 中运行：`256 passed`；覆盖 schema v9、多个独立 anchor、batch 边界、immutable authority、selector、双阶段 reaction、unknown 写操作、媒体端口边界、关闭 fencing、逐块出站和消息 key 合同。新增覆盖 selector 实际观察游标，以及失败后新消息清除 selector 退避。
- 当前 Arch + LLBot：服务加载 commit `87b8dbd` / v0.4.0；群 `1072992996` 和私聊 `2056963663` 是唯一允许目标。已验证两个独立 @ anchor、允许私聊回复、shared session 投影、队列/trigger/reaction 收尾；未执行破坏性群管理写操作。
- `ruff check .`：通过。
- 带 Hermes gateway 的临时 `HERMES_HOME` 真实注册 smoke 已通过：插件启用，注册 12 个工具、4 个 hooks、`onebot11_trigger` auxiliary 和 `onebot11` platform。
- Arch 当前部署 `87b8dbd`：在群 `1072992996` 中由普通用户和超级管理员各发送一条明确 @，生成两个独立 message anchor，并分别收到 `TASK5_ANCHOR_A/B`；允许私聊用户 `2056963663` 收到 `TASK5_DM`。最终 queue、trigger、reaction 均为 0，Hermes gateway active。
- 本轮未修改 Hermes 安装目录；OneBot `delegate_task` 明确 fail-closed，等待 Hermes 上游 per-turn 工具策略后再评估恢复。

## 外部联调状态

2026-08-06 在 Arch `192.168.1.18` 使用真实 Hermes + LLBot direct compose 部署 `0.3.0` 完成 Task 4 指定白名单联调。机器人 QQ 为 `3101482118`，唯一允许群为 `1072992996`，唯一允许私聊用户为 `2056963663`；Hermes/LLBot WS 与 HTTP token 已配置一致且未记录在文档中。以下外部事件仍是通过真实反向 WS 注入的合成 OneBot payload，不等同于真人 QQ 客户端发言，也不代表 v0.4.0。

2026-08-08 已通过 SSH 主机校验连接指定 Arch `192.168.1.18`。当前独立 worktree `/home/notnotype/CodeRepository/hermes-plugin-onebot11/.agent/workspace/wt/onebot11-turn-anchor-task5-live-20260808` 部署 `87b8dbd`；LLBot compose 健康，Hermes gateway active。旧部署保留在 Hermes home 外部回滚 symlink，不参与插件扫描。

以下是 v0.3.0/Task 4 历史证据，不代表 v0.4.0 当前证据：

- 群普通用户和超级管理员消息落在同一 session key `agent:main:onebot11:group:1072992996`；这是 Task 4 历史证据，当前 Task 5 的两个独立 anchor 已在后续段落重新验证。
- `/context` 在入队前旁路返回待处理 batch；`/status` 返回 `chat_type=group`、lease/失败计数等紧凑状态，不再发送旧 summary 原文。
- 普通用户 `/whoami` 只显示 5 个当前范围只读查询工具；请求 `terminal` 产生结构化权限错误和 `permission_denied` 审计，没有执行命令。
- 群处理 reaction 在指定群的真实消息 ID 上按 `set=true -> 回复 -> set=false` 完成；允许私聊用户收到正常 Agent 回复。
- 非白名单群 `786830134` 和私聊 `999999999` 只记录 `access_denied`，没有队列记录和出站请求。

收口前 v0.4.0 部署已确认（不代表当前未部署的收口 HEAD）：

- Hermes 日志确认 `groups=['1072992996'] dm_policy=allowlist super_admins=['2056963663'] mention=True session=shared`；WS 监听 `0.0.0.0:18880`，LLBot HTTP `get_login_info` 返回机器人 QQ `3101482118`。
- v8 队列迁移到 schema v9，迁移前的 pending 数据已备份；使用 `/onebot clear` 只清理插件队列，不删除 Hermes session 历史。
- 两条合成反向 WS @ 事件分别生成 anchor `115`、`116`，目标群实际收到 `TurnAnchor 测试 A` 和 `TurnAnchor 测试 B`，处理后队列、trigger、reaction 均为空。
- 普通群成员 `3199036352` 的 `/whoami` 返回 role `user` 和 5 个当前范围只读工具；请求 `terminal` 被真实 `pre_tool_call` 拒绝并写入 `permission_denied` 审计。
- 允许私聊用户 `2056963663` 收到 `OneBot11 DM v0.4.0 smoke`；私聊使用独立 DM session。
- 非白名单群 `786830134`、非白名单私聊 `999999999` 均只记录 `access_denied`，SQLite 中没有入队记录和 trigger。
- 对真实群消息 ID `1197886633` 进行 reaction recovery：持久化 `maybe_set` 记录后重启 gateway，记录恢复为 0 条，`fetch_emoji_like` 返回空列表；未执行群管理写操作。

外部验收边界：

- 两个 anchor 和权限测试使用真实反向 WS 连接注入的合成 OneBot payload，不等同于两名真人 QQ 客户端同时发言；当前 `TASK5_ANCHOR_A/B` 也属于该边界。合成 payload 使用的伪造 message ID 无法用于 reaction，LLBot 对其返回失败；随后使用真实群消息 ID `2119419776` 完成 `set=true -> set=false`，证明当前 LLBot action 可用。该结果不升级为 OneBot 11 exactly-once 保证。
- v0.4.0 直接群管理写工具、非幂等出站断线后的 `uncertain` 与人工 resolve 未执行；未获额外授权时不执行禁言、踢人、撤回或全员禁言。
- 未在本轮使用 NapCat，也未把 WS 重连重放行为提升为 OneBot 11 协议保证；LLBot 关闭时仍出现已有 Discord/Weixin 关闭异常和 OneBot disconnect timeout，但不影响本次重启后的 v0.4.0 gateway active。

## 约束与取舍

- `onebot11/` 保持零 Hermes 依赖；只有根目录 `adapter.py` 依赖 Hermes gateway。
- 消息处理是至少一次语义；OneBot 11 非幂等请求无法提供 exactly-once，因此未知结果不自动重试。
- 不自动迁移旧的群 `per_user` session 历史到新的 shared session；需要人工决定是否清理旧历史。
- 本轮不纳入 OneBot 12、语音转写、群级热更新、复杂管理后台和强制语义摘要模型。
