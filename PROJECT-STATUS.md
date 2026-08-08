# PROJECT-STATUS

仓库级现状报告。实现变更通过分支和 PR 收口；本文只记录当前代码合同、验证证据和仍需外部联调的事项。

## 当前状态

- **阶段**：v0.4.0 TurnAnchor 实现位于 Issue #9 stacked 分支；基线 PR #7 仍未合并，尚未创建或合并本 Task PR。Arch v0.4.0 联调暂因 SSH host key 冲突阻塞。
- **核心合同**：群固定 shared session；每条锚点消息对应一个 authority 和一个串行 followup；自动 selector 只选 seq；非幂等 unknown 不自动重放。
- **本地验证**：本地 Hermes 源码与 site-packages 全套 `242 passed`，Ruff 通过；临时 Hermes smoke 已通过；Arch 尚未部署 v0.4.0。

## 模块状态

| 模块 | 状态 | 当前合同 |
|---|---|---|
| `onebot11/message.py` | 完成 | array/CQ 字符串解析，保留 text、媒体、reply、文件/语音/视频/转发/未知段标记 |
| `onebot11/events.py` | 完成 | message 事件归一化；自身回传过滤；notice/request/lifecycle 只做限长统计摘要 |
| `onebot11/ws_server.py` | 完成 | token、loopback 默认、有界接收队列、同 chat 顺序、全局 inflight、失败关闭连接促使上游重放 |
| `onebot11/http_api.py` | 完成 | 查询有限重试；发送/管理/reaction 写永不自动重试；有符号 message_id；超时、429/5xx、非 JSON、超大响应分类；媒体 SSRF/类型/大小限制 |
| `onebot11/queue.py` | 完成 | schema v9、TurnAnchor/batch 边界、anchor 失败状态、queued/processing reaction、legacy hold |
| `onebot11/dispatch.py` | 完成 | 每群一个活动 anchor，串行 followup、heartbeat、恢复异常隔离和 fencing |
| `onebot11/triggers.py` | 完成 | 精确触发纯函数；自动 selector 严格选择一个现存 seq，不读取权限配置 |
| `onebot11/permissions.py` | 完成 | `CallerContext`、`ChatTarget`、精确 `(session_id, turn_id)` binding、角色工具并集、fail-closed |
| `onebot11/tools.py` | 完成 | 当前范围查询；真实写请求前 fencing；写工具按 anchor authority 直接执行，unknown 不自动重放 |
| `adapter.py` | 完成 | anchor/caller 绑定、结构化上下文、authority reminder、⏳/👀、unknown-safe 出站 |
| 文档/ADR | 完成 | ADR-0005、Task 5、v0.4.0 changelog 与迁移指南 |

## 验证证据

- 使用本地 Hermes 源码与 site-packages：全套测试 `242 passed`，覆盖 schema v9、多个独立 anchor、batch 边界、immutable authority、selector、双阶段 reaction、unknown 写操作、媒体端口边界、关闭 fencing 和 adapter 生命周期。
- 本轮 Arch + LLBot：服务加载 `0.3.0`；群 `1072992996` 和私聊 `2056963663` 是唯一出站目标。已验证 `/context` 旁路、紧凑 `/status`、普通用户高风险工具拒绝、shared session key、群 reaction 生命周期、允许私聊回复，以及非白名单群/私聊 fail-closed。
- `.venv\\Scripts\\python.exe -m ruff check .`：通过。
- 临时 `HERMES_HOME` 真实注册 smoke 已完成：插件版本 `0.4.0`、12 个 OneBot 工具、4 个 hooks、`onebot11_trigger` auxiliary、QueueStore schema v9 和 `group_sessions_per_user=False` 均已确认。
- 本轮未修改 Hermes 安装目录；OneBot `delegate_task` 明确 fail-closed，等待 Hermes 上游 per-turn 工具策略后再评估恢复。

## 外部联调状态

2026-08-06 在 Arch `192.168.1.18` 使用真实 Hermes + LLBot direct compose 部署 `0.3.0` 完成 Task 4 指定白名单联调。机器人 QQ 为 `3101482118`，唯一允许群为 `1072992996`，唯一允许私聊用户为 `2056963663`；Hermes/LLBot WS 与 HTTP token 已配置一致且未记录在文档中。以下外部事件仍是通过真实反向 WS 注入的合成 OneBot payload，不等同于真人 QQ 客户端发言，也不代表 v0.4.0。

当前 v0.4.0 外部联调被安全阻塞：SSH alias `archlinux` 对应服务器当前提供的 host key 指纹为 `SHA256:EF3F5Zw6/acnlb2FL/ktuwLGZUuilbMhZKuo/9YNyv8`，与本机 `known_hosts` 中的旧 key 不一致。未确认是否发生主机密钥轮换前，不关闭 host key 校验、不接受新 key，也不部署。

以下是 v0.3.0/Task 4 历史证据，不代表 v0.4.0 已外部验收：

- 群普通用户和超级管理员消息落在同一 session key `agent:main:onebot11:group:1072992996`；v0.4.0 已改为两个独立 anchor，尚待外部验证。
- `/context` 在入队前旁路返回待处理 batch；`/status` 返回 `chat_type=group`、lease/失败计数等紧凑状态，不再发送旧 summary 原文。
- 普通用户 `/whoami` 只显示 5 个当前范围只读查询工具；请求 `terminal` 产生结构化权限错误和 `permission_denied` 审计，没有执行命令。
- 群处理 reaction 在指定群的真实消息 ID 上按 `set=true -> 回复 -> set=false` 完成；允许私聊用户收到正常 Agent 回复。
- 非白名单群 `786830134` 和私聊 `999999999` 只记录 `access_denied`，没有队列记录和出站请求。

已确认：

- Hermes 实际加载既有 0.3.0 插件，`session=shared`，WS 监听和 LLBot 反向 WS 自动重连均成功；v0.3.1 尚未重新部署。
- OneBot `get_login_info`、目标群/用户查询、群消息和私聊测试发送均返回 `retcode=0`，出站目标只有上述群和用户。
- 通过真实 WS 服务注入允许群 @ 事件，消息进入持久 SQLite 队列，Agent 回复 `OneBot11联调成功` 并成功发回目标群；允许用户私聊回复 `OneBot11私聊成功`。
- 不带 @ 的消息在 pending 中持久化；重启 Hermes 后仍在队列，管理员 `flush` 后完成处理并清空消息/trigger。
- 真实 LLBot 上报的其他群消息和合成的非白名单私聊均被拒绝并写入审计；没有产生出站。
- 在指定群用真实消息 ID `-71496113` 完成 reaction smoke：`set=true` 后收到 Hermes 群回复，再发送 `set=false`；`fetch_emoji_like` 返回空列表，确认 👀 已清理。

仍需单独完成：

- 以上 Agent 入站事件使用了真实反向 WS 的合成 OneBot payload，不等同于 QQ 客户端真人发言；还未完成两名真人群成员同时触发时的外部观察。
- v0.4.0 的直接写工具、非幂等出站断线后的 `uncertain` 与人工 resolve 尚未在真实 QQ 上执行；未获额外授权时不执行破坏性写动作。
- 未在本轮使用 NapCat，也未把 WS 重连重放行为提升为协议保证。

## 约束与取舍

- `onebot11/` 保持零 Hermes 依赖；只有根目录 `adapter.py` 依赖 Hermes gateway。
- 消息处理是至少一次语义；OneBot 11 非幂等请求无法提供 exactly-once，因此未知结果不自动重试。
- 不自动迁移旧的群 `per_user` session 历史到新的 shared session；需要人工决定是否清理旧历史。
- 本轮不纳入 OneBot 12、语音转写、群级热更新、复杂管理后台和强制语义摘要模型。
