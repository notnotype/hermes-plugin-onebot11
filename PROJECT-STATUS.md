# PROJECT-STATUS

仓库级现状报告。实现变更通过分支和 PR 收口；本文只记录当前代码合同、验证证据和仍需外部联调的事项。

## 当前状态

- **阶段**：OneBot 11 可靠性、安全和分层触发已完成本地闭环，并已完成 Arch + LLBot 指定白名单及群处理 reaction 指示器联调；本分支待提交、推送和创建 PR。
- **核心合同**：群固定一个共享 session；群消息持久入队；触发后按 lease 启动单群单 turn；非幂等出站结果未知时进入 `uncertain`，不自动重放。
- **本地验证**：协议/状态机测试通过；使用本地 Hermes 源码与其 site-packages 运行 adapter 测试通过。最终门禁命令和环境见“验证证据”。

## 模块状态

| 模块 | 状态 | 当前合同 |
|---|---|---|
| `onebot11/message.py` | 完成 | array/CQ 字符串解析，保留 text、媒体、reply、文件/语音/视频/转发/未知段标记 |
| `onebot11/events.py` | 完成 | message 事件归一化；自身回传过滤；notice/request/lifecycle 只做限长统计摘要 |
| `onebot11/ws_server.py` | 完成 | token、loopback 默认、有界接收队列、同 chat 顺序、全局 inflight、失败关闭连接促使上游重放 |
| `onebot11/http_api.py` | 完成 | 查询有限重试；发送/管理/reaction 写永不自动重试；有符号 message_id；超时、429/5xx、非 JSON、超大响应分类；媒体 SSRF/类型/大小限制 |
| `onebot11/queue.py` | 完成 | SQLite WAL、schema 迁移、持久去重、批量 lease、heartbeat、摘要、tombstone、uncertain 人工 resolve |
| `onebot11/dispatch.py` | 完成 | 每群最多一个活动 turn，恢复触发请求，暂停/恢复和失败状态转换 |
| `onebot11/triggers.py` | 完成 | @、关键词、always、问句/记忆候选、5 秒 debounce、60 秒活跃窗口和显式旁路 LLM 三态判断 |
| `onebot11/permissions.py` | 完成 | `CallerContext`、`ChatTarget`、精确 `(session_id, turn_id)` binding、角色工具并集、fail-closed |
| `onebot11/tools.py` | 完成 | 当前群/私聊范围查询和群管理写工具；写操作必须确认 |
| `adapter.py` | 完成 | Hermes glue、shared session、入站访问策略、hooks、工具 handler、群 turn 👀 指示器、出站生命周期和媒体回收 |
| 文档/ADR | 完成 | README、权限、状态、任务 walkthrough 和两项架构决策同步到当前合同 |

## 验证证据

- `.venv\\Scripts\\python.exe -m pytest -q`：`109 passed, 1 skipped`；没有 Hermes 依赖的环境只跳过需要 Hermes 运行时的 adapter 集成测试。
- 使用本地 Hermes 实例的源码和 site-packages：全套测试 `168 passed`；覆盖 adapter hooks、工具注册、共享队列、身份绑定、shared session key、媒体清理、出站 unknown、严格 auxiliary 参数、触发竞争和 reaction 生命周期。
- 严格 auxiliary 回归测试：`3 passed`；确认 `fallback_policy="none"`、`max_attempts=1` 和旧 API 安全降级。
- `.venv\\Scripts\\python.exe -m ruff check .`：通过。
- 真实 Hermes 临时 `HERMES_HOME` 注册 smoke：已确认平台、9 个工具、4 个安全 hooks 和 `onebot11_trigger` auxiliary 均注册，并验证 shared session 合同和严格旁路配置。

## 外部联调状态

2026-08-06 在 Arch `192.168.1.18` 使用真实 Hermes + LLBot direct compose 完成指定白名单联调。机器人 QQ 为 `3101482118`，唯一允许群为 `1072992996`，唯一允许私聊用户为 `2056963663`；Hermes/LLBot WS 与 HTTP token 已配置一致且未记录在文档中。

已确认：

- Hermes 实际加载当前 0.3.0 插件，`session=shared`，WS 监听和 LLBot 反向 WS 自动重连均成功。
- OneBot `get_login_info`、目标群/用户查询、群消息和私聊测试发送均返回 `retcode=0`，出站目标只有上述群和用户。
- 通过真实反向 WS 服务注入允许群消息，普通消息先进入持久 SQLite 队列，随后注入 @ 触发；队列出现单个群 lease，最终消息和 trigger request 均完成清理，滚动摘要写入。
- 指定群收到 Hermes 回复 `OneBot11 联调成功`；允许用户 `2056963663` 收到私聊回复 `OneBot11 私聊联调成功`。
- 已验证 pending 消息在 Hermes 重启后仍可恢复，并由管理员 `flush` 完成处理；WS/HTTP 重连行为只记录为实际联调结果，不升级为 OneBot 11 协议保证。
- 真实 LLBot 上报的其他群消息和合成的非白名单私聊均被拒绝并写入审计；没有产生出站。
- 在指定群用真实消息 ID `-726745341` 完成 reaction smoke：`set=true` 后收到 Hermes 群回复，再发送 `set=false`；LLBot 的 `fetch_emoji_like` 返回空列表，确认 👀 已清理。

仍需单独完成：

- 以上 Agent 入站事件使用了真实反向 WS 的合成 OneBot payload，不等同于 QQ 客户端真人发言；还未完成两名真人群成员同时触发时的外部观察。
- 群管理写工具的预览/确认、非幂等出站断线后的 `uncertain` 与人工 resolve 尚未在真实 QQ 上执行；本地测试覆盖了状态机。
- 未在本轮使用 NapCat，也未把 WS 重连重放行为提升为协议保证。

## 约束与取舍

- `onebot11/` 保持零 Hermes 依赖；只有根目录 `adapter.py` 依赖 Hermes gateway。
- 消息处理是至少一次语义；OneBot 11 非幂等请求无法提供 exactly-once，因此未知结果不自动重试。
- 不自动迁移旧的群 `per_user` session 历史到新的 shared session；需要人工决定是否清理旧历史。
- 本轮不纳入 OneBot 12、语音转写、群级热更新、复杂管理后台和强制语义摘要模型。
