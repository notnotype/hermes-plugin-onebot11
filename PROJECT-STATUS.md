# PROJECT-STATUS

仓库级现状报告。实现变更通过分支和 PR 收口；本文只记录当前代码合同、验证证据和仍需外部联调的事项。

## 当前状态

- **阶段**：PR #8 基线上的 TurnAnchor 收口已完成本地实现和 Hermes 组合验证；当前分支尚未推送、创建 PR 或部署。Arch 上已有旧提交的合成 smoke 证据，但不能替代本分支的真人并发、真实 unknown 出站和真实 reaction 清理验收。
- **核心合同**：群固定一个共享 session；群消息持久入队；每个真实 TurnAnchor 固定 batch 和 authority，同群按序单 lease follow-up；非幂等出站结果未知时进入 `uncertain`，不自动重放。
- **本地验证**：协议/状态机测试通过；使用本地 Hermes 源码与其 site-packages 运行 adapter 测试通过。最终门禁命令和环境见“验证证据”。

## 模块状态

| 模块 | 状态 | 当前合同 |
|---|---|---|
| `onebot11/message.py` | 完成 | array/CQ 字符串解析，保留 text、媒体、reply、文件/语音/视频/转发/未知段标记 |
| `onebot11/events.py` | 完成 | message 事件归一化；自身回传过滤；notice/request/lifecycle 只做限长统计摘要 |
| `onebot11/ws_server.py` | 完成 | token、loopback 默认、有界接收队列、同 chat 顺序、全局 inflight、失败关闭连接促使上游重放 |
| `onebot11/http_api.py` | 完成 | 查询有限重试；发送/管理/reaction 写永不自动重试；有符号 message_id；超时、429/5xx、非 JSON、超大响应分类；媒体 SSRF/类型/大小限制 |
| `onebot11/queue.py` | 完成 | SQLite WAL、schema 迁移、消息/TurnAnchor 去重、固定 batch lease、heartbeat、摘要、tombstone、uncertain 人工 resolve、reopen 和管理动作 operation ledger |
| `onebot11/dispatch.py` | 完成 | 每群最多一个活动 turn，恢复触发请求，暂停/恢复、失败状态转换和 reconnect reset |
| `onebot11/triggers.py` | 完成 | @、关键词、always、问句/记忆候选、5 秒 debounce、60 秒活跃窗口和显式旁路 LLM 三态判断 |
| `onebot11/permissions.py` | 完成 | `CallerContext`、`ChatTarget`、精确 `(session_id, turn_id)` binding、user/trusted_user/super_admin 角色、只读边界和 fail-closed |
| `onebot11/tools.py` | 完成 | 当前群/私聊范围查询和群管理写工具；写操作必须确认 |
| `adapter.py` | 完成 | Hermes glue、shared session、入站访问策略、hooks、工具 handler、群 turn 👀 指示器、出站生命周期、媒体回收、统一配置解析、raw self_id、临时摘要注入和 operation resolve |
| 文档/ADR | 完成 | README、权限、状态、Task 2/3/5 walkthrough、strict auxiliary、reconnect、operation ledger、TurnAnchor 和验收边界同步到当前合同 |

## 验证证据

- `.venv\\Scripts\\python.exe -m pytest -q`：`145 passed, 1 skipped`；纯插件环境只跳过没有 Hermes gateway 的 adapter 集成测试。
- `.venv\\Scripts\\python.exe -m ruff check .`：通过。
- `pip install -e ".[dev]"`：Windows 临时干净 venv 安装通过；`import onebot11` 通过。Linux 安装仍由 PR #8 CI 负责。
- `scripts/verify_hermes_integration.py` + Hermes 主源码和 strict auxiliary worktree：`218 passed`，strict auxiliary 回归 `3 passed`，smoke 通过，`tools=9 hooks=4 strict_auxiliary=True reconnect=True`；覆盖 TurnAnchor authority、旧 API 安全禁用和 selector message-key 竞态。
- Arch 旧 Hermes `91937a6` 的 strict auxiliary 仍不支持 `fallback_policy/max_attempts`；本分支不会在旧 API 上偷偷调用主模型，LLM trigger 会安全禁用并保留 pending。
- 真实 Hermes 临时 `HERMES_HOME` 注册 smoke：已确认平台、9 个工具、4 个安全 hooks 和 `onebot11_trigger` auxiliary 均注册，并验证 shared session/TurnAnchor 合同、严格旁路配置、pending anchor 恢复、home cron 和同实例 reconnect；旧 Hermes 组合也验证为安全禁用 strict LLM trigger。

## 外部联调状态

2026-08-07 在 Arch `192.168.1.18` 曾将旧 Hermes gateway 切换到插件 commit `af984b3`（版本 `0.3.3`），并补齐 `ONEBOT11_HOME_CHANNEL_TYPE=dm`；该记录只作为历史 smoke 证据。本分支后续联调仍固定机器人 QQ `3101482118`、唯一允许群 `1072992996` 和唯一允许私聊用户 `2056963663`，且必须先备份配置/SQLite/session。

旧部署已确认：

- OneBot `get_login_info` 返回 `retcode=0`、机器人 QQ `3101482118`；群管理员 `/onebot status` 回执发到指定群，私聊管理员命令回执只发到 `2056963663`。
- 通过真实反向 WS 向指定群注入三条合成事件（两条普通消息 + 一条 @），SQLite 序号从 100 到 103，最终 `pending/leased/uncertain/failed` 全为 0；确定性摘要包含三条消息，说明群队列批次收口正常。
- 重启恢复回归：指定群注入一条不触发 Agent 的普通消息，重启前为 `pending=1、pending_trigger=0`，重启后仍为 `pending=1、pending_trigger=0`；随后直接清理该测试消息，当前队列为空。LLBot 日志确认 WS 断线后重新连接。
- 本轮新部署使用不存在于 LLBot 消息库的合成 message ID 做 reaction，LLBot 返回 `msg not found`，因此不能把本轮合成事件记为 reaction 成功；此前旧部署的真实消息 ID reaction smoke 证据仍保留，但本轮新 commit 仍需真人消息或真实历史消息复验。
- 白名单外群 `999999999` 的合成事件被拒绝，队列保持 `pending=0、trigger_requests=0`，没有产生出站；本次联调没有向白名单外目标发送消息。
- 在 Arch 临时 Hermes home 中直测同一 adapter 的 `connect → disconnect → reconnect → disconnect`，四个阶段均在 3 秒超时内完成（实际约 1–20ms）；重连后 queue/dispatcher 可以继续工作。

当前 TurnAnchor 分支仍需单独完成：

- 以上本轮 Agent 入站事件使用了真实反向 WS 的合成 OneBot payload，不等同于 QQ 客户端真人发言；还未完成两名真人群成员同时触发时的外部观察。
- 指定群旧部署曾用已删除 smoke 消息验证群管理写工具预览/确认；本分支的非幂等出站 `unknown` 和 `resolve action retry|discard` 仍未在真实 QQ 上制造。
- LLBot 当前请求日志会打印 Bearer header；插件不会把 token 发送到外部媒体地址，但部署侧应在生产前关闭该日志或轮换 token。未在本轮擅自修改 LLBot 凭据。
- 未在本轮使用 NapCat，也未把 WS 重连重放行为提升为协议保证。
- Hermes 全局 SIGTERM 关闭日志仍偶发出现 `onebot11 disconnect timed out after 5.0s`；空 adapter 直测没有复现，日志同时包含 Discord/Weixin 的关闭期错误，当前作为 Hermes 多平台 shutdown 观察项，不据此修改 OneBot 插件。
- Hermes strict auxiliary 修改已在独立干净 worktree 提交并通过 3 个测试，但当前远端 `NousResearch/hermes-agent` 对本账号无写权限，尚未能推送或创建独立 PR。

## 约束与取舍

- `onebot11/` 保持零 Hermes 依赖；只有根目录 `adapter.py` 依赖 Hermes gateway。
- 消息处理是至少一次语义；OneBot 11 非幂等请求无法提供 exactly-once，因此未知结果不自动重试。
- 不自动迁移旧的群 `per_user` session 历史到新的 shared session；需要人工决定是否清理旧历史。
- 本轮不纳入 OneBot 12、语音转写、群级热更新、复杂管理后台、RAG/向量库、运行时自优化、queued `⏳` reaction 和强制语义摘要模型。
- `trusted_user` 只读；权限、白名单和角色变化不由 Agent 运行时修改。
