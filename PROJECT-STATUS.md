# PROJECT-STATUS

仓库级现状报告。实现变更通过分支和 PR 收口；本文只记录当前代码合同、验证证据和仍需外部联调的事项。

## 当前状态

- **阶段**：master 已合并 Task 7 之前的全部功能并部署 Arch（`0.6.0 / schema 12`）。本轮客服收口已完成代码、Hermes 组合验证和受控 Arch 联调：delegated child 在父 QQ turn 结束后仍可使用项目工具但不能越权；复杂问题先发送中文回执，再由后台子代理执行；工具进度只发送固定中文摘要；selector/engage 按候选类型和三档预算判定。
- **核心合同**：群固定一个共享 session；群消息持久入队；每个真实 TurnAnchor 固定 batch 和 authority，同群按序单 lease follow-up；非幂等出站结果未知时进入 `uncertain`，不自动重放。后台委派不延长父 QQ lease，但 delegated child 的项目工具执行不再错误依赖已结束的父 lease；QQ 工具、发送消息、cronjob 和再次委派仍拒绝。
- **本地验证**：协议/状态机测试和 Hermes 组合验证通过；Arch 容器现场健康、插件 checkout 与目标 commit 一致，受控 OneBot HTTP/WS 与白名单拒绝证据已核对。完整真人 QQ Agent pipeline 仍未完成，原因是本轮只能确认指定客服群外的真实 QQ 群消息与白名单拒绝，未取得指定客服群的真人客户端入站消息。

## 模块状态

| 模块 | 状态 | 当前合同 |
|---|---|---|
| `onebot11/message.py` | 完成 | array/CQ 字符串解析，保留 text、媒体、reply、文件/语音/视频/转发/未知段标记 |
| `onebot11/events.py` | 完成 | message 事件归一化；自身回传过滤；notice/request/lifecycle 只做限长统计摘要 |
| `onebot11/ws_server.py` | 完成 | token、loopback 默认、有界接收队列、同 chat 顺序、全局 inflight、失败关闭连接促使上游重放 |
| `onebot11/http_api.py` | 完成 | 查询有限重试；发送/管理/reaction 写永不自动重试；有符号 message_id；超时、429/5xx、非 JSON、超大响应分类；媒体 SSRF/类型/大小限制；文本/图片 segment 出站 |
| `onebot11/queue.py` | 完成 | SQLite WAL、schema 12 迁移、持久 cooldown/LLM judged cursor/失败退避、消息/TurnAnchor 去重、固定 batch lease、heartbeat、摘要、tombstone、uncertain 人工 resolve、reaction cleanup、reopen 和管理动作 operation ledger |
| `onebot11/dispatch.py` | 完成 | 每群最多一个活动 turn，恢复触发请求和 cooldown 到期恢复；LLM selector 开启时由 adapter 策略回调接管，不绕过 anchor 选择；暂停/恢复、失败状态转换和 reconnect reset |
| `onebot11/triggers.py` | 完成 | @、关键词、always、问句/记忆候选、自适应 debounce（消息间隔超过窗口立即判断，活跃时 trailing 节流）、60 秒活跃窗口、只选择真实 `anchor_seq` 的严格 selector；无短确认词特例，engaged 内所有普通消息统一交给 selector；纯图片消息不进 selector；prompt 按候选类型区分；**三档 engage 预算**（shallow/normal/deep）：bot 提问标记（`bot_asked`）+ 同用户回复 → deep 免 debounce 立即判断；回复引用 bot 或任务词 → deep；他人插话回落 normal；连续 ignore 降档 shallow；deep waiting 攒满 N 条新消息立即判；`short_rule_max_chars` 开启后 shallow 档无信号短消息本地 ignore |
| `onebot11/permissions.py` | 完成 | `CallerContext`、`ChatTarget`、精确 `(session_id, turn_id)` binding、user/trusted_user/super_admin 角色、主 agent 只读边界和 fail-closed；`tool_search` 始终禁止，`delegate_task` 作为委派入口，子代理只获得项目工具且不能调用 QQ/发送/再次委派 |
| `onebot11/media.py` | 完成 | 当前 turn 内按规范化来源和内容 hash 做防御性媒体去重，不跨 turn/重启承诺 exactly-once |
| `onebot11/formatting.py` | 完成 | OneBot 默认纯文本转换、Markdown image marker 清理和不可用 renderer 审计 |
| `onebot11/tools.py` | 完成 | 当前群/私聊范围查询和群管理写工具；写操作必须确认 |
| `adapter.py` | 完成代码收口 | Hermes glue、shared session、入站访问策略、generic/OneBot 工具 hooks、群级 slash/context command、工具 handler、群 turn 💬 正在回复指示器（默认 `128172`，可配置）、selector 候选 👀 查看提示（含候选替换/清理）、一次性长时间提示（默认 60s，直接发送不依赖 turn binding，成功/失败写审计）、Agent 最终回复的文本/图片出站生命周期、base64 segment、同轮媒体去重、纯文本、显式控制面消息、运行时 policy snapshot/reload、媒体回收、统一配置解析、raw self_id、消息身份/上下文注入、operation resolve、pi-ai selector、按 event metadata 恢复精确 binding、`_reply_asks_user`（回复问句/请求短语收尾标记 bot_asked）、`_message_replies_to_bot`（reply 目标等于 bot 最后消息时视为引用）；通用 `send_message`/cron plugin media 不是本轮可靠性合同 |
| `onebot11/pi_ai.py` + `scripts/onebot11-pi-trigger.mjs` | 完成 | 零 Hermes 依赖的 Python/Node 短生命周期旁路客户端，固定 pi-ai 版本、环境变量密钥、无语义重试和失败分类 |
| 文档/ADR | 完成 | README、权限、状态、Task 2/3/5/6/7 walkthrough、pi-ai、reconnect、operation ledger、TurnAnchor、session command 和验收边界同步到当前合同 |

## 验证证据

- 在本 worktree 设置 `PYTHONPATH=C:\Users\notnotype\AppData\Local\hermes\hermes-agent;C:\Users\notnotype\AppData\Local\hermes\hermes-agent\venv\Lib\site-packages` 后，`pytest -q`：`419 passed, 4 warnings`（Windows asyncio subprocess transport 关闭期警告，无失败）；纯插件环境为 `247 passed, 1 skipped`，skip 是 `tests/test_adapter.py` 因缺少 Hermes `gateway`。新增覆盖：engaged 短确认词统一走 selector、selector 分类型 prompt、纯图片不进 selector、三档 engage 预算（deep 同用户立即判/他人回落/任务词升级/连续 ignore 降档/short_rule/waiting 攒消息）、bot_asked 与 reply-to-bot 信号、长时间提示新发送路径。
- focused 验证覆盖：媒体 scope、纯文本/marker、GatewayConfig reload、role catalog、hook capability gate、控制面通知去重、审计失败 fail-closed、shared session、queue recovery、权限和图片出站。
- `ruff check .`、`node --check scripts/onebot11-pi-trigger.mjs` 和 `git diff --check`：通过。
- `scripts/verify_hermes_integration.py` 在临时 `HERMES_HOME` 下通过：测试部分为 `419 passed, 4 warnings`，随后输出 `tools=9 hooks=5 pi_ai_trigger=True reconnect=True slash_commands=True`；`node --check scripts/onebot11-pi-trigger.mjs` 也通过。本地 Hermes 集成 smoke 和 Node helper 仍是独立验收证据，不能由纯插件 pytest 自动推断。
- delegated child 回归测试新增合同：父 lease 结束后，子代理可以继续使用项目 `terminal`，但 `qq_*` 与 `send_message` 仍拒绝；本轮聚焦验证为 `7 passed, 165 deselected`，纯插件全量门禁为 `247 passed, 1 skipped`，Ruff 通过。skip 是 `tests/test_adapter.py` 因缺少 Hermes `gateway`。
- Arch 组合现场：`hermes-support-support-hermes-1` 为 `running healthy`；容器内插件 checkout 为 `e05e4b0f25d7be9aef706dde1d16849f06c742a5 fix: ensure support turns acknowledge before agent work`，分支为 `master...origin/master`；`ONEBOT11_ALLOWED_GROUPS=942513604`、`ONEBOT11_SELF_ID=3101482118`、`ONEBOT11_WS_HOST=0.0.0.0`，LLBot HTTP `get_status` 返回 `online=true, good=true`，机器人 QQ 为 `3101482118`。
- 现场日志确认反向 WS 配置为 LLBot 同网容器 `ws://support-hermes:18880`，Hermes 曾记录 `OneBot11: 反向 WS 已监听 0.0.0.0:18880`；目标群 `942513604` 的历史真实消息曾走过 `inbound message -> response ready -> Sending response`，并且当天受控 HTTP 发消息均收到 `message_id`。白名单外真实 QQ 群 `559332109`、`786830134`、`976967537` 的入站分别产生 `access_denied`，没有进入目标队列或产生 Agent turn。
- 本轮真人 QQ 边界：没有取得目标客服群 `942513604` 的真人客户端入站消息；向该群发出的 HTTP 探针只证明了 OneBot HTTP 出站，不证明真人 QQ Agent pipeline。没有把这些探针升级为真人验收，也没有执行禁言、踢人、撤回、全员禁言或修改 Arch 配置。

## 外部联调状态

2026-08-14 通过 `ssh arch` 做只读/受控复核，机器人 QQ 固定为 `3101482118`，当前客服群白名单为 `942513604`，超级管理员为 `2056963663`。容器 `hermes-support-support-hermes-1` 为 `running healthy`，插件 checkout 为 `master`、commit `e05e4b0f25d7be9aef706dde1d16849f06c742a5`，版本 `0.6.0`；没有拉取、切换、重启或修改配置。

容器与 LLBot 同处 `luckylillia_app_network`；LLBot 配置同时存在 `ws://support-hermes:18880` 和 `ws://host.docker.internal:18880` 两个反向 WS 出口，均启用相同 token。Hermes 监听 `0.0.0.0:18880`，LLBot `get_status` 返回 `online=true, good=true`，`get_login_info` 返回机器人 QQ `3101482118`。现场已分开记录合成 HTTP 出站、真实 QQ 群入站和插件审计：白名单外群真实消息被拒绝；目标客服群只看到既有历史 Agent pipeline 证据，未取得本轮真人客户端入站。

此前 2026-08-08 的受控联调备份仍保留在 `/home/notnotype/.hermes/onebot11-backup-20260808-turn-anchor-contract/`，历史 reaction、batch 边界和白名单证据不等同于本分支已完成真人 Agent pipeline 验收。

此前部署的 PR #8/TurnAnchor 版本已确认：

- OneBot `get_login_info` 返回 `retcode=0`、机器人 QQ `3101482118`；当前 WS 在 `0.0.0.0:18880` 监听。
- 使用指定群中已存在的真实消息 ID `2076873675` 做一次受控 WS 重放：该 anchor 被 ack，后续 seq `119–127` 仍为 pending，证明 TurnAnchor batch 边界没有偷吃后续消息。
- LLBot 日志确认对真实消息 `2076873675` 先发送 `emoji_id=128064,set=true`，Hermes 回复到指定群后再发送 `set=false`；回复消息 ID 为 `438359985`。这是真实消息 ID 的受控重放，不等同于真人刚发消息。
- 重启 Hermes gateway 前后指定群均为 `schema=9、pending=9、无 trigger`，WS 恢复监听，pending 保留；这是旧生产版本的历史证据，不代表当前 schema 12 分支已部署。
- 白名单外群 `999999999` 的事件被拒绝，SQLite 中该群为 0 行，指定群队列未变化，没有产生出站。
- Arch 配置原有 `roles.super_admin.tools: image_generate`（非 OneBot 工具），在备份后按 fail-closed 合同移除；白名单、token、机器人 QQ 和 LLBot 配置未放宽。

Arch 已部署 `0.6.0` 基线和本轮客服收口 commit，但本轮没有以 HTTP 自发消息替代真人 QQ 验收。通过现场健康检查、反向 WS/HTTP 连通性、真实白名单外群拒绝、历史目标群 Agent 回复日志和插件审计核对了组合边界；真实目标群真人 Agent pipeline、两名真人并发、非幂等 `unknown`/`resolve`、图片完整链路和管理写动作仍待专门验收。

Arch 当前生产 queue schema 现场未在本轮改写；本轮未直接编辑生产 SQLite，也未修改 `PRAGMA user_version` 伪装兼容。

当前仍需单独完成：

- 取得目标客服群 `942513604` 的真人客户端入站消息，验证中文即时回执、固定工具进度、后台 delegated child completion 和最终回复在 QQ 上的可见顺序。
- 在真人链路可用后，验证两名真人群成员同时 @、selector/engage 窗口和图片-only/文字+图片场景。
- 制造并人工处理非幂等出站 `unknown` 与 `resolve action retry|discard`；不执行真实禁言、踢人、撤回或全员禁言。
- LLBot 当前请求日志会打印 Bearer header；插件不会把 token 发送到外部媒体地址，但部署侧应在生产前关闭该日志或轮换 token。未在本轮擅自修改 LLBot 凭据。
- 未在本轮使用 NapCat，也未把 WS 重连重放行为提升为协议保证。
- Hermes 全局 SIGTERM 关闭日志仍偶发出现 `onebot11 disconnect timed out after 5.0s`；空 adapter 直测没有复现，日志同时包含 Discord/Weixin 的关闭期错误，当前作为 Hermes 多平台 shutdown 观察项，不据此修改 OneBot 插件。
- Hermes strict auxiliary 和媒体/unknown 合同不在本轮插件交付范围，也不创建 Hermes PR。旁路判断由插件自有 pi-ai helper 负责；图片继续在插件侧 best-effort。

## 约束与取舍

- `onebot11/` 保持零 Hermes 依赖；只有根目录 `adapter.py` 依赖 Hermes gateway。
- 消息处理是至少一次语义；OneBot 11 非幂等请求无法提供 exactly-once，因此未知结果不自动重试。
- Agent 最终回复图片是主要出站媒体场景；`send_message`、cron 和 standalone plugin media 的结果合同不在本轮收口范围，按安全降级处理。
- 不自动迁移旧的群 `per_user` session 历史到新的 shared session；需要人工决定是否清理旧历史。
- 本轮不纳入 OneBot 12、语音转写、复杂管理后台、RAG/向量库、运行时自优化和强制语义摘要模型。⏳ 只是 selector 等待提示（best-effort、内存登记），不是周期性心跳，也不纳入崩溃后 exactly-once 清理承诺；长时间运行提示仍只保留一次性状态提示。
- `trusted_user` 不能使用 OneBot 群管理写工具；Hermes generic 高风险工具仍必须在
  `roles.trusted_user.tools` 中逐项显式配置。权限、白名单和角色变化不由 Agent 运行时修改。
- `/onebot reload` 只热更新策略字段；连接、凭据、self_id、队列路径和 session 模式仍需重启。环境变量继续覆盖 YAML。
- OneBot 控制面 heartbeat 依赖 Hermes 明确 metadata；在上游合同完成前不匹配普通文本，也不把系统通知当作业务出站。
