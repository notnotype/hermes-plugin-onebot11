# Task 7：OneBot 11 媒体投递、状态提示、纯文本与运行时策略

- 日期：2026-08-11
- 状态：代码收口完成，待 PR 验证/合并；本 worktree 尚未部署 Arch
- 关联主线：shared session、TurnAnchor、authority 快照、OneBot 非幂等出站 unknown 合同

## 目标

收口 OneBot 最终回复的用户可见行为和运行时配置边界：

1. 同一 turn 内同一张图片不重复投递；
2. OneBot 默认发送纯文本，不把 Markdown 语法直接交给 QQ；
3. 为未来 Markdown 转图片定义不访问外部 URL 的 marker 协议；
4. 只有 Hermes 明确 metadata 的长时间运行/系统错误通知才走控制面；
5. 白名单、角色、trigger、reaction 和显示策略可以 `/onebot reload` 热更新。

## 已实现

### 媒体

- 新增零 Hermes 依赖的 `MediaDeliveryScope`；
- 本地路径使用 `resolve`、`normpath`、`normcase`，HTTP(S) URL 去掉 fragment 并规范 host/默认端口；
- 同一 scope 按来源和已读取内容 SHA-256 去重；
- 群 turn 使用 lease scope，私聊使用精确 session/turn，无法绑定时只使用当前消息 scope；
- 去重不跨 turn、session 或重启，不承诺 exactly-once；
- OneBot 图片继续使用受限 `base64://` segment；不向外部媒体 URL 发送 Bearer token。
- 入站 image segment 优先选择 URL，否则保留 file 标识；非 URL file 只有在配置
  `media_source_roots` 后才会通过 OneBot `get_image` 解析，并复制到受控 turn 目录；
  不读取任意返回路径。

### 文本和控制面

- `format_onebot_text()` 移除标题、强调、代码围栏、反引号和 Markdown 链接语法，但保留代码内容、列表、CJK 和 URL；
- `[[onebot11:markdown-image]]...[[/onebot11:markdown-image]]` 当前只移除 marker、按纯文本发送并审计，不启动 renderer、不访问外部 URL；
- `hermes_control_plane=true` + 明确 `hermes_control_kind`，或
  `hermes_system_error_notice=true`，才进入控制面发送；
- 控制面不调用 `mark_outbound_started()`，同一 turn 只发送一次，并在每个分块前检查 adapter、lease 和白名单；
- 未收到明确 metadata 时不匹配 `⏳ Working` 等文本，也不偷偷复制 Hermes heartbeat。

### 运行时策略

- `RuntimePolicySnapshot` 以单指针原子替换权限、角色、trigger、reaction 和文本显示策略；
- reload 优先调用 Hermes 的 gateway configuration loader，读取已合并的
  `PlatformConfig.extra`，不复制 Hermes YAML merge 规则；
- 环境变量仍然覆盖 YAML；
- 静态连接、token、self_id、queue path 和 session mode 变化明确要求重启；
- 成功 reload 清理旧 confirmation token；active turn 保留创建时的 authority/tool snapshot，
  但工具和出站仍实时检查当前白名单；
- 缺少 `pre_gateway_dispatch`、`pre_llm_call`、`pre_tool_call` 时，插件拒绝注册，避免安全 hook 缺失后静默 fail-open；
- role prompt 展示三类角色工具目录，但明确 role catalog 不是硬授权来源；
- 审计写失败只记录日志，不改变权限 hook 的 fail-closed 结果。
- Hermes generic 工具也进入 OneBot turn 的 `pre_tool_call` 硬门禁；普通用户
  不能因为工具不是 `qq_*` 就绕过角色工具快照。`tool_search` 仍在配置层、运行时
  双重禁止；`delegate_task` 必须显式配置。启用主 agent 只读后，主 agent 可直接
  使用 `read_file/search_files`，需要 terminal/process/write_file/patch 的工作交给
  delegated child；子代理不能调用 QQ 工具、send_message、cronjob 或再次委派。

### 触发、身份和上下文

- cooldown、`llm_judged_seq`、LLM 失败退避和真正创建 trigger 的时间都持久化在 SQLite；
  重复 WS 事件不会刷新 cooldown，新消息会清除 selector 失败退避。
- LLM selector 开启时，恢复轮询只唤醒 adapter 的策略恢复路径，不直接创建
  `cooldown_recovery` anchor；没有旁路 selector 时才使用 QueueStore 的直接 cooldown
  recovery。provider/model 缺失也会记录持久化失败退避。
- selector 只接受 `{"decision":"trigger","anchor_seq":123}`、
  `{"decision":"wait","anchor_seq":null}` 或 `{"decision":"ignore","anchor_seq":null}`；
  `anchor_seq` 必须是当前 pending 队列中真实存在的消息序号，authority 从该消息快照继承。
- 每条上下文消息同时展示 `seq`、真实 `message_id`、去重用 `message_key`、QQ 号、
  昵称、role、reply 和 segment/media markers。没有真实 ID 时 `message_id=""`，
  `message_key="hash:<sha256>"`；hash key 不会进入 OneBot `get_msg` 或撤回 API。
- 群 `/context` 和 `/ctx` 是入队前旁路诊断命令，不进入 shared session 或 Agent 队列。

### 生命周期收口

- lease 失效、adapter shutdown、epoch 变化后旧 turn 只清理内存，不访问 SQLite、
  工具或 OneBot API；QueueStore 关闭会等待已经进入的同步操作后再关闭连接。
- 文本/图片按 delivery unit 结算：全部明确成功才 ack；无出站的明确失败才 release；
  部分成功、unknown 或 fencing 进入 uncertain。
- 👀 reaction 状态持久化为 `pending -> maybe_set -> cleared`，恢复只执行有限次数
  `unset`，绝不重放 `set=true`、Agent turn 或群管理动作。
- 队列消息的 `resolve retry` 只对仍能证明原 authority、anchor 消息和 batch 边界的记录
  创建新的 request_id；旧 trigger 缺失或 authority 不明时保持 hold，不由管理员身份或
  后续 selector 接管。

## 计划出入

- 本轮没有修改本地 Hermes，也没有实现 renderer、Docker 子代理、OneBot 12、原始 WS spool、语义摘要或通用媒体发送；
- Hermes heartbeat 在上游提供明确 control-plane metadata 前不作为 OneBot 业务合同；
- Hermes 通用 web/browser/terminal/file 工具在 OneBot turn 中由插件的
  `pre_tool_call` 按角色快照硬拦截；`tool_search` 继续拒绝，`delegate_task` 必须显式
  配置。主 agent 只读时可直接使用 `read_file/search_files`，需要 terminal/process/
  write_file/patch 的工作交给 delegated child；子代理若丢失父 binding 仍拒绝，且不能
  调用 QQ 工具、send_message、cronjob 或再次委派。

本轮新增客服收口：delegated child 在父 QQ turn 结束后仍可继续使用项目工具，但不继承 QQ 出站、QQ 查询、cronjob 或再次委派权限；不再由适配器发送固定中文收到回执，长任务状态提示最多发送三次且受 turn 生命周期约束。Hermes 工具事件只映射为固定中文摘要，不把参数、路径、URL 或工具正文发送到 QQ。该行为已经由交接时的 delegated-child 回归测试、Hermes 组合 smoke 和 Arch 现场审计共同核对。

Arch 现场使用插件 checkout `e05e4b0f25d7be9aef706dde1d16849f06c742a5`，容器为 `running healthy`，当前白名单为群 `942513604`、超级管理员 `2056963663`、机器人 `3101482118`。LLBot 与 Hermes 反向 WS/HTTP 配置和 `get_status`/`get_login_info` 已核对；白名单外真实 QQ 群消息产生 `access_denied` 且未进入队列。目标客服群的历史真实消息曾完成 `inbound message -> response ready -> Sending response`，但本轮没有取得该群的真人客户端入站消息，因此不把 HTTP 出站探针或历史记录写成当前真人 QQ Agent pipeline 通过。

## 验证

本 worktree 使用本地 Hermes 源码：

```text
PYTHONPATH=C:\Users\notnotype\AppData\Local\hermes\hermes-agent;C:\Users\notnotype\AppData\Local\hermes\hermes-agent\venv\Lib\site-packages pytest -q
419 passed, 4 warnings
```

纯插件门禁为 `247 passed, 1 skipped`；skip 是 `tests/test_adapter.py` 因缺少 Hermes `gateway`。本轮完整 Hermes 环境测试实际运行了 adapter 集成测试；Windows asyncio subprocess transport 关闭期产生 4 条警告，无测试失败。

`ruff check .`、`node --check scripts/onebot11-pi-trigger.mjs` 和 `git diff --check` 通过。`scripts/verify_hermes_integration.py` 在临时 `HERMES_HOME` 下通过，测试部分以最新实际命令结果为准；本轮完整插件门禁为 `425 passed, 4 warnings`，并覆盖最多三次长任务提示、无自动回执和浏览器能力提示。

Arch 组合证据：容器 `hermes-support-support-hermes-1` 为 `running healthy`，插件 checkout 与目标 commit 一致，反向 WS 监听 `0.0.0.0:18880`，LLBot HTTP `get_status` 为 `online=true, good=true`，`get_login_info` 返回机器人 QQ `3101482118`。历史目标群 Agent 日志包含 `inbound message`、`response ready` 和 `Sending response`；本轮白名单外真实群入站只形成 `access_denied`，目标客服群真人入站未取得，故真人 QQ Agent pipeline 标记为未验证。

Arch 联调固定记录为群 `942513604`、超级管理员 `2056963663`、机器人 `3101482118`；本轮未执行禁言、踢人、撤回、全员禁言，未修改 Arch 配置、生产 SQLite 或 `PRAGMA user_version`。合成 HTTP 出站、真实 QQ 群入站和插件审计分开记录，未把 HTTP 探针升级为真人协议保证。

## 后续

- 获取目标客服群 `942513604` 的真人客户端入站后，验证 Hermes 中间正文、最多三次有界长任务状态提示、后台 delegated child completion 和最终回复在 QQ 上的顺序；不再把固定中文收到回执列为验收项。
- 在真人链路可用后，验证两名真人同时 @、selector/engage 窗口、图片-only/文字+图片、多媒体去重和管理写动作预览；
- 制造并人工处理非幂等出站 `unknown` 与 `resolve action retry|discard`；不执行真实禁言、踢人、撤回或全员禁言；
- Hermes 上游增加明确的 long-running/system-error control-plane metadata 后，再开启 OneBot 控制面通知的真实 heartbeat 联调；
- renderer 任务另行定义 PNG 字体、尺寸、临时文件和外部 URL allowlist；
- Hermes 上游完善 tool-search `turn_id` 和 delegation 继承后，再评估是否扩大 generic 工具范围；当前 `tool_search` 仍禁用，`delegate_task` 仅作为显式委派入口，generic 工具本身已经由 OneBot `pre_tool_call` 按 role snapshot 硬门禁。
