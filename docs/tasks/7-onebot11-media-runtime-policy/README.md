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

- 本轮没有修改本地 Hermes，也没有实现 renderer、Docker 子代理、OneBot 12、原始 WS spool、
  语义摘要或通用媒体发送；
- Hermes heartbeat 在上游提供明确 control-plane metadata 前不作为 OneBot 业务合同；
- Hermes 通用 web/browser/terminal/file 工具在 OneBot turn 中由插件的
  `pre_tool_call` 按角色快照硬拦截；`tool_search` 继续拒绝，`delegate_task` 必须显式
  配置。主 agent 只读时可直接使用 `read_file/search_files`，需要 terminal/process/
  write_file/patch 的工作交给 delegated child；子代理若丢失父 binding 仍拒绝，且不能
  调用 QQ 工具、send_message、cronjob 或再次委派。

## 验证

本 worktree 使用本地 Hermes 源码：

```text
PYTHONPATH=C:\Users\notnotype\AppData\Local\hermes\hermes-agent;C:\Users\notnotype\AppData\Local\hermes\hermes-agent\venv\Lib\site-packages pytest -q
326 passed, 1 skipped
ruff check .
```

skip 是 `tests/test_pi_ai.py` 缺少 pi-ai npm 运行依赖；adapter 集成测试在上述
Hermes 源码路径下实际运行，没有因缺 gateway 跳过。`scripts/verify_hermes_integration.py`
在临时 `HERMES_HOME` 下也通过，输出
`Hermes integration smoke passed: tools=9 hooks=5 pi_ai_trigger=True reconnect=True slash_commands=True`；
`node --check scripts/onebot11-pi-trigger.mjs` 也通过；测试数字必须以实际命令结果为准。

Arch 联调仍严格限制为群 `1072992996`、私聊用户 `2056963663`；
本任务未在该 worktree 部署或发送未经授权的管理动作。合成 WS payload
与真人 QQ 客户端消息必须分开记录，前者不升级为 OneBot 11 协议保证。

## 后续

- Hermes 上游增加明确的 long-running/system-error control-plane metadata 后，再开启
  OneBot 控制面通知的真实 heartbeat 联调；
- renderer 任务另行定义 PNG 字体、尺寸、临时文件和外部 URL allowlist；
- Hermes 上游完善 tool-search `turn_id` 和 delegation 继承后，再评估是否扩大
  generic 工具范围；当前 `tool_search` 仍禁用，`delegate_task` 仅作为显式委派入口，
  generic 工具本身已经由 OneBot `pre_tool_call` 按 role snapshot 硬门禁。
