# Hermes OneBot 11 插件

让 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 通过 OneBot 11 协议接入 QQ 的官方插件。装好后,你可以直接在 QQ 私聊或群里和 Hermes 对话,还能让它在群里查询消息记录。

## 它能做什么

- **私聊**：和机器人一对一聊天,每条消息都会回复。
- **群聊**：整群共享一个 Hermes session；允许的消息先进入持久 SQLite 队列。每个 durable TurnAnchor 只消费自己边界内的批次，同群仍保持单活动 turn 并按 anchor 顺序串行处理；@、关键词、`always` 或分层 LLM trigger 才会创建 anchor。
- **连续对话**：成功回复后进入最多 60 秒的活跃窗口；窗口内普通消息统一交给低成本旁路模型判断是否回复（没有特例词），单窗口最多仲裁 2 次。debounce 是自适应的：群消息间隔超过 5 秒（不活跃）时立即判断，间隔小于 5 秒（活跃）时按 trailing 节流合并。
- **群级旁路命令**：`/context`、`/ctx` 在入队前返回有界队列/lease/policy 诊断；超级管理员可以发送 `/new [title]`、`/reset` 或 `/clear` 重置当前群的 shared session。它们都不会作为普通群消息交给 Agent。
- **处理指示器**：问句/记忆候选进入 selector 判断时给候选消息添加 👀（表示“bot 正在看这条消息”），判断结束（触发、忽略、超时或 wait 到期）后移除；任何触发方式进入回复阶段后，给触发消息添加 💬（表示“正在回复这一条”），Hermes turn 收尾时自动移除。两种指示器都是 best-effort，失败或结果未知不影响回复、队列 ack 或 Agent 完成，也不重放设置请求；没有真实消息 ID 或 QQ 框架不支持该扩展时按 best-effort 跳过。
- **中间正文**：Hermes ReAct 过程中产生的 AI 中间评论（commentary/工具进度/状态提示）默认在群聊隐藏、在私聊展示，可用 `show_interim_group` / `show_interim_dm` 配置。最终回复不受影响，永远发送。
- **回复格式**：默认把 Markdown 转成 OneBot 可读的纯文本；同一 turn 内重复的本地图片/URL/相同内容只投递一次。Markdown 图片逃生口 `[[onebot11:markdown-image]]...[[/onebot11:markdown-image]]` 目前只去掉 marker 并按纯文本发送，不访问其中的外部 URL。
- **运行时配置**：超级管理员可以发送 `/onebot reload` 热更新白名单、角色工具、trigger、cooldown、reaction、一次性长时间提示延迟和显示策略；HTTP/WS 地址、token、机器人 QQ 号、队列路径和 session 模式仍需重启。reload 后 active turn 保留创建时的权限快照，并清理旧确认令牌。
- **上下文**：队列有条数、字节数和单条消息上限，确认后形成滚动摘要，并保留最近消息原文；每条消息还带 `seq`、真实 `message_id`、去重 `message_key`、用户、role、reply 和媒体标记。当前批次作为普通 user message，摘要优先通过 Hermes `channel_prompt` 临时注入，不重复写入 shared session transcript。旧 Hermes 不支持时退回有界文本模式并记录审计。
- **图片与消息段**：兼容 array/CQ 字符串，支持图片、reply、文件、语音、视频、转发和未知段标记；入站图片下载有 host、端口、类型、魔数和大小限制，出站图片使用受限 `base64://` segment，适配 Hermes 宿主机与 LLBot 容器路径隔离。
- **工具与管理**：提供当前群/私聊范围内的查询工具，以及撤回、禁言、踢人、全员禁言工具。写操作只生成预览，必须由同一超级管理员在同一目标群发送短期确认命令。
- **可靠性**：队列支持崩溃恢复、去重、lease heartbeat 和人工处理 `uncertain` 出站结果。OneBot 非幂等请求不自动重试、unknown 不走 plain-text 或 cron standalone fallback，不承诺 exactly-once。

## 环境要求

- Hermes Agent（源码安装,版本 ≥ 0.20）
- Python ≥ 3.11
- Node.js ≥ 22.19（仅启用 LLM trigger 时需要）
- 一个支持 OneBot 11 的 QQ 框架,比如 [LLBot](https://luckylillia.com) 或 [NapCat](https://napneko.github.io/)

## 安装

```bash
hermes plugins install notnotype/hermes-plugin-onebot11
```

## 连接方式

本插件使用 OneBot 11 的**反向 WebSocket**（QQ 框架主动连过来）+ **HTTP API**（发消息走 HTTP）。

在你的 QQ 框架里开启两项（以 LLBot 为例,配置文件 `config_<QQ号>.json` 的 `ob11` 段）：

```jsonc
{
  "ob11": {
    "enable": true,
    "connect": [
      {
        "type": "ws-reverse",        // 反向 WS: 框架连到 Hermes
        "enable": true,
        "url": "ws://127.0.0.1:18880",
        "token": "你的token",         // 与 ONEBOT11_ACCESS_TOKEN 一致, 可留空
        "messageFormat": "array"
      },
      {
        "type": "http",              // HTTP: 发消息走这个
        "enable": true,
        "port": 3000,
        "token": "你的token",
        "messageFormat": "array"
      }
    ]
  }
}
```

> LLBot 跑在 Docker 里时,`url` 填 `ws://host.docker.internal:18880`,HTTP 地址同理。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `ONEBOT11_WS_PORT` | 18880 | 反向 WS 监听端口 |
| `ONEBOT11_WS_HOST` | 127.0.0.1 | 反向 WS 监听地址；非 loopback 必须配置 token |
| `ONEBOT11_ACCESS_TOKEN` | 空 | WS/HTTP 的 Bearer token,与框架侧一致 |
| `ONEBOT11_HTTP_API` | YAML 可替代 | QQ 框架的 OneBot 11 HTTP API 地址（通常为 `http://127.0.0.1:3000`）；也可写入 `extra.http_api` |
| `ONEBOT11_SELF_ID` | YAML 可替代 | 机器人 QQ 号（识别群里的 @）；也可写入 `extra.self_id` |
| `ONEBOT11_DM_POLICY` | open | 私聊策略：`open` / `allowlist` / `disabled`；`open` 仍需显式 allow-all |
| `ONEBOT11_ALLOWED_USERS` | 空 | 私聊白名单（逗号分隔的 QQ 号） |
| `ONEBOT11_ALLOWED_GROUPS` | 空 | 群白名单（逗号分隔的群号;空 = 所有群可用） |
| `ONEBOT11_REQUIRE_MENTION` | true | 群聊是否必须 @ 机器人 才创建 trigger；未触发消息仍入队 |
| `ONEBOT11_PLAIN_TEXT_ENABLED` | true | 默认将 Markdown 回复转换为纯文本；也可写入 `extra.plain_text_enabled` |
| `ONEBOT11_SUPER_ADMINS` | 空 | 超级管理员 QQ 列表；为空时写工具和管理命令全部 fail-closed |
| `ONEBOT11_ADMINS` | 空 | `ONEBOT11_SUPER_ADMINS` 的兼容旧名 |
| `ONEBOT11_ALLOW_ALL_USERS` | 空 | 明确允许 `dm_policy=open` 的私聊；也可使用 `GATEWAY_ALLOW_ALL_USERS=true` |
| `ONEBOT11_QUEUE_DB` | Hermes home | SQLite 队列路径；未配置时使用 Hermes home 下的 `onebot11/queue.sqlite3` |
| `ONEBOT11_HOME_CHANNEL` | 空 | 定时任务目标 ID；必须同时在 `platforms.onebot11.extra.home_channel_type` 指定 `group` 或 `dm` |
| `ONEBOT11_HOME_CHANNEL_TYPE` | 空 | 定时任务目标类型：`group` 或 `dm`；不会根据 QQ 号形状猜测 |
| `ONEBOT11_LLM_TRIGGER_PROVIDER` | YAML 可替代 | 旁路 provider；启用时必须明确配置 |
| `ONEBOT11_LLM_TRIGGER_MODEL` | YAML 可替代 | 旁路 model；启用时必须明确配置 |
| `ONEBOT11_LLM_TRIGGER_BASE_URL` | 空 | 自定义 provider 的 HTTP/HTTPS OpenAI-compatible 地址 |
| `ONEBOT11_LLM_TRIGGER_API_KEY_ENV` | 空 | API key 的环境变量名，不是密钥值 |
| `ONEBOT11_LLM_TRIGGER_GROUPS` | 空 | 允许调用 selector 的群号列表 |

启动网关时带上环境变量即可,例如：

```bash
ONEBOT11_WS_PORT=18880 \
ONEBOT11_HTTP_API=http://127.0.0.1:3000 \
ONEBOT11_SELF_ID=3101482118 \
hermes gateway run
```

队列和媒体的高级参数放在 Hermes `config.yaml` 的
`platforms.onebot11.extra` 中；LLM trigger 的 provider/model/key-env/group
也可以使用上面的环境变量。默认值已经适合单实例运行；生产部署通常只需要指定持久队列路径、共享 session 合同和恢复参数：

```yaml
platforms:
  onebot11:
    extra:
      session_mode: shared
      group_sessions_per_user: false
      home_channel: "1072992996"
      home_channel_type: group
      queue_lease_seconds: 120
      queue_recovery_poll_seconds: 5
      queue_max_messages: 1000
      queue_max_bytes: 2000000
      queue_max_message_bytes: 32000
      media_orphan_ttl_seconds: 86400
      max_image_bytes: 8000000
      max_image_total_bytes: 16000000
      max_images_per_message: 4
      media_allowed_hosts: []   # 图片 URL 必须命中此列表；默认还允许 HTTP API host
      media_allowed_ports: []   # 为空时使用 HTTP API 的端口
      media_source_roots: []    # get_image 返回的本地路径只允许来自这些绝对路径
      llm_trigger:
        enabled: false
        provider: deepseek
        model: deepseek-v4-flash
        base_url: ""
        api_key_env: DEEPSEEK_API_KEY
        groups: []
        timeout: 30
        input_bytes: 12000
        concurrency: 2
        max_failures: 3
        trigger_debounce_seconds: 5
        engaged_idle_seconds: 60
        engaged_max_seconds: 300
        engaged_max_arbitrations: 2
        # 三档 engage 预算（可选，默认值如下；只有预算不同，判断仍走 selector）
        tiers:
          shallow:                       # 普通闲聊：省 token、更快
            engaged_idle_seconds: 30
            max_seconds: 120
            max_arbitrations: 1
            timeout_seconds: 12
            input_bytes: 6000
          deep:                          # 重要任务：大预算
            engaged_idle_seconds: 180
            max_seconds: 900
            max_arbitrations: 4
            timeout_seconds: 45
            input_bytes: 20000
            wait_messages: 2             # waiting 攒满 2 条新消息立即判
        short_rule_max_chars: 0          # 0=关闭；>0 时 shallow 档短消息本地 ignore
      processing_reaction_enabled: true
      processing_reaction_emoji_id: "128172"  # LLBot 的 💬，表示正在回复
      show_interim_group: true    # 群聊展示 Hermes 中间正文（commentary/进度）
      show_interim_dm: true       # 私聊展示 Hermes 中间正文
      roles:
        user:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info]
        trusted_user:
          users: []
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_group_info]
        super_admin:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info, qq_delete_message,
                  qq_set_group_ban, qq_set_group_kick, qq_set_group_whole_ban]
```

`queue_recovery_poll_seconds` 只负责发现已过期 lease，不会抢占仍有效的 lease。正常主动断开时，
未开始出站的 lease 回到 pending 且不消耗失败预算；只有过期的明确 `agent_running` lease
才按最多 3 次的 2/4/8 秒退避恢复，达到上限进入 `failed`。出站已开始、阶段未知或租约阶段缺失进入 `uncertain`。

#### 独立 roles 文件（可选）

`roles`、`super_admins` 默认从 `platforms.onebot11.extra` 读取；如果 Hermes 对
`~/.hermes/config.yaml` 有写保护（代理工具无法修改），可以把角色配置放到独立文件：

```yaml
# ~/.hermes/onebot11/roles.yaml（默认路径；也可用 ONEBOT11_ROLES_FILE 或
# platforms.onebot11.extra.roles_file 指定其它路径）
super_admins:
- '2056963663'
roles:
  trusted_user:
    users:
    - '1259901822'
    - '1336488699'
    tools:
    - qq_get_message
    - qq_get_group_msg_history
    - image_generate
```

文件存在时，其 `roles` / `super_admins` / `admins` 整体覆盖 config.yaml 中的同名键，
启动、`validate_config` 和 `/onebot reload` 都使用同一解析路径；文件不存在则回退到
config.yaml，行为不变。文件必须是合法 YAML mapping，未知键会直接拒绝启动（fail-closed）。

LLM trigger 默认关闭，启用时必须同时配置明确的 `provider`、`model` 和群 allowlist；每群最多一个判断任务，使用 5 秒 debounce 和全局并发上限。判断由插件自有 Node helper 通过固定版本 `@earendil-works/pi-ai` 发起，不经过 Hermes auxiliary，不回退 Hermes 主 Agent，也不主动切换 provider。`api_key_env` 只保存环境变量名，密钥值不会进入 YAML、命令行、日志或 SQLite。Node、依赖缺失、超时、非法 JSON 或模型失败都按不触发处理，消息继续留在 pending。
旁路模型只接受 `{"decision":"trigger","anchor_seq":123}`、`{"decision":"wait","anchor_seq":null}` 或 `{"decision":"ignore","anchor_seq":null}`；`trigger` 必须选择真实 pending 消息的 seq，权限完全继承该消息。非法结果、超时或模型失败按不触发处理，并按持久化退避等待后续判断。

#### Engage 分级预算

成功回复后进入 engaged 时，插件会按上下文选择三档预算（只影响 debounce/窗口/仲裁次数/超时/输入大小，`trigger/wait/ignore` 三态合同不变）：

- `deep`（重要任务）：bot 上轮回复以问句/请求收尾且同用户回复、消息引用 bot 上一条回复、或命中任务词（报错/复现/日志等）。同用户 follow-up 免 debounce 立即判断；窗口 180s/900s、最多 4 次仲裁、超时 45s、输入 20KB；`wait` 状态下攒满 2 条新消息立即判，不等窗口到期。
- `normal`（默认）：现状 60s/300s、2 次仲裁、超时 30s、输入 12KB。
- `shallow`（省 token）：连续 2 次 ignore 后降级。窗口 30s/120s、1 次仲裁、超时 12s、输入 6KB。开启 `short_rule_max_chars` 后，shallow 档无信号的短消息（≤ N 字、非问句、无回指、未引用 bot、bot 未提问）本地判 ignore，不进 selector。

`bot_asked` 只由成功回复文本决定：回复以问句或明确的请求短语（"复现一下/发我/贴一下"等，只检查尾部 80 字）收尾时，bot 下一条同用户消息获得 deep 预算。他人插话不享受 deep，回落 normal。重启后所有档位回到 idle/normal，不持久化。
`media_orphan_ttl_seconds` 到期后由下一次 adapter 启动或 turn 收尾清理遗留媒体目录。
`processing_reaction_enabled` 默认开启；它使用 LLBot 的 `set_msg_emoji_like` 扩展，只作用于群聊真实消息 ID。回复阶段的 💬 使用 `processing_reaction_emoji_id`（默认 `128172`）；selector 判断阶段的 👀 使用固定 ID `128064`（LLBot/QQ 已验证支持；`9203` 即 ⏳ 与 `8971` 在 QQ reaction API 上显示异常）。添加或移除 reaction 的未知结果不会重放 Agent turn，也不会阻断队列 ack。

长时间运行提示不通过匹配 `⏳ Working` 等文本识别。只有 Hermes 提供
`hermes_control_plane=true` 且 `hermes_control_kind=long_running`，或未来的
`hermes_system_error_notice=true` 时，OneBot 才把它作为控制面消息发送；
控制面消息不写业务 `outbound_started`，同一 turn 只发送一次。当前 Hermes
若没有这些 metadata，不承诺发送 heartbeat，避免把系统通知误判为业务回复。

出站图片的主要支持场景是 Agent 最终回复（best-effort）：插件只接受 Hermes 允许的媒体
目录中的 PNG、JPEG、GIF、WebP，发送前编码为 `base64://` OneBot image
segment，并可带 caption/reply。Hermes 是否提供媒体结果聚合不影响插件启动；
图片返回 unknown 时，插件不会把图片 URL/路径发成普通文本，也不会自动重发整轮 Agent。
通用 `send_message`、cron 和跨进程 standalone sender 的 plugin media
目前不是本插件的可靠性合同；这些窄路径保持文本能力和安全降级，不影响主 Agent
回复链路。

## 权限说明

群聊是 OneBot 11 的主要使用场景,权限分三层（详见 [docs/permissions.md](docs/permissions.md)）：

1. **谁能入队**：群消息先按 `allowed_groups` 判断；私聊必须满足 `allowlist`，或在 `open` 策略下显式配置 allow-all。
2. **谁能用工具**：`super_admins` 对应超级管理员；`roles.trusted_user.users` 可把指定 QQ 号标记为 trusted_user。trusted_user 不能使用 OneBot 群管理写工具，但可按工具名显式获得 Hermes generic 能力。
3. **会话范围**：工具和出站目标都绑定当前 `(session_id, turn_id)`，群里只能查/操作本群。
4. **写操作**：模型第一次调用只返回 `/onebot confirm TOKEN`；确认命令在入站层执行，不进入 session 或队列。

同一 anchor 批次可能来自多个用户，但权限主体始终是该 anchor 对应真实消息的触发用户；
其他用户消息只作为不可信上下文，不会把普通用户升级为超级管理员，也不会改变当前群目标。
OneBot turn 的 generic Hermes 工具同样经过 `pre_tool_call` 硬门禁；普通用户不能因为
工具名不是 `qq_*` 就绕过角色配置。`delegate_task` 可以作为显式 generic 能力授予，
用于把需要执行环境的工作交给 Hermes 子代理；`tool_search` 仍始终拒绝。若启用
`main_agent_read_only`，主 agent 只能直接使用 `read_file`、`search_files`（内容搜索和
glob 文件搜索，等价于 Grep/Glob）、网页/视觉等只读工具以及 `delegate_task`；
子代理才可使用项目所需的 `terminal`、`process`、`write_file` 和 `patch`，并且不能
调用 OneBot QQ 工具、发送消息或再次委派。`search_files(target=content)` 是内容
正则搜索，`search_files(target=files)` 是 glob 文件搜索，因此主 agent 不需要直接
执行 `rg`。只读模式不限制 QQ 群管理写工具（仍需超级管理员 + 确认令牌），但
`read_file` 会拒绝读取 `.env`/`auth.json`/`auth.lock` 凭据文件。

群级运维命令由超级管理员直接发送：`/onebot status`、`queue`、`flush`、`clear`、`pause`、`resume`、`resolve retry|discard`、`resolve action retry|discard OPERATION_ID` 和 `confirm TOKEN`。`clear` 不删除 Hermes session 历史，但会同时失效当前群旧的 debounce/活跃触发状态；`uncertain` 和 `failed` 都不会自动重试，必须明确 resolve。队列消息的 `resolve retry` 只在旧 anchor 的 authority、真实消息和 batch 仍可证明时生成新的 request_id，并保留原权限；无法证明的旧记录保持 hold，不能由管理员身份或后续 selector 猜测接管。管理动作 `unknown` 的 `retry` 只解除重复执行阻断，之后仍需重新生成预览并确认，不会直接重放。

## 开发

```bash
# 安装开发依赖
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
npm ci

# 测试 + lint + helper 语法
pytest -q
ruff check .
node --check scripts/onebot11-pi-trigger.mjs
```

需要真实 Hermes gateway 的 adapter、hooks、shared session、同实例 reconnect
和插件自有 pi-ai helper 验收时，运行：

```powershell
.\scripts\verify_hermes_integration.ps1 `
  -HermesSource C:\path\to\hermes-agent
```

Linux/macOS 可直接运行同一个 Python 入口：

```bash
python scripts/verify_hermes_integration.py \
  --hermes-source /path/to/hermes-agent
```

该命令使用临时 `HERMES_HOME`，会真实收集平台、9 个工具、5 个 hooks，
并执行 shared session、pending trigger 恢复、显式 home cron、schema recovery、
reconnect、worker-thread binding 恢复、图片 base64 segment 和 pi-ai helper 合同 smoke；
不会把测试队列、审计或 session 写入真实 Hermes home。CI 只负责插件可安装、`onebot11/`
协议/状态机测试和 Ruff；Hermes 组合测试是本地验收证据。纯插件环境只保证
`import onebot11`，根目录 `adapter.py` 需要 Hermes gateway 依赖。

## License

MIT
