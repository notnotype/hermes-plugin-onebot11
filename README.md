# Hermes OneBot 11 插件

让 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 通过 OneBot 11 协议接入 QQ 的官方插件。装好后,你可以直接在 QQ 私聊或群里和 Hermes 对话,还能让它在群里查询消息记录。

## 它能做什么

- **私聊**：和机器人一对一聊天,每条消息都会回复。
- **群聊**：整群共享一个 Hermes session；允许的消息先进入持久 SQLite 队列。每个 durable TurnAnchor 只消费自己边界内的批次，同群仍保持单活动 turn 并按 anchor 顺序串行处理；@、关键词、`always` 或分层 LLM trigger 才会创建 anchor。
- **连续对话**：成功回复后进入最多 60 秒的活跃窗口；窗口内普通消息经过 5 秒 trailing debounce，再交给低成本旁路模型判断是否回复，单窗口最多仲裁 3 次。
- **处理指示器**：群 turn 认领后给触发消息添加 👀，Hermes turn 收尾时自动移除；没有真实消息 ID 或 QQ 框架不支持该扩展时按 best-effort 跳过，不影响回复。
- **上下文**：队列有条数、字节数和单条消息上限，确认后形成滚动摘要，并保留最近消息原文；当前批次作为普通 user message，摘要优先通过 Hermes `channel_prompt` 临时注入，不重复写入 shared session transcript。旧 Hermes 不支持时退回有界文本模式并记录审计。
- **图片与消息段**：兼容 array/CQ 字符串，支持图片、reply、文件、语音、视频、转发和未知段标记；入站图片下载有 host、端口、类型、魔数和大小限制，出站图片使用受限 `base64://` segment，适配 Hermes 宿主机与 LLBot 容器路径隔离。
- **工具与管理**：提供当前群/私聊范围内的查询工具，以及撤回、禁言、踢人、全员禁言工具。写操作只生成预览，必须由同一超级管理员在同一目标群发送短期确认命令。
- **可靠性**：队列支持崩溃恢复、去重、lease heartbeat 和人工处理 `uncertain` 出站结果。OneBot 非幂等请求不自动重试、unknown 不走 plain-text 或 cron standalone fallback，不承诺 exactly-once。

## 环境要求

- Hermes Agent（源码安装,版本 ≥ 0.20）
- Python ≥ 3.11
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
| `ONEBOT11_SUPER_ADMINS` | 空 | 超级管理员 QQ 列表；为空时写工具和管理命令全部 fail-closed |
| `ONEBOT11_ADMINS` | 空 | `ONEBOT11_SUPER_ADMINS` 的兼容旧名 |
| `ONEBOT11_ALLOW_ALL_USERS` | 空 | 明确允许 `dm_policy=open` 的私聊；也可使用 `GATEWAY_ALLOW_ALL_USERS=true` |
| `ONEBOT11_QUEUE_DB` | Hermes home | SQLite 队列路径；未配置时使用 Hermes home 下的 `onebot11/queue.sqlite3` |
| `ONEBOT11_HOME_CHANNEL` | 空 | 定时任务目标 ID；必须同时在 `platforms.onebot11.extra.home_channel_type` 指定 `group` 或 `dm` |
| `ONEBOT11_HOME_CHANNEL_TYPE` | 空 | 定时任务目标类型：`group` 或 `dm`；不会根据 QQ 号形状猜测 |

启动网关时带上环境变量即可,例如：

```bash
ONEBOT11_WS_PORT=18880 \
ONEBOT11_HTTP_API=http://127.0.0.1:3000 \
ONEBOT11_SELF_ID=3101482118 \
hermes gateway run
```

队列、媒体和 LLM trigger 的高级参数放在 Hermes `config.yaml` 的
`platforms.onebot11.extra` 中（不是环境变量）。默认值已经适合单实例运行；生产部署通常只需要指定持久队列路径、共享 session 合同和恢复参数：

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
      llm_trigger:
        enabled: false
        provider: ""
        model: ""
        groups: []
        timeout: 10
        input_bytes: 12000
        concurrency: 2
        trigger_debounce_seconds: 5
        engaged_idle_seconds: 60
        engaged_max_seconds: 300
        engaged_max_arbitrations: 3
      processing_reaction_enabled: true
      processing_reaction_emoji_id: "128064"  # LLBot 的 👀
      roles:
        user:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info]
        trusted_user:
          users: ["2056963663"]
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_group_info]
        super_admin:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info, qq_delete_message,
                  qq_set_group_ban, qq_set_group_kick, qq_set_group_whole_ban]
```

`queue_recovery_poll_seconds` 只负责发现已过期 lease，不会抢占仍有效的 lease。正常主动断开时，
未开始出站的 lease 回到 pending 且不消耗失败预算；只有过期的明确 `agent_running` lease
才按最多 3 次的 2/4/8 秒退避恢复，达到上限进入 `failed`。出站已开始、阶段未知或租约阶段缺失进入 `uncertain`。
LLM trigger 默认关闭，启用时必须同时配置明确的旁路 `provider`、`model` 和群 allowlist；每群最多一个判断任务，使用 5 秒 debounce 和全局并发上限。判断调用固定使用 `fallback_policy=none`、`max_attempts=1`，不支持这两个 Hermes auxiliary 参数的旧版本会安全禁用 LLM trigger，不回退主模型。缺少模型、超时或返回非法 JSON 都按“不触发”处理。
旁路模型只接受 `{"decision":"trigger|wait|ignore","wait_seconds":0}`；`wait` 时 `wait_seconds` 只能是 `5/10/30/60`，`trigger` 和 `ignore` 必须使用 `0`。非法结果保留队列消息，不创建 lease。
`media_orphan_ttl_seconds` 到期后由下一次 adapter 启动或 turn 收尾清理遗留媒体目录。
`processing_reaction_enabled` 默认开启；它使用 LLBot 的 `set_msg_emoji_like` 扩展，只作用于群聊真实消息 ID。添加或移除 reaction 的未知结果不会重放 Agent turn，也不会阻断队列 ack。

出站图片只接受 Hermes 允许的媒体目录中的 PNG、JPEG、GIF、WebP，发送前
编码为 `base64://` OneBot image segment，并可带 caption/reply。旧 Hermes
缺少媒体结果合同或返回 unknown 时，插件不会把图片 URL/路径发成普通文本，
也不会自动重发整轮 Agent。

## 权限说明

群聊是 OneBot 11 的主要使用场景,权限分三层（详见 [docs/permissions.md](docs/permissions.md)）：

1. **谁能入队**：群消息先按 `allowed_groups` 判断；私聊必须满足 `allowlist`，或在 `open` 策略下显式配置 allow-all。
2. **谁能用工具**：`super_admins` 对应超级管理员；`roles.trusted_user.users` 可把指定 QQ 号标记为 trusted_user，但它和普通用户一样只能使用只读工具。
3. **会话范围**：工具和出站目标都绑定当前 `(session_id, turn_id)`，群里只能查/操作本群。
4. **写操作**：模型第一次调用只返回 `/onebot confirm TOKEN`；确认命令在入站层执行，不进入 session 或队列。

同一 anchor 批次可能来自多个用户，但权限主体始终是该 anchor 对应真实消息的触发用户；
其他用户消息只作为不可信上下文，不会把普通用户升级为超级管理员，也不会改变当前群目标。

群级运维命令由超级管理员直接发送：`/onebot status`、`queue`、`flush`、`clear`、`pause`、`resume`、`resolve retry|discard`、`resolve action retry|discard OPERATION_ID` 和 `confirm TOKEN`。`clear` 不删除 Hermes session 历史，但会同时失效当前群旧的 debounce/活跃触发状态；`uncertain` 和 `failed` 都不会自动重试，必须明确 resolve。管理动作 `unknown` 的 `retry` 只解除重复执行阻断，之后仍需重新生成预览并确认，不会直接重放。

## 开发

```bash
# 安装开发依赖
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 测试 + lint
pytest -q
ruff check .
```

需要真实 Hermes gateway 的 adapter、hooks、shared session、strict auxiliary 和同实例 reconnect 验收时，运行：

```powershell
.\scripts\verify_hermes_integration.ps1 `
  -HermesSource C:\path\to\hermes-agent `
  -HermesAuxiliarySource C:\path\to\hermes-agent-auxiliary-no-fallback
```

Linux/macOS 可直接运行同一个 Python 入口：

```bash
python scripts/verify_hermes_integration.py \
  --hermes-source /path/to/hermes-agent \
  --hermes-auxiliary-source /path/to/hermes-agent-auxiliary-no-fallback
```

该命令使用临时 `HERMES_HOME`，会真实收集平台、9 个工具、4 个 hooks、`onebot11_trigger` auxiliary，
并执行 shared session、pending trigger 恢复、显式 home cron 和 reconnect smoke；不会把测试队列、
审计或 session 写入真实 Hermes home。CI 只负责插件可安装、`onebot11/` 协议/状态机测试和 Ruff；
Hermes 组合测试是本地验收证据。纯插件环境只保证 `import onebot11`，根目录 `adapter.py` 需要 Hermes gateway 依赖。

## License

MIT
