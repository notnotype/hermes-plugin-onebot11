# Hermes OneBot 11 插件

让 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 通过 OneBot 11 协议接入 QQ 的官方插件。装好后,你可以直接在 QQ 私聊或群里和 Hermes 对话,还能让它在群里查询消息记录。

## 它能做什么

- **私聊**：和机器人一对一聊天,每条消息都会回复。
- **群聊**：整群共享一个 Hermes session；每条明确触发消息形成独立 TurnAnchor，并按序启动独立 followup。
- **处理指示器**：锚点持久排队后显示 ⏳，认领后切换为 👀，turn 收尾自动移除；框架不支持时按 best-effort 跳过。
- **上下文**：每个 batch 截止到自己的 `anchor_seq`，以结构化 JSONL 物化消息 ID、发送者、role、reply 和媒体标记；锚点之后的消息不会越界进入当前 turn。
- **图片与消息段**：兼容 array/CQ 字符串，支持图片、reply、文件、语音、视频、转发和未知段标记；图片下载有 host、端口、类型、魔数和大小限制。
- **工具与管理**：authority 完全继承锚点消息发送者的 turn-start 角色快照。角色允许、目标和 lease 有效时写工具直接执行；unknown 不自动重试。
- **可靠性**：队列支持崩溃恢复、去重、lease heartbeat 和人工处理 `uncertain` 出站结果。OneBot 非幂等请求不自动重试，不承诺 exactly-once。
- **安全边界**：白名单同时约束实时入站、cron 和恢复；普通 OneBot caller 不能跨到 subagent。`delegate_task` 在 Hermes 支持 per-turn 工具权限前禁止配置和调用。

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
| `ONEBOT11_REQUIRE_MENTION` | true | 兼容开关；false 只启用自动锚点候选，不直接授予消息发送者 authority |
| `ONEBOT11_SUPER_ADMINS` | 空 | 超级管理员 QQ 列表；为空时写工具和管理命令全部 fail-closed |
| `ONEBOT11_ADMINS` | 空 | `ONEBOT11_SUPER_ADMINS` 的兼容旧名 |
| `ONEBOT11_ALLOW_ALL_USERS` | 空 | 明确允许 `dm_policy=open` 的私聊；也可使用 `GATEWAY_ALLOW_ALL_USERS=true` |
| `ONEBOT11_QUEUE_DB` | Hermes home | SQLite 队列路径；未配置时使用 Hermes home 下的 `onebot11/queue.sqlite3` |
| `ONEBOT11_HOME_CHANNEL` | 空 | 定时任务目标 ID；必须同时在 `platforms.onebot11.extra.home_channel_type` 指定 `group` 或 `dm` |

启动网关时带上环境变量即可,例如：

```bash
ONEBOT11_WS_PORT=18880 \
ONEBOT11_HTTP_API=http://127.0.0.1:3000 \
ONEBOT11_SELF_ID=3101482118 \
hermes gateway run
```

队列、媒体和自动锚点 selector 的高级参数放在 Hermes `config.yaml` 的
`platforms.onebot11.extra` 中（不是环境变量）。默认值已经适合单实例运行；生产部署通常只需要指定持久队列路径、共享 session 合同和恢复参数：

```yaml
platforms:
  onebot11:
    extra:
      session_mode: shared
      group_sessions_per_user: false
      queue_lease_seconds: 120
      queue_recovery_poll_seconds: 5
      queue_max_messages: 1000
      queue_max_bytes: 2000000
      queue_max_message_bytes: 32000
      agent_input_bytes: 65536
      agent_recent_originals: 3
      media_orphan_ttl_seconds: 86400
      max_image_bytes: 8000000
      max_image_total_bytes: 16000000
      media_allowed_hosts: []   # 图片 URL 必须命中此显式列表；不会隐式信任 HTTP API host
      media_allowed_ports: []   # 为空时仅允许 API host 的 API 端口；其他 host/端口需显式配置
      media_source_roots: []    # get_image 返回的本地文件只能来自这些显式根目录
      llm_trigger:
        enabled: false
        provider: ""
        model: ""
        groups: []
      processing_reaction_enabled: true
      processing_reaction_emoji_id: "128064"  # LLBot 的 👀
      queued_reaction_enabled: true
      queued_reaction_emoji_id: "9203"         # LLBot 的 ⏳，以实际框架为准
      roles:
        user:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info]
        trusted_user:
          users: ["2056963663"]
          tools: [web_search, web_extract, browser_navigate]
        super_admin:
          tools: [onebot_get_permissions, onebot_set_role_tools,
                  onebot_set_trusted_users, qq_get_message, qq_get_group_msg_history,
                  qq_get_group_info, qq_get_group_member_info, qq_delete_message,
                  qq_set_group_ban, qq_set_group_kick, qq_set_group_whole_ban]
```

`queue_recovery_poll_seconds` 只负责发现已过期 lease，不会抢占仍有效的 lease。
自动锚点 selector 仍沿用 `llm_trigger` 配置名以兼容 0.3.x。它默认关闭，启用时必须配置明确的旁路 `provider`、`model` 和群 allowlist。模型只能返回一个现存 `anchor_seq` 或 null，看不到角色和工具配置，也不能授予权限。
`media_orphan_ttl_seconds` 到期后由下一次 adapter 启动或 turn 收尾清理遗留媒体目录。
`processing_reaction_enabled` 默认开启；它使用 LLBot 的 `set_msg_emoji_like` 扩展，只作用于群聊真实消息 ID。添加或移除 reaction 的未知结果不会重放 Agent turn，也不会阻断队列 ack。
reaction 的清理状态持久化在队列数据库中；重启或恢复只会有限重试 `unset`，不会重放 `set`、Agent turn 或群管理动作。非 URL 图片会先调用 OneBot `get_image`，只有返回路径位于 `media_source_roots` 时才复制到临时目录。

authority reminder 由 `pre_llm_call` 追加到当前 user request，不修改稳定 system prompt；Hermes 保存该 turn 的 wire sidecar，后续历史可按相同字节重放。它只是模型提醒，真实权限由 binding、lease 和不可变工具快照校验。时间等易变信息仍只适合 request-only 动态上下文。

## 权限说明

群聊是 OneBot 11 的主要使用场景,权限分三层（详见 [docs/permissions.md](docs/permissions.md)）：

1. **谁能入队**：群消息先按 `allowed_groups` 判断；私聊必须满足 `allowlist`，或在 `open` 策略下显式配置 allow-all。
2. **谁能用工具**：`super_admins` 对应超级管理员；`roles.trusted_user.users` 指定受信用户；工具集合按精确工具名匹配，普通用户默认只有当前目标范围内的只读工具。
3. **会话范围**：工具和出站目标都绑定当前 `(session_id, turn_id)`，群里只能查/操作本群。
4. **写操作**：按锚点 authority 直接硬校验并执行；同一 turn 的同一 unknown 动作禁止自动重复调用。

群内任何已授权用户都可以旁路发送 `/context`、`/status`、`/whoami`、`/help`、`/commands`；这些命令不进入队列或 Agent session。`/new`、`/reset`、`/restart`、`/model`、`/compress` 会被明确拒绝。群级运维命令由超级管理员直接发送：`/onebot status`、`queue`、`flush`、`clear`、`pause`、`resume`、`resolve retry|discard`。`flush` 创建继承命令管理员权限的 operator anchor；`clear` 不删除 Hermes session 历史。

## 开发

```bash
# 安装开发依赖
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 测试 + lint
pytest -q
ruff check .
```

## License

MIT
