# Hermes OneBot 11 插件

让 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 通过 OneBot 11 协议接入 QQ 的官方插件。装好后,你可以直接在 QQ 私聊或群里和 Hermes 对话,还能让它在群里查询消息记录。

## 它能做什么

- **私聊**：和机器人一对一聊天,每条消息都会回复。
- **群聊**：整群共享一个 Hermes session；允许的消息先进入持久 SQLite 队列，再由 @、关键词或显式 LLM trigger 触发一轮处理。
- **处理指示器**：群 turn 认领后给触发消息添加 👀，Hermes turn 收尾时自动移除；没有真实消息 ID 或 QQ 框架不支持该扩展时按 best-effort 跳过，不影响回复。
- **上下文**：队列有条数、字节数和单条消息上限；当前 batch 的早期消息生成确定性摘要，最近消息保留原文，并作为一个 user turn 物化进共享 session。ack 后不再把同一批追加为跨轮摘要。
- **图片与消息段**：兼容 array/CQ 字符串，支持图片、reply、文件、语音、视频、转发和未知段标记；图片下载有 host、端口、类型、魔数和大小限制。
- **工具与管理**：提供当前群/私聊范围内的查询工具，以及撤回、禁言、踢人、全员禁言工具。写操作只生成预览，必须由同一超级管理员在同一目标群发送短期确认命令；权限可按精确工具名配置为 user、trusted_user、super_admin。
- **可靠性**：队列支持崩溃恢复、去重、lease heartbeat 和人工处理 `uncertain` 出站结果。OneBot 非幂等请求不自动重试，不承诺 exactly-once。

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
      media_allowed_hosts: []   # 图片 URL 必须命中此列表；默认还允许 HTTP API host
      media_allowed_ports: []   # 为空时使用 HTTP API 的端口
      llm_trigger:
        enabled: false
        provider: ""
        model: ""
        groups: []
      processing_reaction_enabled: true
      processing_reaction_emoji_id: "128064"  # LLBot 的 👀
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
LLM trigger 默认关闭，启用时必须同时配置明确的旁路 `provider`、`model` 和群 allowlist；缺少模型、超时或返回非法 JSON 都按“不触发”处理。
`media_orphan_ttl_seconds` 到期后由下一次 adapter 启动或 turn 收尾清理遗留媒体目录。
`processing_reaction_enabled` 默认开启；它使用 LLBot 的 `set_msg_emoji_like` 扩展，只作用于群聊真实消息 ID。添加或移除 reaction 的未知结果不会重放 Agent turn，也不会阻断队列 ack。

当前 Hermes 版本没有 request-only provider hook，因此动态时间/目标信息只会在宿主实现 `pre_provider_request` 后注入请求副本，不会用 `pre_llm_call` 伪装。队列 batch 本身直接作为当前 user turn 写入 session，历史消息因此可以继续命中 provider 的前缀缓存。

## 权限说明

群聊是 OneBot 11 的主要使用场景,权限分三层（详见 [docs/permissions.md](docs/permissions.md)）：

1. **谁能入队**：群消息先按 `allowed_groups` 判断；私聊必须满足 `allowlist`，或在 `open` 策略下显式配置 allow-all。
2. **谁能用工具**：`super_admins` 对应超级管理员；`roles.trusted_user.users` 指定受信用户；工具集合按精确工具名匹配，普通用户默认只有当前目标范围内的只读工具。
3. **会话范围**：工具和出站目标都绑定当前 `(session_id, turn_id)`，群里只能查/操作本群。
4. **写操作**：模型第一次调用只返回 `/onebot confirm TOKEN`；确认命令在入站层执行，不进入 session 或队列。

群内任何已授权用户都可以旁路发送 `/context`、`/status`、`/whoami`、`/help`、`/commands`；这些命令不进入队列或 Agent session。`/new`、`/reset`、`/restart`、`/model`、`/compress` 会被明确拒绝。群级运维命令由超级管理员直接发送：`/onebot status`、`queue`、`flush`、`clear`、`pause`、`resume`、`resolve retry|discard` 和 `confirm TOKEN`。`clear` 不删除 Hermes session 历史；`uncertain` 和 `failed` 都不会自动重试，必须明确 resolve。

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
