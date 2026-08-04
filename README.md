# Hermes OneBot 11 插件

让 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 通过 OneBot 11 协议接入 QQ 的官方插件。装好后,你可以直接在 QQ 私聊或群里和 Hermes 对话,还能让它在群里查询消息记录。

## 它能做什么

- **私聊**：和机器人一对一聊天,每条消息都会回复。
- **群聊**：在群里 @ 机器人触发对话,整群共享一个对话上下文,发言前会自动带上昵称。
- **图片**：收到的图片会存到本地,供 Hermes 的视觉能力使用。
- **查消息**：提供几个查询工具,可以让它在群里查最近的消息记录、按消息 ID 查单条消息。
- **权限控制**：管理员列表 + 会话范围校验——群里的机器人只能查它自己所在群的消息,查不到别的群,更查不到陌生人的私聊。

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
| `ONEBOT11_ACCESS_TOKEN` | 空 | WS/HTTP 的 Bearer token,与框架侧一致 |
| `ONEBOT11_HTTP_API` | `http://127.0.0.1:3000` | QQ 框架的 HTTP API 地址 |
| `ONEBOT11_SELF_ID` | 空 | 机器人 QQ 号（识别群里的 @） |
| `ONEBOT11_DM_POLICY` | open | 私聊策略：`open` / `allowlist` / `disabled` |
| `ONEBOT11_ALLOWED_USERS` | 空 | 私聊白名单（逗号分隔的 QQ 号） |
| `ONEBOT11_ALLOWED_GROUPS` | 空 | 群白名单（逗号分隔的群号;空 = 所有群可用） |
| `ONEBOT11_REQUIRE_MENTION` | true | 群聊是否必须 @ 机器人 才响应 |
| `ONEBOT11_ADMINS` | 空 | 管理员 QQ 列表（空 = 所有已授权用户同权） |
| `ONEBOT11_HOME_CHANNEL` | 空 | 定时任务默认投递目标（群号或 QQ 号） |

启动网关时带上环境变量即可,例如：

```bash
ONEBOT11_WS_PORT=18880 \
ONEBOT11_HTTP_API=http://127.0.0.1:3000 \
ONEBOT11_SELF_ID=3101482118 \
hermes gateway run
```

## 权限说明

群聊是 OneBot 11 的主要使用场景,权限分三层（详见 [docs/permissions.md](docs/permissions.md)）：

1. **谁能对话**：群聊默认群里任何人都能 @ 机器人;私聊由 `ONEBOT11_DM_POLICY` 控制。
2. **谁能用工具**：管理员列表（`ONEBOT11_ADMINS`）区分普通用户和管理员。
3. **会话范围**：工具只能作用于发起会话本身——群里只能查本群消息。

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
