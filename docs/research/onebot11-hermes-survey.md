# OneBot 11 × Hermes 适配器调研

> 调研日期：2026-08-04。来源：OneBot 规范仓库、Hermes 官方文档与源码、GitHub 生态。

## OneBot 11 协议要点

- 规范仓库：botuniverse/onebot-11（688★,维护中）。
- 四种通信方式（统一 UTF-8 JSON）：
  - HTTP：OneBot 做服务端,外部调 API。
  - HTTP POST：OneBot 做客户端,把事件 POST 到你的 URL。
  - 正向 WS：OneBot 做 WS 服务端。
  - 反向 WS：OneBot 做 WS 客户端,主动拨到你提供的 WS 服务端（**本插件采用**）。
- 消息格式：字符串（CQ 码）或数组（消息段 `[{type,data}]`）；本插件两者都兼容，并统一保留 reply/媒体/未知段标记。
- 事件四类：message / notice / request / meta（lifecycle、heartbeat）。
- API：`{"action","params","echo"}` JSON POST → `{"status","retcode","data","echo"}`。
- 鉴权：`Authorization: Bearer <token>`。
- OneBot 12 是新一代跨平台标准,但 QQ 生态（NapCat / LLBot / go-cqhttp / Lagrange）主流仍是 11。

## LLBot 侧配置（本机部署）

LLBot 支持 OneBot11/GoCQ、Milky、Satori。ob11 段可配置：正向 WS 3001 / 反向 WS / HTTP 3000 / HTTP-POST,各自独立开关,token 可空,`messageFormat: array`。LLBot 跑在 Docker,compose 已带 `host.docker.internal:host-gateway`,反向 WS 填 `ws://host.docker.internal:<port>` 即可。

## Hermes 适配器机制（v0.20.0 实测源码）

- 插件 = `~/.hermes/plugins/<name>/` 下 `plugin.yaml` + `adapter.py` + `__init__.py`。
- `hermes plugins install <git-url>`：`git clone --depth 1` 到临时目录,无 subdir 时 **repo 根即插件目录**,插件名取 `plugin.yaml` 的 `name` 字段,整个目录 `shutil.move` 到 `~/.hermes/plugins/<name>/`。也支持 `owner/repo`、`owner/repo/subdir`、浏览器 URL。
- `BasePlatformAdapter`（gateway/platforms/base.py:2629）：`connect/disconnect/send/send_typing/get_chat_info`;入站构造 `MessageEvent`（base.py:2054）调 `self.handle_message(event)`。
- `ctx.register_platform(...)` 一次挂满集成点：`env_enablement_fn`、`allowed_users_env`/`allow_all_env`、`cron_deliver_env_var` + `standalone_sender_fn`、`max_message_length`、`platform_hint`、`emoji` 等。
- `ctx.register_tool(name, toolset, schema, handler)` 注册平台专属工具。
- 参考实现：`plugins/platforms/irc/`（995 行零依赖插件骨架 + token 锁）。

## GitHub 生态现状

| 项目 | 活跃度 | 结论 |
|---|---|---|
| hermes-napcat（shubyi） | 2026-04-21 一天写完,停更 3.5 个月;11★ | patch 式改 Hermes 核心,升级即坏;不采用,借逻辑 |
| NousResearch/hermes-agent PR #17917 | 2026-04-30 开,Open 未合并 | 官方内置 NapCat 适配器未落地 |
| constansino/hermes_qq | 18★,2026-07 仍在更新 | 桥接器（走 Hermes API Server）,非 gateway 插件 |
| nonebot/adapter-onebot | 119★,活跃 | NoneBot 生态,协议模型参考 |
| openclaw-onebot | — | OpenClaw 插件,证明各 agent 框架都在做 OneBot |

## 决策记录

1. 走官方插件路径（零核心改动）,不采用 hermes-napcat 的 patch 方式。
2. 架构 = 反向 WS 收事件 + HTTP API 发送（与 LLBot ob11 配置同构）。
3. repo 根即插件目录,`hermes plugins install notnotype/hermes-plugin-onebot11` 直接可装。
4. v1 只做消息闭环 + 三个查询工具;群管写操作、notice/request 事件、正向 WS、语音转写均后置。
