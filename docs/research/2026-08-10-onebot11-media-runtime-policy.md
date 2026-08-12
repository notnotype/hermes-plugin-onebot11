# OneBot 11 媒体、控制面与运行时策略调研

> 调研日期：2026-08-10。本文记录本地代码和 Hermes 源码的合同核对结果，不把
> 合成 WS payload 或单元测试当成真人 QQ 联调证据。

## 结论

本轮最小可行边界是：

- 最终图片在 OneBot adapter 出站边界做同轮防御性去重；
- 默认回复转换为纯文本；
- Markdown 图片只定义 marker，不在 adapter 内引入浏览器或 renderer；
- 控制面消息必须由 Hermes metadata 明确标记；
- 权限和 trigger 策略使用不可变 snapshot 运行时替换。

这解决用户可见的重复图片、Markdown 泄漏和权限配置必须重启三个问题，
没有把 OneBot 变成新的通用调度框架，也没有修改 Hermes 主循环。

## 媒体边界

`MediaDeliveryScope` 的 scope 生命周期是当前 lease、精确 DM turn 或当前消息
fallback。它同时保存来源 fingerprint 和图片内容 fingerprint，但只保存受限 hash，
不保存完整路径、URL 或图片内容。

这能覆盖：

- 同一 URL 的重复提取；
- Windows 路径大小写、分隔符和空格差异；
- URL 与本地文件实际内容相同。

它不能也不应该承诺：

- 跨 turn 的永久去重；
- 跨进程/跨重启 exactly-once；
- Hermes 在更上游已经把同一媒体合并成单条结果。

出站路径仍只接受 Hermes 允许的媒体根目录、PNG/JPEG/GIF/WebP、
大小和魔数校验；OneBot Bearer token 只发给 OneBot HTTP API，不发给媒体 URL。

## 文本边界

OneBot/QQ 不负责渲染 Hermes Markdown，因此 adapter 默认把标题、强调、代码围栏、
反引号和 Markdown 链接转为普通文本。代码内容、列表、中文和 URL 保留。

marker：

```text
[[onebot11:markdown-image]]
Markdown 内容
[[/onebot11:markdown-image]]
```

当前版本的处理是移除 marker、继续纯文本化并记录
`markdown_image_requested_unavailable`。不会启动浏览器，不会下载 marker 内的 URL，
也不会把 marker 原样发送给 QQ。真正的转图片 renderer 另设任务，需独立定义字体、
尺寸、超时、临时目录和外部 URL 策略。

## 控制面消息

不能通过匹配 `⏳ Working` 之类的文本区分 Hermes 系统通知，因为普通 Agent 回复
可能恰好包含相同内容。当前 adapter 只接受：

```python
{"hermes_control_plane": True, "hermes_control_kind": "long_running"}
```

以及未来兼容的：

```python
{"hermes_system_error_notice": True}
```

控制面消息不写业务 `outbound_started`，不影响 OneBot queue ack/release/uncertain；
同一 turn 只发送一次，并在分块之间重新检查 lease、adapter 状态和白名单。
在 Hermes 上游 metadata 合同完成前，不声称 OneBot 已自动发送 heartbeat。

## 运行时 reload

Hermes 的普通 `hermes_cli.config.load_config()` 是全局 Agent 配置；平台运行时的
canonical loader 是 `gateway.config.load_gateway_config()`，它返回已经合并环境变量、
YAML、legacy gateway 配置和 plugin platform extra 的 `GatewayConfig`。

因此 adapter reload 优先读取后者的 `PlatformConfig.extra`，旧 Hermes 没有该入口时
才回退到通用 loader。OneBot 不重新实现 Hermes 的 YAML precedence。环境变量继续是
覆盖层，所以被环境变量锁定的白名单字段不会被 YAML reload 覆盖。

热字段包括白名单、角色工具、trigger、cooldown、reaction 和纯文本显示；
HTTP/WS 地址、token、self_id、queue path、session mode 等静态字段要求重启。
active turn 的 authority/tool snapshot 不因 reload 中途扩大，但每次工具和出站仍复查
当前目标白名单。reload 成功会使旧确认 token 失效。

## Hermes 上游任务

以下能力不在本仓库 hack：

- `long_running` 和系统错误通知携带显式 control-plane metadata；
- pre-tool hook 异常的宿主级 fail-closed 选项；
- tool-search bridge 传递 `session_id`、`turn_id`、`api_request_id`；
- per-turn `allowed_tool_names` 继承到 tool search、execute_code 和 delegation 子 Agent。

在这些能力完成前，OneBot 不开启动态 `tool_search`，也不把 role catalog 伪装成
通用 Hermes 工具授权。`delegate_task` 仅作为显式委派入口：主 agent 只读时可由角色
配置授予，子代理使用项目工具但不能调用 QQ、发送消息或再次委派；缺少父 binding
时仍 fail-closed。Docker 子代理、OneBot 12、语音转写和语义摘要继续不纳入本轮。
