# Task 7：OneBot 11 媒体投递、状态提示、纯文本与运行时策略

- 日期：2026-08-10
- 状态：实现中；本 worktree 尚未合并或部署 Arch
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

## 计划出入

- 本轮没有修改本地 Hermes，也没有实现 renderer、Docker 子代理、OneBot 12、原始 WS spool、
  语义摘要或通用媒体发送；
- Hermes heartbeat 在上游提供明确 control-plane metadata 前不作为 OneBot 业务合同；
- Hermes 通用 web/browser/terminal/file 工具不由本插件伪造 per-turn policy；它们仍须通过
  Hermes platform toolset 和上游 turn 传递合同隔离。OneBot 角色目录只覆盖本插件注册的 OneBot 工具。

## 验证

本 worktree 使用本地 Hermes 源码：

```text
PYTHONPATH=C:\Users\notnotype\AppData\Local\hermes\hermes-agent pytest -q
289 passed, 1 skipped
ruff check .
```

skip 只表示没有 Hermes gateway 的纯插件环境会跳过 `tests/test_adapter.py`；
完整 adapter 测试必须使用上述 Hermes 源码路径，不能把 skip 当成通过。

Arch 联调仍严格限制为群 `1072992996`、私聊用户 `2056963663`；
本任务未在该 worktree 部署或发送未经授权的管理动作。合成 WS payload
与真人 QQ 客户端消息必须分开记录，前者不升级为 OneBot 11 协议保证。

## 后续

- Hermes 上游增加明确的 long-running/system-error control-plane metadata 后，再开启
  OneBot 控制面通知的真实 heartbeat 联调；
- renderer 任务另行定义 PNG 字体、尺寸、临时文件和外部 URL allowlist；
- Hermes 上游完成 per-turn tool policy、tool-search `turn_id` 和 delegation 继承后，
  再评估把通用高风险工具纳入 OneBot role snapshot。
