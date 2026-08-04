# OneBot 11 接入最小闭环

- 关联 issue：[#1](https://github.com/notnotype/hermes-plugin-onebot11/issues/1)（待创建,依赖 GitHub 授权）
- 状态：进行中
- 开始日期：2026-08-04

## 用户需求

让 Hermes 通过 OneBot 11 协议接入 QQ（私聊 + 群聊）,带消息查询工具与面向群聊的权限管理。工作流照搬 neuro-book 的 AGENTS.md。

## 目标

- 反向 WS 收事件 + HTTP API 发送的最小闭环
- 三个查询工具（单条消息 / 群历史 / 私聊历史）
- 权限：管理员列表 + 会话范围校验（群聊只能查本群）
- `hermes plugins install notnotype/hermes-plugin-onebot11` 一键可装
- 与真实 LLBot 联调通过

## 执行过程

1. 调研 OneBot 11 协议 + Hermes 适配器机制 → `docs/research/onebot11-hermes-survey.md`
2. 仓库初始化（Task 1）：gh 未装 → 下载 gh 2.97.0 到 ~/.local/bin；bootstrap 提交（文档骨架）
3. worktree `feat/i1-t1-onebot11-min-loop` 建立,代码全部在 worktree 内开发
4. 核心模块按 TDD 逐个落地：
   - message.py（array 消息段解析,9 用例）
   - events.py（事件归一化,8 用例;含「缺 message 字段丢弃」修正）
   - ws_server.py（反向 WS + Bearer 校验,6 用例）
   - http_api.py（发送/动作/重试/分块/下载,12 用例）
   - permissions.py（权限门禁,9 用例;修正:开放模式只放宽 admin 门槛,会话范围校验始终生效）
   - tools.py（会话注入:群号/QQ 号不由 LLM 传,5 用例）
   - adapter.py（12 用例;关键发现见下）
5. 关键决策与踩坑：
   - **Platform("onebot11") 不能模块级调用**：插件模块在 register() 前被 import,而 Platform._missing_ 只接受已注册平台名 → 惰性解析（irc 同款做法）
   - **工具权限的可信通道**：工具 handler 只收到 session_id,没有会话身份 → 用 gateway runner 的 `_get_cached_session_source(session_id)` 找回 SessionSource,user/chat 从入站事件来,LLM 无法伪造
   - **send 的群/私聊区分**：send() 只有 chat_id,用入站登记表 `_chat_types`（默认群聊）
   - hermes venv 补装了 pytest + pytest-asyncio（仅开发用）

## 变更文件

- 根：adapter.py、plugin.yaml、pyproject.toml、AGENTS.md、CONTRIBUTING.md、README.md、PROJECT-STATUS.md、RELEASE.md、.gitignore、LICENSE、.github/workflows/ci.yml
- onebot11/：message.py、events.py、ws_server.py、http_api.py、permissions.py、tools.py、__init__.py
- tests/：test_message/test_events/test_ws_server/test_http_api/test_http_download/test_permissions/test_tools/test_adapter
- docs/：README.md、permissions.md、research/、tasks/

## 验证结果

- 本地单元测试：49 passed（worktree venv）+ 15 passed（adapter,hermes venv + PYTHONPATH,需干净环境）
- ruff check 全过
- `hermes plugins list` 能看到 onebot11-platform 0.1.0（symlink 到 ~/.hermes/plugins/）
- CI 待跑（PR #2 创建后 CI 曾 pending,仓库有 3000 端口联调改动未合并）
- **与 LLBot 联调已通**：
  - LLBot ob11 开启 ws-reverse（ws://host.docker.internal:18880）+ HTTP 3000,compose 映射 3000 端口
  - Hermes 侧 3 平台在线,反向 WS 连接成功,事件上报正常
  - 群白名单（ONEBOT11_ALLOWED_GROUPS）功能测试验证：白名单外群消息被拦、白名单内群放行
  - 网关日志可见白名单加载状态（INFO）
- **联调踩坑记录**：
  - 插件模块在 register() 前被 import → Platform() 不能模块级调用,惰性解析
  - 内部子包导入需双上下文兼容（网关包加载 + 测试顶层加载）,用 try/except 桥
  - 内层包内部 import 必须相对导入
  - 插件目录必须有根 __init__.py（否则 "No __init__.py" 加载失败）
  - 网关重启后 LLBot 会重放积压消息 + 旧会话恢复 → 白名单部署前建立的会话仍可能被续跑;清理非白名单群旧会话可消除
  - 测试环境注意：shell 里若导出了 ONEBOT11_* 变量会影响测试,用 env -i 跑
  - hermes venv 补装了 pytest + pytest-asyncio（仅开发用）

## 后续 TODO

- [ ] 用户重启 LLBot 后真机验证：3 个白名单群 @ 触发正常,非白名单群零响应
- [ ] 在常用群 /sethome 设置 home channel（消除 📬 提示）
- [ ] 权限系统设计（调研 Discord/Telegram 后）:群内用户不可信,不放开 shell/管理工具;工具权限基于 ONEBOT11_ADMINS
- [ ] 出站侧白名单拦截（可选加固:send() 里丢弃非白名单群目标,防关机通知等核心出站消息漏网）
- [ ] 回复机器人消息也触发（reply-to-bot,需跟踪自己发的 message_id）
- [ ] 群角色权限（v1.1,get_group_member_info）
- [ ] 图片下载在真实 LLBot 上的 URL 形态验证

## 会话与触发调研结论（Telegram/Discord 对齐）

- 会话模型：chat_id = 群号;`group_sessions_per_user` 默认 true → 群里每用户独立会话（Telegram/Discord 一致,base 默认即此;我们的适配器继承,无需额外配置）。设 false 则整群共享。群文本保留 [昵称] 前缀供上下文可读。
- 触发：`ONEBOT11_REQUIRE_MENTION`（默认 true,对齐 Telegram require_mention）——群聊必须 @ 机器人才响应,未 @ 的消息在适配器层过滤（INFO 日志）。reply-to-bot 触发为后续增强。
