# PROJECT-STATUS

仓库级现状报告。TODO / 跟进事项一律开 GitHub Issue,不写进本文件。

## 当前状态

- **阶段**：首个功能已完成并合并（PR #2，Closes issue #1：OneBot 11 接入最小闭环）
- **最近验证**：与真实 QQ 框架（LLBot 8.1.5,直连模式）联调已通；群白名单 + @ 触发已上线
- **重点**：消息解析 → 反向 WS → HTTP 发送 → 工具与权限 → adapter 组装 → 联调（全部完成）

## 模块状态

| 模块 | 状态 | 说明 |
|---|---|---|
| onebot11/message.py | 完成 | array 消息段解析（文本/图片/@,CQ 字符串格式显式不支持） |
| onebot11/events.py | 完成 | OneBot 事件 → InboundEvent（群聊 chat_id=group_id、私聊=user_id） |
| onebot11/ws_server.py | 完成 | 反向 WS 服务端（Bearer 校验、heartbeat 忽略） |
| onebot11/http_api.py | 完成 | HTTP 发送与动作调用（重试、分块、reply 段、图片下载） |
| onebot11/tools.py | 完成 | 平台工具（消息查询 x3,toolset onebot11） |
| onebot11/permissions.py | 完成 | 权限模型（管理员列表 + 会话范围校验;v1.1 预留群角色） |
| adapter.py | 完成 | 适配器主类 + register（群白名单、@ 触发、私聊策略） |
| plugin.yaml | 完成 | 插件元数据（name=onebot11-platform,kind=platform） |

## 联调状态（LLBot 生产环境）

- Hermes 网关 3 平台在线,反向 WS 连接成功,事件上报正常
- 群白名单 ONEBOT11_ALLOWED_GROUPS=1072992996,287447372,976967537（白名单外群消息过滤,INFO 日志可观测）
- 群聊 @ 触发 ONEBOT11_REQUIRE_MENTION=true（默认开;未 @ 机器人的群消息忽略）
- 会话模型：群里每用户独立会话（group_sessions_per_user 默认 true,对齐 Telegram/Discord）
- 发送路径：Hermes → http://127.0.0.1:3000（LLBot ob11 HTTP,compose 映射 3000）

## 风险

- 权限系统待重设计（调研 Discord/Telegram 后）：白名单群用户不可信任,不放开 shell/管理工具
- 出站侧白名单拦截（可选加固）:非白名单群目标的发送目前只靠会话清理兜底
- 反向 WS 依赖 Hermes 侧常驻服务,LLBot 断线自动重拨

## 近期任务

- 见 [docs/tasks/](docs/tasks/) 的 active task walkthrough（Task 13 收尾已完成:合并、删分支、文档）
