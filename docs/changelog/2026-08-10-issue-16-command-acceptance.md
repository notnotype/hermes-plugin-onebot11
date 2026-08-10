# Issue #16：群级会话命令与连续对话验收收口（未发布）

- 新增 `/new [title]`、`/reset`、`/clear` 群级会话命令桥接；仅超级管理员可用，
  命令不进入 OneBot 普通消息队列。
- reset 通过 Hermes 公共入口和 `on_session_reset` hook 完成，使用消息序号边界，
  不误删 reset 期间抵达的新消息。
- 旧 adapter epoch、旧 reset generation 的 late completion 不再污染新连接或新会话
  的触发状态。
- 失败、取消和 unknown turn 不会继续保留 engaged；成功 turn 才能开启连续对话窗口。
- reset 未能安全收口时不推进 generation；文本部分成功、图片 lease 失效和命令回执
  也分别保持 unknown-safe/fail-closed。
- Hermes 组合 smoke 增加 slash command、5 hooks、reconnect 和 pi-ai helper 验收。

当前 package/plugin 版本仍为 `0.6.0`；本文件记录 Issue #16 分支的未发布变更，
不代表已经合并或部署到 Arch。
