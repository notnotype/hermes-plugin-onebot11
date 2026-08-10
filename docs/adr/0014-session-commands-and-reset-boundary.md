# ADR-0014：群级会话命令与 reset 边界

- 状态：已接受
- 日期：2026-08-10
- 关联：Issue #16、Task 6、OneBot 11 原始需求

## 背景

Hermes 的 `/new` 等命令原本由通用消息入口处理。OneBot 群消息又必须先进入
插件自己的队列，导致群内 `/new` 如果不做桥接，就会被当作普通上下文，无法可靠地
重置 shared session。直接调用 Hermes 私有 reset 函数则会把插件绑定到不稳定的内部 API。

## 决策

1. OneBot adapter 在群消息入队前只识别 `/new [title]`、`/reset`、`/clear`。
   `/clear` 是 OneBot 侧别名，统一翻译为公共 `/new`；`/onebot ...` 仍由管理命令
   处理，普通未知 slash command 不会被拦截。
2. 会话命令必须同时通过当前 OneBot 访问策略和超级管理员校验。未授权命令直接拒绝，
   不进入队列、Hermes session 或 Agent 上下文。
3. 命令通过 Hermes 公共消息/命令入口执行；插件只订阅 `on_session_reset` hook。
   Hermes reset 成功后，插件按精确群身份清理 OneBot 消息、anchor、摘要和内存触发状态。
4. reset 记录开始时的 `latest_seq` 作为删除边界。边界之后抵达的消息不被旧 reset
   删除；reset 进行期间的普通群消息得到明确提示，避免把新消息混入即将清理的旧会话。
5. reset 前停止 selector、debounce 和 engaged timer，并 fence 当前群 lease。旧 task
   即使延迟完成，也只能清理自身资源；adapter epoch 和 reset generation 不匹配时，
   不能推进新 runtime 的触发状态或创建 recovery trigger。
6. reset 不删除 Hermes 其他群、私聊或旧 session 历史，也不改变 paused 配置、operation
   ledger 和审计记录。已开始或未知的非幂等出站保留 unknown，必须人工处理。

## 后果

- OneBot 群可以使用稳定的公共命令合同，而不依赖 Hermes 私有 reset 实现。
- 命令处理需要 Hermes `v0.20.0` 提供可观察的 `on_session_reset` 生命周期；
  本插件不为更旧、缺少该 hook 的 Hermes 猜测 reset 是否成功，也不把它们宣称为
  支持版本。
- reset 期间普通消息不会排队等待旧会话；用户需要在提示后重新发送。
- 旧 Hermes 如果不提供 `on_session_reset`，命令可以进入公共入口，但插件不会猜测
  何时清理队列；应记录审计并由管理员处理，而不是静默删除消息。
