# ADR-0014：Hermes 交付合同与 Arch 验收边界

- 状态：已接受
- 日期：2026-08-09

## 背景

OneBot 11 插件需要调用 Hermes 的 auxiliary 和媒体投递生命周期，但 `onebot11/` 必须保持零 Hermes 依赖。插件本地通过注入 Hermes 源码可以验证组合行为，但这不等于 Hermes 远端已经发布，也不等于 Arch 上正在运行的 Hermes 支持同一合同。

如果旁路模型失败时自动回退主 Agent，或 OneBot 非幂等发送返回未知后继续 plain-text/fallback，就可能重复执行整轮 Agent 或重复发送管理动作。图片还必须跨越“Hermes 宿主机—LLBot 容器”的路径边界。

## 决策

- `onebot11_trigger` 只允许显式 provider/model、群 allowlist、`fallback_policy="none"` 和 `max_attempts=1`。旧 Hermes 缺少严格参数时，插件禁用 LLM trigger 并保留 pending 消息，不调用主模型兜底。
- Hermes 媒体结果按每个 `SendResult` 聚合：image-only 只有所有图片明确成功才算 turn success；部分成功、缺块或 `error_kind="unknown"` 都不能当作成功。
- `unknown` delivery 立即停止 Hermes 重试、plain-text fallback 和 cron standalone fallback；非幂等操作由 OneBot 队列/operation ledger 保持 `uncertain`，交给管理员 resolve。
- OneBot 出站图片由插件编码为受限 `base64://` segment，不使用宿主机路径直传、临时 HTTP 媒体服务器或 Docker volume。
- Hermes strict auxiliary 与媒体/unknown 改动独立于插件 PR 交付；插件在旧版本上安全降级，不能通过 hack 猜测或绕过 Hermes API。
- 本地纯插件测试、Hermes 临时 `HERMES_HOME` 组合测试、Hermes 独立 worktree 测试和 Arch + LLBot 真人验收分别记录，不互相冒充。

## 影响

- Arch 在升级 Hermes 合同前，LLM trigger 和出站图片可能保持 disabled/unsupported；文本、队列、权限和硬触发继续可用。
- 本地组合测试可以证明 adapter 胶水和协议合同，但不能证明远端 PR 已合并或生产配置已切换。
- Arch 验收只允许使用已批准的机器人、群和私聊目标；不以真实禁言、踢人、撤回或全员禁言作为默认测试手段。
- OneBot 非幂等出站仍不承诺 exactly-once；未知结果不能自动消除，只能显式人工处理。
