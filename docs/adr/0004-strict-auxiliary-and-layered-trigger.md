# ADR-0004：严格旁路模型合同与分层触发边界（历史记录）

- 状态：已接受
- 日期：2026-08-06

> 当前生产实现已改为插件自有 pi-ai helper；见 [ADR-0013](0013-plugin-owned-pi-ai-trigger-and-media-scope.md)。

## 历史决策

### 严格旁路模型

OneBot11 的 `onebot11_trigger` 只使用显式配置的 provider、model 和群 allowlist。
当前实现如果调用 Hermes auxiliary，会固定传：

```text
fallback_policy="none"
max_attempts=1
```

旧 Hermes 没有这两个参数时，插件检测到 API 不兼容后禁用 LLM trigger，消息继续留在 SQLite pending 队列；不得偷偷调用主 Agent、自动切换 provider 或重复请求。

严格 auxiliary 不是本插件的生产启动依赖，本轮不为它单独修改或升级
Hermes。硬触发、私聊直触发和队列恢复不依赖旁路模型。未来若要在旧
Hermes 上恢复低成本判断，优先由插件直接调用明确配置的 provider；这不
属于当前实现，且仍必须保持单次、无 fallback、无主 Agent 兜底。

旁路模型只负责输出严格三态：

```json
{"decision":"trigger","wait_seconds":0}
{"decision":"wait","wait_seconds":5}
{"decision":"ignore","wait_seconds":0}
```

`wait_seconds` 只允许 `5/10/30/60`，其他值、非法 JSON、超时和模型错误都按“不触发”处理。

### 分层触发

@、关键词、`always` 和管理员命令是确定性硬触发，直接创建 durable trigger。空闲状态只把问句和有上下文的记忆回指作为 LLM 候选；成功 Agent turn 后的活跃窗口允许普通消息进入 5 秒 debounce。每群最多一个判断任务，硬触发优先并使旧判断结果失效。

活跃窗口默认 idle 60 秒、最长 300 秒、最多 3 次仲裁。等待只等待真实新消息，不进行空轮询；窗口状态只保存在内存中。

## 原因

触发判断是成本控制和唤醒体验之间的策略层，不应拥有 Agent 工具权限，也不应拥有主模型 fallback 权限。严格旁路合同把一次判断的成本、延迟和失败行为限制在可审计范围内；纯函数候选识别让没有旁路模型时仍可安全运行。

## 影响

升级后旧 Hermes 不会因为缺少新 auxiliary 参数而启动失败，但其 LLM trigger 会被安全禁用。@、关键词、`always`、私聊直触发和管理员 flush 不受影响。重启会丢失 active/engaged 内存状态，但不会丢失已经入队的消息或显式 trigger request。

本 ADR 只保留旧 auxiliary 方案的历史背景和验证记录，不再约束当前生产代码。
