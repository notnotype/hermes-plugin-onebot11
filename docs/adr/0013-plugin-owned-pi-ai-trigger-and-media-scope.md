# ADR-0013：插件自有 pi-ai 触发与图片能力边界

- 状态：已接受
- 日期：2026-08-09
- 关联：原始 OneBot 11 触发需求、Issue #5、Task 3

## 背景

OneBot11 的软触发需要低成本、严格单次请求的旁路判断。此前代码把这个判断接到
Hermes auxiliary，并要求 Hermes 暴露额外的 no-fallback 参数。这个合同过窄，
会让插件能否启动和 Hermes 是否合并另一个 PR 发生不必要的耦合。

同样，OneBot 11 的图片协议细节属于平台适配器，不值得为了 image-only completion
结果再改造 Hermes 全局 delivery。

## 决策

1. LLM trigger 由插件通过固定版本 `@earendil-works/pi-ai` 调用。
2. Python 只启动短生命周期 Node helper，并通过 JSON stdin 传 provider、model、
   prompt、超时和密钥环境变量名；密钥值只从进程环境读取。
3. 不使用 Hermes auxiliary、Hermes 主 Agent fallback 或插件侧语义重试。Node、
   依赖、provider/model、超时和非法结果都按 `ignore`，消息留在 pending。
4. 内置 provider/model 使用 pi-ai catalog；自定义 provider 只允许
   `http`/`https` OpenAI-compatible `base_url` 和合法 `api_key_env`。
5. Agent 最终回复图片继续由 OneBot adapter 编码为受限 `base64://` segment，
   采用 best-effort。图片数量、单图/总量、魔数、临时目录和 lease fencing 仍由插件负责。
6. 不扩展通用 `send_message`、cron/standalone sender 的 plugin media，也不修改
   Hermes 的全局媒体结果合同。

## 后果

- Hermes 使用官方稳定版即可运行硬触发、队列、权限和图片 best-effort。
- 启用软触发需要 Node.js ≥22.19，并在插件目录执行 `npm ci --omit=dev`。
- Hermes 可能无法精确知道旧式 image-only delivery 的每个图片结果；这是接受的窄场景妥协。
- RAG、运行时自优化和 provider 自动切换仍不在范围内。
