# Task 3：OneBot 11 分层触发与活跃窗口

> 本文保留 spike 的历史状态机结果。当前生产 selector 已改为只选择真实消息
> `anchor_seq`，严格 JSON 合同见 [ADR-0013](../../adr/0013-plugin-owned-pi-ai-trigger-and-media-scope.md)；
> 下文的 `wait_seconds` 只属于历史 spike，不是当前 OneBot 协议。

- 关联 Issue：[Issue #5：OneBot 11 分层触发与活跃窗口](https://github.com/notnotype/hermes-plugin-onebot11/issues/5)
- 状态：balanced 策略已落入生产代码，并接入 TurnAnchor；spike 保留为设计依据
- 类型：逻辑/状态机 + 插件自有 pi-ai 集成

## 背景

当前插件的确定性触发器已经支持 `@`、关键词、`always` 和私聊直触发。生产实现进一步把问句/记忆候选、低成本旁路判断和活跃窗口组合起来，同时限制每群判断并发和单窗口仲裁次数，避免每条群消息都调用模型。

需要回答的问题是：硬触发、问句/记忆候选、活跃窗口和低价 LLM 仲裁如何组合，才能在成本、延迟、连续对话和漏唤醒之间取得可接受的平衡。

## Spike 范围

`spikes/onebot11_trigger_spike.py` 是一次性、内存态的 `PROTOTYPE`。它使用虚拟时间模拟：

- `@`、关键词和管理员命令等硬触发直接创建 Agent turn；
- 问句特征、记忆分数和活跃窗口消息进入自适应 debounce：群不活跃（消息间隔超过 5 秒）立即判断，活跃时按 trailing 节流合并；
- 旁路模型只接受严格的 `trigger`、`wait`、`ignore` 三态；
- `wait` 只等待下一批真实新消息，不进行空轮询；
- Agent turn 完成后进入 60 秒活跃窗口，并限制最长连续活跃时间；
- 统计 LLM 调用数、估算成本、决策延迟、连续对话保留率、误唤醒和漏唤醒。

运行：

```powershell
.venv\Scripts\python.exe spikes\onebot11_trigger_spike.py --scenario all
```

也可以手动驱动状态机：

```powershell
.venv\Scripts\python.exe spikes\onebot11_trigger_spike.py --interactive
```

交互命令是 `message <秒> <用户> <文本>`、`tick <秒>`、`q`。每次输入都会打印完整状态。

## 已落地的 balanced 合同

- 硬触发：@、关键词、`always` 和管理员命令直接创建 durable trigger，不消耗旁路 LLM。
- TurnAnchor：selector 只为仍存在且未消费的真实消息创建 anchor；同群多个 anchor 按序串行，每个 anchor 是一个独立 shared-session follow-up。
- 候选：空闲状态只识别问句，以及带回指词且当前群有摘要/最近原文的记忆候选；候选经过自适应 debounce（间隔超过 debounce 窗口立即判断，活跃时补齐窗口）。
- 旁路结果：严格接受 `trigger|wait|ignore` 三态；`wait` 使用 `5/10/30/60` 秒，只等待真实新消息，不创建 lease 或空轮询；`trigger/ignore` 的 `wait_seconds` 必须为 `0`。
- 活跃窗口：成功 Agent turn 后 idle 60 秒，最长连续 300 秒，最多 3 次 LLM 仲裁；失败、取消和 uncertain 不进入 engaged。
- 短确认词：engaged/waiting/debounce 窗口内的“可以、好的、好、行、嗯、嗯嗯、明白、收到、继续、接着、对、是的”等短确认词由确定性规则直接创建 trigger（`engaged_ack`），不调用旁路模型、不消耗仲裁次数；带实际问题的消息（如“可以吗？”）仍进入 selector。idle 状态下的短确认词不触发。Hermes turn 收口时若最新 pending 消息是短确认词，也会直接补建 `engaged_ack` trigger，不再丢失触发机会。
- 竞争处理：每群最多一个判断任务；判断期间的队列 revision 变化会重新安排一次，硬触发会使旧结果失效。
- waiting 状态也受当前活跃窗口的仲裁上限约束；达到上限后只等待硬触发或管理员 flush，不再消耗旁路模型调用。
- selector 判断提示：问句/记忆候选进入判断时给候选消息添加 `emoji_id=128064`（👀，表示 bot 正在看这条消息），判断结束（触发、忽略、超时、非法结果或 wait 到期）后移除；任何触发进入回复阶段后由群 turn 的 ⌛（`emoji_id=8971`，表示正在回复）接管。硬触发和短确认词直接触发时跳过 👀 直接使用 ⌛。两种指示器都是 best-effort 内存状态，重启后不恢复，进程崩溃遗留的远端 reaction 不纳入清理承诺。`emoji_id=9203`（⏳）不被 QQ reaction API 支持（实测返回 failed），因此 ⌛ 使用 `8971`。
- 旁路调用只接受显式 provider/model 和群 allowlist，由插件自有 Node helper 调用固定版本的 `@earendil-works/pi-ai`；不经过 Hermes auxiliary，不做 provider fallback 或主 Agent fallback。Node/依赖/provider/model/模型结果异常时安全按 `ignore`。
- 上下文：群当前 anchor batch 作为普通 user message，滚动摘要优先通过 `channel_prompt` 临时注入；旧 Hermes 明确退回有界文本模式。摘要不会作为每轮普通 user transcript 重复累积。

Hermes 自优化只适合生成配置 diff/建议并由管理员审核，不允许运行时修改 Python、权限、白名单或自动启用关键词。本任务不实现自优化执行链路、RAG 或向量库。

## 结果

运行结果写入同目录的 `NOTES.md`。固定场景下，balanced 使用 9 次 LLM 调用、漏唤醒 1/10、误唤醒率 0.111；conservative 使用 8 次调用、漏 2/10；high-recall 使用 10 次调用但没有比 balanced 多保留一次唤醒。结论支持进入正式实现，但只吸收 balanced 的纯函数和状态转换，不把交互壳带入生产代码。

## 计划出入

spike 没有接真实 provider；生产代码改为由插件自有 pi-ai helper 接入显式 provider/model，并把 provider fallback 与隐式重试关掉。Hermes strict auxiliary 不再是代码路径，也不创建 Hermes PR。没有加入 RAG、向量库、自动语义压缩或运行时自优化。活跃窗口仍是内存状态，重启后回到 idle，只恢复 SQLite 消息和显式 trigger request。

Issue #16 的验收收口保留这套 balanced 状态机：问句和活跃窗口普通消息继续进入 pi-ai selector，@/关键词/always/管理员命令继续绕过 selector。Arch 默认只开启问句 selector，关键词是否启用由部署配置决定；本地代码和状态机测试覆盖关键词，但未把它写成 Arch 已验收能力。
