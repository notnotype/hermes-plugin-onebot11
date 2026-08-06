# Task 3：OneBot 11 分层触发与活跃窗口

- 关联 Issue：[Issue #5：OneBot 11 分层触发与活跃窗口](https://github.com/notnotype/hermes-plugin-onebot11/issues/5)
- 状态：balanced 策略已落入生产代码；spike 保留为设计依据
- 类型：逻辑/状态机 + Hermes auxiliary 集成

## 背景

当前插件的确定性触发器已经支持 `@`、关键词、`always` 和私聊直触发。生产实现进一步把问句/记忆候选、低成本旁路判断和活跃窗口组合起来，同时限制每群判断并发和单窗口仲裁次数，避免每条群消息都调用模型。

需要回答的问题是：硬触发、问句/记忆候选、活跃窗口和低价 LLM 仲裁如何组合，才能在成本、延迟、连续对话和漏唤醒之间取得可接受的平衡。

## Spike 范围

`spikes/onebot11_trigger_spike.py` 是一次性、内存态的 `PROTOTYPE`。它使用虚拟时间模拟：

- `@`、关键词和管理员命令等硬触发直接创建 Agent turn；
- 问句特征、记忆分数和活跃窗口消息进入 5 秒 trailing debounce；
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
- 候选：空闲状态只识别问句，以及带回指词且当前群有摘要/最近原文的记忆候选；候选经过 5 秒 trailing debounce。
- 旁路结果：严格接受 `trigger|wait|ignore` 三态；`wait` 只等待真实新消息，不创建 lease 或空轮询。
- 活跃窗口：成功 Agent turn 后 idle 60 秒，最长连续 300 秒，最多 3 次 LLM 仲裁；失败、取消和 uncertain 不进入 engaged。
- 竞争处理：每群最多一个判断任务；判断期间的队列 revision 变化会重新安排一次，硬触发会使旧结果失效。
- 兼容性：旁路调用只接受显式 provider/model 和群 allowlist，并固定 `fallback_policy=none`、`max_attempts=1`。旧 Hermes auxiliary API 缺少参数时安全跳过。

Hermes 自优化只适合生成配置 diff/建议并由管理员审核，不允许运行时修改 Python、权限、白名单或自动启用关键词。本任务不实现自优化执行链路、RAG 或向量库。

## 结果

运行结果写入同目录的 `NOTES.md`。固定场景下，balanced 使用 9 次 LLM 调用、漏唤醒 1/10、误唤醒率 0.111；conservative 使用 8 次调用、漏 2/10；high-recall 使用 10 次调用但没有比 balanced 多保留一次唤醒。结论支持进入正式实现，但只吸收 balanced 的纯函数和状态转换，不把交互壳带入生产代码。

## 计划出入

spike 没有接真实 provider；生产代码通过 Hermes auxiliary API 接入显式旁路 provider/model，并把 provider fallback 与隐式重试关掉。没有加入 RAG、向量库、自动语义压缩或运行时自优化。活跃窗口仍是内存状态，重启后回到 idle，只恢复 SQLite 消息和显式 trigger request。
