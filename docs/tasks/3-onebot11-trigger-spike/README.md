# Task 3：OneBot 11 分层触发与活跃窗口 Spike

- 关联 Issue：[Issue #5：OneBot 11 分层触发与活跃窗口](https://github.com/notnotype/hermes-plugin-onebot11/issues/5)
- 状态：spike complete；正式实现待下一步确认
- 类型：逻辑/状态机 spike

## 背景

当前插件的确定性触发器已经支持 `@`、关键词、`always` 和私聊直触发，旁路 LLM 仍然是单一布尔判断。下一步希望同时提升唤醒成功率和连续对话体验，但不能让每条群消息都调用模型。

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

## 关键决策候选

默认策略暂定为 balanced：硬触发不消耗 LLM；问句和较高记忆命中进入一次仲裁；Agent 回复后 60 秒内的普通消息进入合并判断；连续噪声通过最长活跃时间封顶。LLM 判断失败按不触发处理。

Hermes 自优化只适合生成配置 diff/建议并由管理员审核，不允许运行时修改 Python、权限、白名单或自动启用关键词。本 spike 不实现自优化执行链路。

## 结果

运行结果写入同目录的 `NOTES.md`。固定场景下，balanced 使用 9 次 LLM 调用、漏唤醒 1/10、误唤醒率 0.111；conservative 使用 8 次调用、漏 2/10；high-recall 使用 10 次调用但没有比 balanced 多保留一次唤醒。结论支持进入正式实现，但只吸收 balanced 的纯函数和状态转换，不把交互壳带入生产代码。

计划出入：本 spike 没有接真实 provider、RAG、Hermes 自优化或持久化，因为本任务的问题是先验证状态机和成本/唤醒权衡；这些边界需要后续真实审计样本和正式合同后再决定。
