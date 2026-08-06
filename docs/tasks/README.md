# Task Walkthrough 规则

- 每个重大任务一个目录：`docs/tasks/<序号>-<task-slug>/README.md`。
- 记录：用户需求、目标、执行过程、关键决策、变更文件、验证结果、后续 TODO。
- 同一功能后续调节,继续更新原任务目录,不每轮新建碎片文档。
- 任务完成后的 walkthrough 要报告**实际结果与任务计划的出入**。
- 归档任务移到 `docs/tasks/archived/<task-slug>/`。

## 当前 active 任务

- [2-onebot11-reliability](2-onebot11-reliability/README.md)：共享 session、持久队列、触发器、权限门禁和可靠出站
- [4-onebot11-context-permissions](4-onebot11-context-permissions/README.md)：当前 batch 上下文物化、精确角色工具权限和群 slash command

## 历史任务

- [1-onebot11-min-loop](1-onebot11-min-loop/README.md)：OneBot 11 接入最小闭环；其旧的 per-user session 和管理员合同已被 Task 2 取代。
