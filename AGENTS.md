# AGENTS.md

## 默认约定（2026-08-11 更新）

本文档描述的**开发工作流默认不再强制**：只有用户在当前对话中显式要求
“按 AGENTS.md 执行”时才生效。未显式要求时，Agent 直接按用户当次指令工作，
验证通过后可以自行合并 PR、关闭 issue 并收尾，不需要先征求流程许可。

以下工程约束仍然始终有效（与工作流无关）：

- 使用中文与用户交互。
- 代码和注释使用中文；函数写注释；类型标注重要，外部未知数据用 `unknown`。
- 不要用 hack 绕过问题、制造技术债；遇到设计问题先停下向用户说明。
- 单点、少量、需要判断的修改用文件编辑工具，不用 shell 绕过；大范围机械
  替换才用脚本批处理，且先 dry run 确认范围。
- 本仓库内部包 `onebot11/` 保持**零 Hermes 依赖**（纯协议逻辑，可独立测试）；
  只有根目录 `adapter.py` 依赖 `gateway.platforms.base`。
- 本地门禁：`pytest -q` 全绿 + `ruff check .` 无错。
- 与真实 QQ 框架联调才算功能完成。
- 任务完成后报告并清理用户不知情的临时文件。

## 参考工作流（仅当用户显式要求时生效）

- 开发入口、Issue/PR 流程见 [`CONTRIBUTING.md`](CONTRIBUTING.md)。
- 需求与 TODO 用 GitHub Issue 承载；分支 + worktree 承载开发；代码改动走
  squash PR 合并进 `master`；master 始终保持可构建、可测试，不 force push。
- 分支命名：`{type}/{refs}-{slug}`，`type` ∈ `feat`/`fix`/`docs`/`refactor`/
  `test`/`chore`，`refs` = `i{issue号}` 或 `t{task号}`。
- 文档索引：`PROJECT-STATUS.md` 记录仓库现状；`docs/tasks/<序号>-<slug>/`
  记录 active 重大任务 walkthrough；移动/改名文档必须同步更新交叉链接。
