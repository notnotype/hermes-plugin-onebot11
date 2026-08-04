# AGENTS.md

面向人类贡献者的开发入口、Issue/PR 流程见 [`CONTRIBUTING.md`](CONTRIBUTING.md)；本文件作为开发 Agent 和仓库实现细则的权威来源。工作流继承自 neuro-book 的 AGENTS.md,按 Python 插件仓库裁剪。

## Core Rules

- 使用 *中文* 为默认语言与用户交互
- **学会在需求和实现复杂度之间妥协：制定计划、需求、审查用户需求或设计系统时,多思考一步：这个需求是否很冷门？妥协是否能大幅降低复杂度？**
- 如果遇到性能与复杂度权衡问题,报告、解释、给出建议、交给用户做最终决定
- **Bug 诊断流程**：先阅读相关上下文并定位可能原因,再用最小测试/脚本/请求复现并确认症状。不要直接修改业务代码修复；诊断完成后先给出报告（现象、复现结果、根因判断、影响范围、建议修复方案）,等待用户确认后再进入实现
- 没有收到用户明确的指令,永远不要擅自改代码、文件。优先做只读调研、讨论、分析
- 任务完成后不要主动运行 git 命令查看变更
- **单点、少量、需要判断的修改一律用文件编辑工具,不要用 shell 绕过**。大范围机械替换才用脚本批处理,且必须：1) 先 dry run 确认命中范围;2) 有把握才批处理
- 不要自动进行浏览器验证,可以建议用户让你进行浏览器验证
- 代码审查报告使用直白的话语再解释一次
- 任务完成后的 walkthrough 要报告实际结果与任务计划的出入
- 代码修复/重构设计时考虑：是否系统性？能否在代码设计上约束 Agent 以后不犯同类错误？会导致哪些测试出问题？
- 任务结束后报告并清理用户不知情的临时文件

## Git 工作流

- GitHub Issue 承载需求与 TODO,分支 + worktree 承载开发,squash PR 合并进 `master`。
- 分支命名：`{type}/{refs}-{slug}`，`type` ∈ `feat`/`fix`/`docs`/`refactor`/`test`/`chore`，`refs` = `t{task号}` 或 `i{issue号}`（issue 在前：`i{issue号}-t{task号}`），`slug` 英文 kebab-case 不超过 5 个单词。
- 每个代码分支必须能追溯到至少一个 issue 或 task。
- 开发循环：
  1. 想法/需求先开 GitHub Issue（`type:*` + `status:*` 标签）。
  2. 重大任务按 `docs/tasks/README.md` 建任务目录。
  3. 开工前 `git fetch origin`，`git worktree add .agent/workspace/wt/<slug> -b <branch> origin/master`。
  4. worktree 内开发；完成后 push 分支并 `gh pr create`。PR 完整覆盖 issue 用 `Closes #N`，部分覆盖用 `Refs #N`。
  5. **到此停下,向用户报告验证结果与 PR 链接。Agent 不得自行合并 PR、关闭 issue 或做其它收尾动作。**
  6. 收尾（仅用户许可后）：CI 通过 + 本地验证通过 → `gh pr merge --squash --delete-branch` → issue 随 `Closes` 自动关闭 → 主工作区 `git fetch && git merge --ff-only origin/master` → `git worktree remove` + 删残留分支。
- Agent 创建 Issue 约定：必须打 `source: agent` 标签;正文四段式（一句人话概述 → 背景 → 内容/方案 → 验收/证据）;Task 引用用完整链接;不复制会话原话。
- master 纪律：`master` 始终保持可构建、可测试；代码改动一律走分支 + squash PR。文档类例外（typo、PROJECT-STATUS / walkthrough 更新、RELEASE 维护）可直推 master。不 force push。

## 文档索引

- `PROJECT-STATUS.md`：仓库级现状。TODO / 跟进事项开 GitHub Issue。
- `docs/tasks/<序号>-<slug>/README.md`：active 重大任务 walkthrough。
- `docs/research/`：调研资料；`docs/drafts/`：未定稿草案；`docs/archived/`：过期参考。
- 移动/改名文档必须同步更新交叉链接。

## Python 编码细则

- 代码和注释使用中文；函数必须写注释（docstring 或行内）。
- 不要过度设计。先尝试在现有组件基础上修改,实在不行才建立新组件。
- 不要过度创建函数：只有一处复用的逻辑不抽函数,优先 inline。
- 实现需求时先考虑第三方库。
- 类型标注重要：每个组件都标注类型;外部未知数据用 `unknown`,`any` 需写明原因。
- 简单逻辑不主动写测试文件,复杂逻辑必须写测试。只在最常用、最复杂、最容易犯错的地方加测试。
- 不要用 hack 绕过问题、制造技术债。遇到设计问题立刻终止任务并告知用户。
- 不要一次性应用 800 行以上的超大补丁,拆分为多次。
- 本仓库内部包 `onebot11/` 保持**零 Hermes 依赖**（纯协议逻辑,可独立测试）；只有根目录 `adapter.py` 依赖 `gateway.platforms.base`。

## 验证

- 本地门禁：`pytest -q` 全绿 + `ruff check .` 无错。
- 与真实 QQ 框架联调才算功能完成。
