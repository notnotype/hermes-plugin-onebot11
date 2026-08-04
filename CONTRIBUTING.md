# CONTRIBUTING.md

面向人类贡献者的开发入口、Issue/PR 流程和 Task 责任边界。开发 Agent 与仓库实现细则见 [AGENTS.md](AGENTS.md)。

## 工作流概览

- 需求先开 GitHub Issue（`type:*` + `status:*` 标签）。
- 代码改动一律走分支 + squash PR 合并进 `master`。
- 分支命名：`{type}/{refs}-{slug}`，如 `feat/i1-t1-onebot11-min-loop`。
- 每个代码分支必须能追溯到至少一个 issue 或 task。

## Issue 约定

- 正文四段式：一句人话概述 → 背景 → 内容/方案 → 验收/证据。
- 机器起草的 issue 必须打 `source: agent` 标签。
- 不复制会话原话,不裸写内部编号。

## 本地验证（实际门禁）

```bash
pytest -q    # 全部测试通过
ruff check . # 无 lint 错误
```

## 收尾

- PR 合并前确认 CI 通过 + 本地验证通过。
- squash 提交信息 = PR 标题（Conventional Commit 格式）。
- Agent 不自行合并 PR、关闭 issue;收尾许可来自用户。
