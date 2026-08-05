# Task 2: 群聊会话与权限 v2(一群一会话 + 消息队列 + 多触发 + 角色权限)

> Issue: [#3](https://github.com/notnotype/hermes-plugin-onebot11/issues/3) · PR: [#4](https://github.com/notnotype/hermes-plugin-onebot11/pull/4)
> 计划: `~/.hermes/plans/2026-08-04_2150-onebot11-group-session-v2.md`

## 目标

把群聊从"每用户会话 + 仅 @ 触发"升级为"一群一会话 + 监听全部消息进队列 + 多触发(mention/关键词/LLM) + 触发时拼接群聊摘要 + 工具调用侧角色权限(超级管理员/普通用户)",让机器人更接近真人群聊体验。

## 变更内容

### 新模块(onebot11/ 包,零 Hermes 依赖)

| 模块 | 职责 |
|---|---|
| `onebot11/queue.py` | 按群分桶的环形消息队列(条数上限/单条截断/快照/清空) |
| `onebot11/context.py` | 队列 → 群聊上下文文本(最近 N 条原文 + 更早消息可选 LLM 摘要 + 总长上限) |
| `onebot11/triggers.py` | 触发判定:mention 恒触发 + 关键词(正则)+ 可选 LLM 判定回调 |

### 改动(adapter.py / permissions.py / plugin.yaml / 文档)

- `permissions.py`:新增 `role_of`(超级管理员/普通用户)与 `check_role_tool_call`(调用侧角色守卫)
- `adapter.py`:
  - 群会话粒度开关 `ONEBOT11_GROUP_SESSIONS_PER_USER`(默认 false = 一群一会话;extra 字符串布尔正确解析)
  - 入站群消息:白名单 → 触发判定 → 未触发入队 / 触发则拼接上下文进会话
  - 工具调用侧角色守卫(admin-only 工具表,越权返回权限错误给 LLM)
  - 普通用户触发时注入角色说明
  - LLM 触发判定与队列摘要(经 `ctx.llm`,失败降级)
  - 废弃 `ONEBOT11_REQUIRE_MENTION`(@ 恒为触发源;保留向后兼容解析)
- `plugin.yaml` v0.2.0:新增 8 个 env 声明,删除 REQUIRE_MENTION
- 文档:README env 表 + 迁移说明、permissions.md 角色与调用侧权限、.env.example 全量更新

## 验证结果

- 协议层 57 passed(新增 queue/context/triggers/permissions 用例)
- adapter 层 30 passed(群会话粒度/队列触发/上下文拼接/角色守卫/LLM 接线/字符串布尔解析)
- ruff 全过;CI lint-test 通过(PR #4)
- 生产:网关加载 v2 代码,symlink → 本分支;日志确认新配置格式
- **审查发现的 bug 已修**:config.yaml 部署时 extra 字符串 `"false"` 被 `bool()` 误判为 True(per-user),已按字符串解析修复并补测试

## 配置要点(生产)

- `~/.hermes/.env`:删 `ONEBOT11_REQUIRE_MENTION`,加 `ONEBOT11_GROUP_SESSIONS_PER_USER=false`
- `~/.hermes/config.yaml`:`group_sessions_per_user: false`(**runner 级 session key 由顶层决定**,adapter extra 只影响并发守卫;两处必须一致)
- `ONEBOT11_ADMINS` 待填用户 QQ(超级管理员);`ONEBOT11_ADMIN_TOOLS` 按需

## 后续 TODO

- [ ] 真机验证(用户重启 LLBot):未 @ 普通消息无响应只入队;关键词触发能引用此前群聊内容;@ 恒触发;普通用户调 admin 工具被拒;管理员可用
- [ ] 收尾(用户许可后):squash 合并 PR #4、删分支/worktree、symlink 指回 master、PROJECT-STATUS/RELEASE/changelog v0.2.0 更新
- [ ] v2.1:群角色感知(owner/admin/member,get_group_member_info)
- [ ] LLM 触发的成本控制(空闲窗口合并判定/速率上限)
