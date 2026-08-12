# OneBot 11 权限与目标范围

本插件把“能进入 Hermes”与“当前 turn 能调用哪些工具”分开处理。OneBot 入站策略在入队前执行，工具权限在 `pre_tool_call` 和 handler 两层执行；提示词只用于让模型了解规则，不能代替硬校验。

## 入站访问策略

| 场景 | 合同 | 配置 |
|---|---|---|
| 群消息 | `allowed_groups` 非空时只接受列出的群；为空时接受群消息并按 trigger 决定是否启动 turn | `ONEBOT11_ALLOWED_GROUPS` |
| 私聊白名单 | 只接受列出的 QQ | `ONEBOT11_DM_POLICY=allowlist` + `ONEBOT11_ALLOWED_USERS` |
| 私聊关闭 | 直接丢弃，不进入队列或 session | `ONEBOT11_DM_POLICY=disabled` |
| 私聊开放 | 只有显式 allow-all 才接受；没有 allow-all 时 fail-closed | `ONEBOT11_DM_POLICY=open` + `ONEBOT11_ALLOW_ALL_USERS=true`，或 `GATEWAY_ALLOW_ALL_USERS=true` |

未知 `dm_policy`、`group_sessions_per_user=true` 或非 `shared` session 配置都会拒绝启动。插件声明 `enforces_own_access_policy=True`，
通过 adapter 访问策略的消息会以 `role_authorized=True` 进入 Hermes，避免被网关默认 allowlist 再次误拒绝。
`extra`、roles、旁路模型和数值边界由 adapter 构造与 `validate_config()` 共用同一解析器；
显式错误类型不会被 `or {}` 静默吞掉。

群消息即使没有 @、关键词或 `always` trigger，也会先写入 SQLite 队列；这保证触发时能拿到上次触发以来的上下文。没有 trigger 不会启动 Agent turn。

群 turn 认领后，插件默认使用 LLBot 的 `set_msg_emoji_like` 扩展给触发消息添加 `emoji_id=128172`（💬，表示正在回复这一条，可通过 `processing_reaction_emoji_id` 配置），Hermes turn 收尾时发送 `set=false` 移除。问句/记忆候选进入旁路 selector 判断时，先给候选消息添加 `emoji_id=128064`（👀，表示 bot 正在看这条消息），判断结束（触发、忽略、超时或 wait 到期）后移除。两个指示器都只作用于当前群的真实消息 ID；内部 hash、私聊消息或 lease 已失效时跳过。reaction 是 best-effort UI 提示，失败或结果未知不会阻断 Agent 回复、队列 ack，也不会重放 `set=true`。清理记录持久化在队列 SQLite 中，启动恢复最多有限次数地尝试 `unset`；达到上限后只在状态/审计中保留，不会无限刷屏。进程硬崩溃遗留的远端 💬/👀 不纳入清理承诺。emoji ID 说明：`9203`（⏳）与 `8971` 在 QQ reaction API 上显示异常，因此回复阶段默认使用实测可用的 `128172`（💬）。

## 角色与工具

超级管理员由 QQ 号列表定义：

```yaml
platforms:
  onebot11:
    extra:
      super_admins: ["10001"]
      roles:
        user:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info]
        trusted_user:
          users: []
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_group_info]
        super_admin:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info, qq_delete_message,
                  qq_set_group_ban, qq_set_group_kick, qq_set_group_whole_ban]
```

`ONEBOT11_SUPER_ADMINS` 优先，`ONEBOT11_ADMINS` 仅作为兼容旧名。超级管理员为空时没有任何 OneBot 群管理写权限；普通用户默认只有只读工具。`roles.trusted_user.users` 只定义 trusted_user 身份；trusted_user 不能配置 OneBot 群管理写工具，但可以按工具名逐项配置 Hermes generic 工具（例如网页、浏览器、终端或文件能力）。它不能修改权限、白名单或角色配置。可配置的角色工具集合会取所有角色许可工具的并集注册到 Hermes，再由当前 turn 的角色门禁限制实际执行。

默认权限（未显式配置 `tools` 时）：

- `super_admin`：全部 OneBot 工具 + 全部 Hermes 通用工具；`tool_search` 始终禁止，`delegate_task` 作为显式委派能力使用；OneBot 群管理写工具仍需当前群 + 确认令牌。
- `user`：五个只读 OneBot 工具。
- `trusted_user`：五个只读 OneBot 工具；需要 Hermes 通用能力时必须在 `tools` 中逐项显式配置。

权限不是 Agent 工具：没有 `set_role`/`set_tools` 之类的运行时权限修改工具，角色和白名单只通过
`config.yaml`（或环境变量）修改文件并 reload/重启生效。拥有 shell 权限即拥有改权限文件的权限。

`pre_llm_call` 会把 `user`、`trusted_user`、`super_admin` 的当前工具目录写进
提示词，帮助模型理解角色差异；这只是 role catalog，不是授权来源。真正授权仍来自
当前锚点的不可变 authority 快照、`(session_id, turn_id)` binding、lease、adapter
状态和工具 handler 硬校验。Hermes 的 generic tool-search bridge 可能在普通
`pre_tool_call` 之前只读取 catalog；这不是授权或执行入口，OneBot 不在插件内
monkey patch Hermes。`delegate_task` 可以显式加入角色工具目录，但不把 OneBot QQ
权限传给 Hermes 子代理。开启 `main_agent_read_only: true` 后，主 agent 直接只能
使用 Hermes 的只读工具；子代理在 Hermes 的 delegated-child 上下文中可以使用项目
工具（包括 terminal、process、read_file、search_files、write_file、patch），但不能
调用 QQ 工具、`send_message`、`cronjob` 或再次委派。缺少 delegated-child 上下文时
按主 agent 处理并 fail-closed。

网页搜索、浏览器自动化、终端、文件读写等 Hermes 通用工具在 OneBot turn 中也会
进入插件的 `pre_tool_call` 角色硬门禁；普通用户不会因为工具名不是 `qq_*` 就自动获得
权限。`tool_search` 始终禁止；`delegate_task` 是显式委派能力，只有角色工具快照允许时
主 agent 才能调用。启用 `main_agent_read_only` 后，即使 super_admin 也不能直接使用
terminal、process、write_file、patch 或 execute_code；但可以直接使用
read_file/search_files 做只读检查，并把执行工作委派给子代理。子代理的工具仍受
OneBot 父 turn 的 binding、lease、epoch 和访问策略约束；子代理不能调用 QQ 工具、
send_message、cronjob 或再次委派。`trusted_user` 或 `user` 要开放 generic 能力，
仍只能在 `tools` 中逐项配置。该门禁依赖 Hermes 传递精确 `(session_id, turn_id)`；
缺少父 binding 时 fail-closed，不伪造权限继承。

主 agent 只读时不需要通过 terminal 执行 `rg`：Hermes 原生 `search_files` 的
`target=content` 是内容正则搜索（Grep），`target=files` 是 glob 文件搜索（Glob），
`read_file` 用于读取文件。只有实际修改、运行测试或执行 shell 工作才交给
`delegate_task` 子代理。

只读不限制 QQ 群管理写工具：撤回、禁言、踢人、全员禁言仍由 super_admin + 当前群 +
`/onebot confirm` 确认令牌把关。主 agent 的 `read_file` 不能读取 `.env`、
`auth.json`、`auth.lock` 等凭据文件，避免密钥进入模型上下文；
`config.yaml`/`roles.yaml` 仍可读。

默认只读工具：

- `qq_get_message`：返回的消息必须属于当前群或当前私聊。
- `qq_get_group_msg_history`：只能在当前群查询，群号不从模型参数读取。
- `qq_get_friend_msg_history`：只能在当前私聊查询当前用户。
- `qq_get_group_info`、`qq_get_group_member_info`：只能作用于当前群。

写工具（撤回、禁言、踢人、全员禁言）只能作用于当前群，普通用户永远拒绝。首次调用只产生预览和短期 `/onebot confirm TOKEN`，不会立即执行；确认命令必须由同一超级管理员在同一群发送，令牌单次消费且不写入审计日志。确认命令在 adapter 入站层直接处理，不会进入 Hermes session 或消息队列。

确认令牌仍只保存在当前 adapter 进程内存中，进程重启会让旧令牌失效；但管理动作台账持久化在同一个队列 SQLite 中。进程恢复会把遗留的 `started` 标记为 `unknown`，同一 fingerprint 在 `unknown` 状态下禁止重复调用。`/onebot resolve action retry OPERATION_ID` 只把动作置为 `retry_armed`，随后必须重新生成预览并再次确认；`discard` 只记录放弃，不访问 OneBot。审计只保留 operation id、fingerprint 摘要、工具、目标和结果，不记录 token、完整参数或媒体 URL。

## 中间正文

Hermes 在 ReAct 过程中产生的 AI 中间评论（commentary）、工具进度和状态提示会直调 adapter 的
`send()`；OneBot 插件用 `_send_with_retry`（最终回复）与直调 `send()`（中间消息）区分二者。
默认私聊展示中间正文；群聊默认隐藏（避免刷屏），但可设置 `show_interim_group: true`
开启（长任务如"生成一张图片"时能实时看到进度）。`show_interim_group` / `show_interim_dm`
配置热更新生效。最终回复始终发送，不受该配置影响；
Hermes cron 或系统通知若直发到群，也会按群聊中间正文规则处理，请按需配置。

## 运行时 reload

超级管理员可直接发送 `/onebot reload`。它通过 Hermes 的当前 gateway
configuration loader 读取已经合并过的 `platforms.onebot11.extra`，再由插件自己的
严格解析器校验；不在 OneBot 内复制 Hermes 的 YAML merge 规则。环境变量仍是覆盖层，
所以如果某个白名单字段由环境变量提供，YAML reload 不能覆盖它。

可热更新：`allowed_groups`、DM 白名单/策略、`super_admins`、`trusted_users`、
`roles.*.tools`、关键词/always/LLM trigger、cooldown、reaction、一次性长时间提示延迟
和纯文本显示配置。
不可热更新：HTTP/WS 地址、token、self_id、队列数据库路径、session 模式以及其它
连接/协议边界；这些变化会返回“需要重启”并保留旧 snapshot。解析失败也保留旧
snapshot。新消息、新 anchor 和恢复任务使用新策略；active turn 继续使用创建时的
authority 和工具快照，但每次工具/出站仍重新检查当前白名单。成功 reload 会使旧
confirmation token 失效。

## 权限修改 SOP 与写保护

权限的唯一事实来源是独立文件 `~/.hermes/onebot11/roles.yaml`（存在时覆盖
config.yaml 的 `roles`/`super_admins`/`admins`）。修改流程固定为：

1. 站长手动编辑 `roles.yaml`（代理不直接改文件）；
2. 群内发送 `/onebot reload` 生效；
3. 站长明确授权时，代理可以先展示 diff，但仍由站长落地。

Hermes 的 file 工具对 `~/.hermes/config.yaml` 有写保护；OneBot 侧在
`pre_tool_call` 对 `terminal` 命令再做一层兜底：检测到写意图
（重定向/tee、sed/perl -i、python 写文件、mv/cp/rm、编辑器）指向
`config.yaml`、`roles.yaml`、`.env`、`auth.json`、`auth.lock` 时直接拒绝，
防止代理用 shell 绕过文件写保护。纯读取（cat/grep）不受影响。

## 身份传递

每次入站 turn 都创建不可变 `CallerContext`。Hermes 的 `session_key` 只用于 session 路由，不能当作身份；工具身份按完整 `(session_id, turn_id)` 绑定。handler 或 hook 只收到其中一个坐标时，不得使用 ContextVar 猜测缺失的另一个坐标，必须 fail-closed。
Hermes 的 worker thread 与 async final delivery 可能不共享 `ContextVar`；最终文本和图片出站在缺少当前 task binding 时，只能从当前 synthetic event 的精确 `onebot11_binding_key` 恢复，并继续校验 binding store、adapter epoch、机器人 `self_id`、lease、目标和访问策略。没有该 key、key 冲突或恢复失败时不访问 OneBot。

出站目标使用明确的 `ChatTarget(group|dm, chat_id)`。当前 turn 只能向它绑定的目标发送；同一个数字同时被识别为群号和 QQ 号时，未带明确类型的发送会被拒绝。

## 队列与不确定结果

群队列是持久 SQLite 状态机；当前 schema 为 12，启动时自动迁移已知旧表结构，未知更高版本拒绝启动。缺少或损坏 authority 快照的旧 anchor 会进入 `uncertain`，不自动执行；同一群可以有多个 pending TurnAnchor，但同一时间最多一个活动 lease，并按 anchor 序号串行处理。每个 anchor 绑定一个真实消息和固定 batch 边界，后续消息不会被旧 turn 偷吃。旧 lease 在失败、恢复或断开结算时保留自己的 anchor，不会通过唯一索引冲突卡住后续 anchor。
@、关键词、always 和管理员 flush 属于硬触发，会为明确消息创建/升级 anchor，并绑定该消息的权限主体和 reaction 目标；普通恢复或 LLM selector 只能选择仍存在的 pending 消息，不能把结果静默改绑到另一条消息。selector 的 `anchor_seq` 不是权限来源，权限只从该真实消息的入队快照继承。

```text
pending -> leased(agent_running) -> acked/deleted
leased --明确失败且未开始出站--> pending（2、4、8 秒退避，最多 3 次）
leased --出站已开始、lease 过期或阶段未知--> uncertain
leased --达到失败上限--> failed
uncertain --管理员 retry（生成新 anchor）--> pending
uncertain --管理员 discard--> deleted
failed --管理员 retry--> pending
failed --管理员 discard--> deleted
```

消息入队允许至少一次；OneBot 非幂等 HTTP 请求（发送、撤回、禁言、踢人、全员禁言）不自动重试。连接断开、非 JSON 响应、超时、5xx 或部分分块成功时，结果可能是 `unknown`，插件不会重新执行整轮 Agent，必须由管理员 `/onebot resolve retry|discard` 明确处理。lease 一旦写入出站 marker，任何明确错误也不会自动 release；队列消息的 `retry` 只会在 authority、anchor 消息和 batch 边界都可证明时生成新的 request_id，仍可能再次执行动作，因此只应在确认目标端没有执行后使用。缺少旧 trigger、legacy anchor 或 authority 快照损坏的记录不会猜测权限，`retry` 会保持 hold，管理员应选择 discard 或发送新的明确触发消息。管理动作台账的 `retry` 只解除该动作 fingerprint 的阻断，不会直接调用 API。完成 ack/release 只有在 SQLite 原子状态转换成功后才会推进下一轮。

主动 disconnect 不增加失败次数；只有过期的明确 `agent_running` lease 才消耗有限恢复预算。lease phase 缺失或未知时统一进入 `uncertain`，不自动重放。

OneBot 出站图片只能来自 Hermes 允许的媒体根目录，并在发送前校验扩展名、
魔数和大小；插件把文件编码为受限 `base64://` image segment，不向外部
图片 URL 携带 OneBot Bearer token，也不把失败的图片 URL/路径回退成文本。
Agent 最终回复图片是 best-effort；图片-only、文字+图片和多图都由插件逐块
尝试发送，部分成功或 unknown 不自动重放整轮 Agent。通用 `send_message`、
cron 和 standalone sender 的 plugin media 不在本轮可靠性合同内。

## 运维命令

超级管理员可在目标群发送：

`/onebot status`、`/onebot queue`、`/onebot flush`、`/onebot clear`、`/onebot pause`、`/onebot resume`、`/onebot resolve retry`、`/onebot resolve discard`、`/onebot resolve action retry OPERATION_ID`、`/onebot resolve action discard OPERATION_ID`、`/onebot confirm TOKEN`。

`pause` 只停止自动 dispatch，消息继续入队；`clear` 清理 pending 消息和滚动摘要但不删除 Hermes session 历史，活动 lease、`uncertain` 或 `failed` 必须先显式处理。

群级会话生命周期命令是另一条边界：超级管理员可以发送 `/new [title]`、`/reset` 或 `/clear`。
它们在普通群消息入队前被识别；未授权用户会收到权限拒绝，命令本身不会进入 OneBot
队列或 Hermes session。`/clear` 作为 OneBot 别名翻译为公共 `/new`，reset 成功后由
`on_session_reset` hook 清理当前群队列、摘要和内存触发状态。reset 使用命令开始时的消息
序号作为边界，不会误删 reset 期间新到的消息；reset 期间普通消息需要稍后重发。

## 分层触发和旁路模型

群消息的触发顺序是“硬触发优先，候选消息再仲裁”：

- @、关键词、`always` 和管理员命令直接创建持久 trigger，不调用 LLM。
- 空闲状态只把问句或带有“之前/上次/刚才/继续”等回指词、且当前群已有摘要或最近原文的消息送入候选。
- 候选消息使用 5 秒 trailing debounce；每群最多一个判断任务，冷却期间不创建判断。
- 旁路模型必须显式配置 provider、model 和群 allowlist。判断由插件自有 Node/pi-ai helper 发起，不经过 Hermes auxiliary，不调用主 Agent 作为隐式 fallback，也不主动切换 provider。`api_key_env` 只保存环境变量名，密钥值只从进程环境读取。
- 启用旁路判断需要 Node.js ≥22.19 和插件目录中的 `npm ci --omit=dev`；Node、依赖、provider/model、超时或模型结果异常时按 `ignore`，消息保留在 pending。
- 模型只能返回 `{"decision":"trigger","anchor_seq":123}`、`{"decision":"wait","anchor_seq":null}` 或 `{"decision":"ignore","anchor_seq":null}`。`trigger` 的序号必须真实存在于本次 pending 队列；非法 anchor、非法 JSON、超时或模型错误均不创建 trigger，消息留在 pending，并按 2/4/8 秒、最大 60 秒退避。新消息会提前唤醒判断。
- 成功 turn 后进入最多 60 秒 idle 活跃窗口，最长连续活跃时间 300 秒；重启后 active/engaged 状态回到 idle，只恢复 SQLite 消息和显式 durable trigger。
- **engage 三档预算**（只影响 debounce/窗口/仲裁次数/超时/输入大小，三态合同不变）：
  - `deep`：bot 上轮回复以问句或请求短语收尾（`bot_asked`，只检查尾部 80 字）且同用户回复、消息引用 bot 上一条回复、或命中任务词（报错/复现/日志等）。同用户 follow-up 免 debounce 立即判断；窗口 180s/900s、最多 4 次仲裁、超时 45s、输入 20KB；`wait` 状态攒满 2 条新消息立即判。
  - `normal`（默认）：60s/300s、2 次仲裁、超时 30s、输入 12KB。
  - `shallow`：连续 2 次 ignore 后降级，窗口 30s/120s、1 次仲裁、超时 12s、输入 6KB；开启 `short_rule_max_chars` 后，无信号短消息本地 ignore 不进 selector。
  - 他人插话不享受 deep；任何硬触发或新成功 turn 都回到 normal；重启后回 idle/normal。
- status 中的 debounce/wait/engaged 时间以剩余秒数展示；LLM 审计区分实际 semaphore 等待时长、模型失败和结果未知。

群历史摘要通过 Hermes 支持的 `channel_prompt` 临时注入，当前批次才写入普通 user transcript；摘要被标记为“不可信群消息数据”，其中的指令不能覆盖系统规则。上下文每条消息同时展示 `seq`、真实 `message_id`、`message_key`、用户、role、reply 和媒体标记。无真实 ID 时使用 `message_id=""` 与 `message_key="hash:<sha256>"` 分离保存；`qq_get_message` 和撤回工具会结构化拒绝 hash key。旧 Hermes 没有 `channel_prompt` 时退回有界单文本模式，并写入审计。群 `/context`、`/ctx` 是入队前旁路诊断，不进入 session 或队列。

这些规则只决定“是否启动一轮 Agent”，不改变角色权限。实际工具调用仍必须通过当前 `(session_id, turn_id)` binding、访问策略和 lease fencing。

## Home channel cron

定时任务第一阶段只保证显式配置的 `home_channel`。如果配置了目标 ID，必须同时配置
`home_channel_type=group|dm`；群目标仍检查 `allowed_groups`，私聊目标仍检查 DM policy 和
`allowed_users`。没有明确类型、目标不在白名单或发送响应缺少 `message_id` 时 fail-closed/unknown，
不会根据 QQ 号长度猜测目标类型。
