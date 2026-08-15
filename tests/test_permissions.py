"""权限门禁测试：管理员列表 + 会话范围校验（群聊安全底线）。"""


import pytest

from onebot11.permissions import (
    WRITE_TOOLS,
    CallerContext,
    ToolContext,
    TurnBinding,
    TurnBindingStore,
    access_allowed,
    build_role_tools,
    build_trusted_users,
    file_tool_reads_sensitive_config,
    file_tool_writes_sensitive_config,
    parse_admin_list,
    role_for_user,
    role_prompt,
    terminal_writes_sensitive_config,
    validate_message_scope,
    validate_tool_call,
)


def test_解析管理员列表():
    assert parse_admin_list("111, 222,333") == {"111", "222", "333"}
    assert parse_admin_list("") == set()
    assert parse_admin_list(None) == set()


def test_普通工具群聊可用():
    """qq_get_group_msg_history 在群聊中允许（本群会话）。"""
    ctx = ToolContext(user_id="123", chat_type="group", chat_id="888")
    assert validate_tool_call("qq_get_group_msg_history", {}, ctx, {"999"}) is None


def test_trusted_user优先级和只读边界():
    """trusted_user 可以明确授予只读工具，但永远不能使用群管理写工具。"""
    assert role_for_user("1", {"1"}, {"1"}) == "super_admin"
    assert role_for_user("2", set(), {"2"}) == "trusted_user"
    assert role_for_user("3", set(), {"2"}) == "user"
    ctx = ToolContext(
        user_id="2",
        chat_type="group",
        chat_id="888",
        role="trusted_user",
        allowed_tools=frozenset({"qq_get_message", "qq_set_group_ban"}),
    )
    assert validate_tool_call("qq_set_group_ban", {}, ctx, set()) is not None


def test_trusted_user工具和用户列表只能来自明确配置():
    """trusted_user 的 users/tools 配置必须是 mapping，且不能带写工具。"""
    extra = {
        "roles": {
            "trusted_user": {
                "users": ["2056963663"],
                "tools": ["qq_get_message"],
            }
        }
    }
    assert build_trusted_users(extra) == frozenset({"2056963663"})
    assert build_role_tools(extra)["trusted_user"] == frozenset({"qq_get_message"})
    with pytest.raises(ValueError, match="只能包含只读工具"):
        build_role_tools(
            {
                "roles": {
                    "trusted_user": {
                        "users": ["2056963663"],
                        "tools": ["qq_set_group_ban"],
                    }
                }
            }
        )


def test_群历史工具私聊被拒():
    """qq_get_group_msg_history 在私聊中拒绝。"""
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    err = validate_tool_call("qq_get_group_msg_history", {}, ctx, set())
    assert err is not None
    assert "群" in err


def test_群信息工具私聊被拒():
    """群信息工具不能把私聊 QQ 号误当成群号。"""
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    assert validate_tool_call("qq_get_group_info", {}, ctx, set()) is not None
    assert validate_tool_call("qq_get_group_member_info", {}, ctx, set()) is not None


def test_私聊历史普通用户可用():
    """普通用户默认允许当前私聊的只读查询。"""
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    err = validate_tool_call("qq_get_friend_msg_history", {}, ctx, {"999"})
    assert err is None


def test_私聊历史admin在群聊被拒():
    """管理员在群聊里也查不了私聊历史。"""
    ctx = ToolContext(user_id="999", chat_type="group", chat_id="888")
    err = validate_tool_call("qq_get_friend_msg_history", {}, ctx, {"999"})
    assert err is not None
    assert "私聊" in err


def test_私聊历史admin在私聊允许():
    """管理员在私聊会话中允许查私聊历史（只能查自己,由 adapter 注入 user_id）。"""
    ctx = ToolContext(user_id="999", chat_type="dm", chat_id="999")
    assert validate_tool_call("qq_get_friend_msg_history", {}, ctx, {"999"}) is None


def test_管理员列表为空时普通用户仍可读():
    """超级管理员为空不隐式放开写权限；普通用户仍可读。"""
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    assert validate_tool_call("qq_get_friend_msg_history", {}, ctx, set()) is None


def test_查询单条消息普通可用():
    """qq_get_message 任何已授权会话可用。"""
    ctx = ToolContext(user_id="123", chat_type="group", chat_id="888")
    assert validate_tool_call("qq_get_message", {}, ctx, set()) is None


def test_未知工具默认拒绝():
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    assert validate_tool_call("qq_unknown", {}, ctx, set()) is not None


def test_超级管理员默认拥有Hermes通用工具():
    """super_admin 即使未在 tools 中列出 terminal 等通用工具也应放行。"""
    ctx = CallerContext(
        user_id="2056963663",
        chat_type="dm",
        chat_id="2056963663",
        role="super_admin",
        allowed_tools=frozenset({"qq_get_message"}),
        self_id="3101482118",
    )
    assert validate_tool_call("terminal", {}, ctx) is None
    assert validate_tool_call("read_file", {}, ctx) is None
    assert validate_tool_call("skill_view", {}, ctx) is None
    # OneBot 工具仍必须出现在角色的工具集合中。
    assert validate_tool_call("qq_set_group_ban", {}, ctx) is not None
    # delegate_task 是显式的 generic 编排能力；tool_search 仍始终拒绝。
    assert validate_tool_call("delegate_task", {}, ctx) is None
    assert validate_tool_call("tool_search", {}, ctx) is not None


def test_普通用户无权调用Hermes通用工具():
    """user 角色未显式配置的通用工具必须拒绝，不能因为非 qq_ 前缀自动放行。"""
    ctx = CallerContext(
        user_id="123",
        chat_type="dm",
        chat_id="123",
        role="user",
        allowed_tools=frozenset({"qq_get_message"}),
        self_id="3101482118",
    )
    assert validate_tool_call("terminal", {}, ctx) is not None


def test_trusted_user显式配置的通用工具放行():
    """trusted_user 通过 roles 显式配置 image_generate 后应放行。"""
    ctx = CallerContext(
        user_id="1259901822",
        chat_type="group",
        chat_id="1072992996",
        role="trusted_user",
        allowed_tools=frozenset({"qq_get_message", "image_generate"}),
        self_id="3101482118",
    )
    assert validate_tool_call("image_generate", {}, ctx) is None
    # 未显式配置的通用工具仍拒绝。
    assert validate_tool_call("terminal", {}, ctx) is not None
    # OneBot 只读工具按配置放行。
    assert validate_tool_call("qq_get_message", {}, ctx) is None


def test_trusted_user未配置image_generate时拒绝():
    """roles 没给 trusted_user 配 image_generate 时必须 fail-closed。"""
    ctx = CallerContext(
        user_id="1336488699",
        chat_type="group",
        chat_id="1072992996",
        role="trusted_user",
        allowed_tools=frozenset({"qq_get_message"}),
        self_id="3101482118",
    )
    assert validate_tool_call("image_generate", {}, ctx) is not None


def test_主agent只读模式允许文件检索和delegate但拒绝执行():
    """主 agent 可以用 Hermes 原生 search_files 做 Grep/Glob，不需要 shell。"""
    ctx = CallerContext(
        user_id="2056963663",
        chat_type="group",
        chat_id="942513604",
        role="super_admin",
        allowed_tools=frozenset({"search_files", "read_file"}),
        self_id="3101482118",
    )
    assert validate_tool_call(
        "search_files", {}, ctx, main_agent_read_only=True
    ) is None
    assert validate_tool_call(
        "read_file", {}, ctx, main_agent_read_only=True
    ) is None
    assert validate_tool_call(
        "delegate_task", {}, ctx, main_agent_read_only=True
    ) is None
    assert validate_tool_call(
        "terminal", {}, ctx, main_agent_read_only=True
    ) is not None
    assert validate_tool_call(
        "write_file", {}, ctx, main_agent_read_only=True
    ) is not None


def test_子代理继承shell但不继承OneBot和编排权限():
    """子代理可以执行项目 shell，但不能再次派发、发消息或调用 QQ 工具。"""
    ctx = CallerContext(
        user_id="2056963663",
        chat_type="group",
        chat_id="942513604",
        role="super_admin",
        allowed_tools=frozenset(),
        self_id="3101482118",
    )
    assert validate_tool_call(
        "terminal", {}, ctx, main_agent_read_only=True, delegated_child=True
    ) is None
    assert validate_tool_call(
        "write_file", {}, ctx, main_agent_read_only=True, delegated_child=True
    ) is None
    assert validate_tool_call(
        "qq_get_message", {}, ctx, delegated_child=True
    ) is not None
    assert validate_tool_call(
        "delegate_task", {}, ctx, delegated_child=True
    ) is not None


def test_子代理截图提示只允许manifest媒体事实来源():
    """delegated child 不得把 stdout、evidence 或裸路径伪装成 MEDIA。"""
    context = CallerContext(
        user_id="1259901822",
        chat_type="group",
        chat_id="942513604",
        role="user",
        allowed_tools=frozenset(),
        self_id="3101482118",
    )
    prompt = role_prompt(context, delegated_child=True)
    assert "repository-research-adapter profile" in prompt
    assert "严格执行其 command" in prompt
    assert "manifest 是唯一事实来源" in prompt
    assert "manifest.evidence.mediaFiles" in prompt
    assert "runner stdout 都不能直接改写成 MEDIA:" in prompt
    assert "mediaFiles 缺失或校验失败时不得输出 MEDIA:" in prompt


def test_search_files覆盖grep和glob说明():
    """权限提示明确告诉模型 search_files 是只读 Grep/Glob 替代。"""
    context = CallerContext(
        user_id="2056963663",
        chat_type="group",
        chat_id="942513604",
        role="super_admin",
        allowed_tools=frozenset({"read_file", "search_files", "delegate_task"}),
        self_id="3101482118",
    )
    prompt = role_prompt(context, main_agent_read_only=True)
    assert "search_files" in prompt
    assert "Grep/Glob" in prompt


def test_主agent只读提示不展示被配置的写工具():
    """只读提示不能把角色快照中的 terminal/write_file 误报为可用。"""
    context = CallerContext(
        user_id="2056963663",
        chat_type="group",
        chat_id="942513604",
        role="trusted_user",
        allowed_tools=frozenset({"terminal", "write_file", "read_file"}),
    )
    prompt = role_prompt(context, main_agent_read_only=True)
    assert "当前允许工具：read_file" in prompt
    assert "terminal" not in prompt.split("当前允许工具：", 1)[1].split("\n", 1)[0]
    assert "write_file" not in prompt.split("当前允许工具：", 1)[1].split("\n", 1)[0]


def test_只读模式超级管理员QQ写工具仍可用():
    """只读只限 shell/文件/代码执行；QQ 写工具仍走 super_admin + 确认令牌。"""
    super_ctx = CallerContext(
        user_id="2056963663",
        chat_type="group",
        chat_id="942513604",
        role="super_admin",
        allowed_tools=WRITE_TOOLS | {"qq_get_message"},
        self_id="3101482118",
    )
    assert validate_tool_call(
        "qq_set_group_ban", {}, super_ctx, main_agent_read_only=True
    ) is None
    user_ctx = CallerContext(
        user_id="1259901822",
        chat_type="group",
        chat_id="942513604",
        role="user",
        allowed_tools={"qq_get_message"},
        self_id="3101482118",
    )
    assert validate_tool_call(
        "qq_set_group_ban", {}, user_ctx, main_agent_read_only=True
    ) is not None


def test_敏感凭据文件读保护():
    """主 agent 只读模式可以查代码，但不能把凭据读进上下文。"""
    assert file_tool_reads_sensitive_config(".env") is not None
    assert file_tool_reads_sensitive_config("/opt/data/auth.json") is not None
    assert file_tool_reads_sensitive_config("~/auth.lock") is not None
    assert file_tool_reads_sensitive_config("config.yaml") is None
    assert file_tool_reads_sensitive_config("roles.yaml") is None
    assert file_tool_reads_sensitive_config(".env.example") is None
    assert file_tool_reads_sensitive_config("/opt/data/repo/neuro-book/README.md") is None
    assert file_tool_reads_sensitive_config("") is None


def test_只读提示保留QQ写工具与确认说明():
    """提示词必须告诉模型：QQ 写工具不受只读限制，但仍需确认。"""
    context = CallerContext(
        user_id="2056963663",
        chat_type="group",
        chat_id="942513604",
        role="super_admin",
        allowed_tools=WRITE_TOOLS | {"read_file", "search_files"},
        self_id="3101482118",
    )
    prompt = role_prompt(context, main_agent_read_only=True)
    assert "qq_set_group_ban" in prompt
    assert "/onebot confirm" in prompt


def test_客服只读提示要求先复用经验并自动后台委派():
    """客服主 agent 应先查已有经验，并使用 Hermes 的顶层后台委派合同。"""
    context = CallerContext(
        user_id="1259901822",
        chat_type="group",
        chat_id="942513604",
        role="user",
        allowed_tools=frozenset({
            "read_file",
            "search_files",
            "web_search",
            "delegate_task",
        }),
        self_id="3101482118",
    )
    prompt = role_prompt(context, main_agent_read_only=True)
    assert "delegate_task" in prompt
    assert "自动放到后台" in prompt
    assert "已有客服 evidence、项目 skill 和项目文档" in prompt
    assert "不要默认调用 skill_manage" in prompt
    assert "目录不可写时" in prompt
    assert "不要声称自己拥有未出现在当前允许工具列表中的浏览器" in prompt
    assert "不得声称截图已完成或服务已启动" in prompt


def test_客服媒体提示禁止裸路径改写为MEDIA():
    """提示词必须要求 manifest 媒体根复制和复核，拒绝裸截图路径。"""
    context = CallerContext(
        user_id="1259901822",
        chat_type="group",
        chat_id="942513604",
        role="user",
        allowed_tools=frozenset({"read_file", "search_files", "delegate_task"}),
        self_id="3101482118",
    )
    prompt = role_prompt(context, main_agent_read_only=True)
    assert "读取项目 adapter 的 manifest" in prompt
    assert "manifest 是媒体回传事实的唯一来源" in prompt
    assert "只有 manifest.evidence.mediaFiles 中明确给出的安全绝对路径" in prompt
    assert "重新检查 PNG 魔数、大小和 realpath" in prompt
    assert "runner stdout、manifest.evidence.files、仓库路径、evidence 路径、截图文件名或任意裸路径都不是媒体授权来源" in prompt
    assert "不能改写成 MEDIA:" in prompt
    assert "mediaFiles 缺失或为空" in prompt
    assert "先调用 skill_view 查看 repository-research" in prompt
    assert "严格执行 profile 的 command" in prompt
    assert "禁止临时拼接 bun run dev、product:start、裸 Playwright 或后台 shell" in prompt

def test_terminal写敏感配置被拦截():
    """terminal 写 config.yaml/roles.yaml/.env 等必须被 OneBot 侧拒绝。"""
    assert terminal_writes_sensitive_config("sed -i 's/a/b/' ~/.hermes/config.yaml") is not None
    assert terminal_writes_sensitive_config("echo x > ~/.hermes/onebot11/roles.yaml") is not None
    assert terminal_writes_sensitive_config("python3 -c \"open('/home/u/.hermes/.env','w').write('x')\"") is not None
    assert terminal_writes_sensitive_config("tee -a ~/.hermes/config.yaml") is not None
    assert terminal_writes_sensitive_config("mv /tmp/new ~/.hermes/auth.json") is not None
    assert terminal_writes_sensitive_config("rm ~/.hermes/auth.lock") is not None
    assert terminal_writes_sensitive_config("vim ~/.hermes/config.yaml") is not None


def test_terminal读取配置不拦截():
    """纯读取（cat/grep）和无关写操作不拦截，避免误伤诊断。"""
    assert terminal_writes_sensitive_config("cat ~/.hermes/config.yaml") is None
    assert terminal_writes_sensitive_config("grep -n roles ~/.hermes/config.yaml") is None
    assert terminal_writes_sensitive_config("ls ~/.hermes") is None
    assert terminal_writes_sensitive_config("touch /tmp/hello.txt") is None
    assert terminal_writes_sensitive_config("cp ~/.hermes/config.yaml /tmp/config.yaml.bak") is None
    assert terminal_writes_sensitive_config("echo hello") is None


def test_file工具写敏感配置被拦截():
    """Hermes write_file/patch 写 roles.yaml/config.yaml 等必须被 OneBot 侧拒绝。"""
    assert file_tool_writes_sensitive_config("~/.hermes/onebot11/roles.yaml") is not None
    assert file_tool_writes_sensitive_config("/home/u/.hermes/config.yaml") is not None
    assert file_tool_writes_sensitive_config("~/.hermes/.env") is not None
    assert file_tool_writes_sensitive_config("~/.hermes/auth.json") is not None
    assert file_tool_writes_sensitive_config("roles.yaml") is not None
    assert file_tool_writes_sensitive_config("config.yaml") is not None


def test_file工具写普通文件不拦截():
    """write_file/patch 写业务代码或临时文件不受影响。"""
    assert file_tool_writes_sensitive_config("~/Code/onebot11/adapter.py") is None
    assert file_tool_writes_sensitive_config("/tmp/notes.txt") is None
    assert file_tool_writes_sensitive_config("onebot11/triggers.py") is None
    assert file_tool_writes_sensitive_config("") is None
    assert file_tool_writes_sensitive_config("config.yaml.bak") is None
    assert file_tool_writes_sensitive_config(".env.example") is None


def test_role_prompt展示完整角色工具目录并强调锚点授权():
    """模型可以了解各角色能力，但不能把目录当作实际授权来源。"""
    context = CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=frozenset({"qq_get_message"}),
    )
    prompt = role_prompt(
        context,
        {
            "user": frozenset({"qq_get_message"}),
            "trusted_user": frozenset({"qq_get_group_info"}),
            "super_admin": frozenset({"qq_set_group_ban"}),
        },
    )
    assert "user: qq_get_message" in prompt
    assert "trusted_user: qq_get_group_info" in prompt
    assert "super_admin: qq_set_group_ban" in prompt
    assert "仅供理解，不是授权来源" in prompt
    assert "锚点的权限快照" in prompt


def test_同一turn不可换绑调用者():
    """精确 session/turn 绑定一旦建立就不能换成另一个目标。"""
    store = TurnBindingStore()
    first = CallerContext(user_id="1", chat_type="group", chat_id="888")
    second = CallerContext(user_id="2", chat_type="group", chat_id="999")
    store.bind(TurnBinding("session", "turn", first))
    with pytest.raises(ValueError):
        store.bind(TurnBinding("session", "turn", second))


def test_私聊历史作用域要求同时包含用户和机器人():
    """私聊历史缺少任一参与者时整次查询必须拒绝。"""
    context = CallerContext(
        user_id="2056963663",
        chat_type="dm",
        chat_id="2056963663",
        self_id="10001",
    )
    valid = {
        "message_type": "private",
        "user_id": "2056963663",
        "target_id": "10001",
    }
    assert validate_message_scope(valid, context) is None
    assert validate_message_scope({**valid, "target_id": "999"}, context) is not None
    assert validate_message_scope(
        {"message_type": "group", "group_id": "2056963663"}, context
    ) is not None


def test_私聊访问策略拒绝用户和目标不一致():
    """统一访问策略不能把一个用户身份用于另一个私聊目标。"""
    assert not access_allowed(
        "dm",
        "2056963663",
        "999",
        allowed_groups=set(),
        dm_policy="allowlist",
        allowed_users={"2056963663"},
    )
    assert access_allowed(
        "dm",
        "2056963663",
        "2056963663",
        allowed_groups=set(),
        dm_policy="allowlist",
        allowed_users={"2056963663"},
    )
