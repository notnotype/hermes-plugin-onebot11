"""权限门禁测试：管理员列表 + 会话范围校验（群聊安全底线）。"""


import pytest

from onebot11.permissions import (
    CallerContext,
    ToolContext,
    TurnBinding,
    TurnBindingStore,
    access_allowed,
    build_role_tools,
    build_trusted_users,
    parse_admin_list,
    role_for_user,
    role_prompt,
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
    # FORBIDDEN 工具对超级管理员也始终拒绝。
    assert validate_tool_call("delegate_task", {}, ctx) is not None


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
