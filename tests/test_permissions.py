"""权限门禁测试：管理员列表 + 会话范围校验（群聊安全底线）。"""


import pytest

from onebot11.permissions import (
    CONFIG_WRITE_TOOLS,
    CallerContext,
    ToolContext,
    TurnBinding,
    TurnBindingStore,
    build_role_tools,
    build_trusted_users,
    chat_access_allowed,
    is_onebot_tool_name,
    parse_admin_list,
    role_for_user,
    validate_tool_call,
)


def test_解析管理员列表():
    assert parse_admin_list("111, 222,333") == {"111", "222", "333"}
    assert parse_admin_list("") == set()
    assert parse_admin_list(None) == set()


def test_访问策略群和私聊都必须显式满足合同():
    """群白名单、私聊 allowlist 和 open 的显式 allow-all 规则保持一致。"""
    assert chat_access_allowed("group", "1072992996", allowed_groups={"1072992996"})
    assert not chat_access_allowed("group", "786830134", allowed_groups={"1072992996"})
    assert chat_access_allowed(
        "dm",
        "2056963663",
        allowed_users={"2056963663"},
        dm_policy="allowlist",
    )
    assert not chat_access_allowed("dm", "2056963663", dm_policy="open")
    assert chat_access_allowed("dm", "2056963663", dm_policy="open", allow_all_users=True)
    assert not chat_access_allowed("unknown", "1072992996")


def test_普通工具群聊可用():
    """qq_get_group_msg_history 在群聊中允许（本群会话）。"""
    ctx = ToolContext(user_id="123", chat_type="group", chat_id="888")
    assert validate_tool_call("qq_get_group_msg_history", {}, ctx, {"999"}) is None


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


def test_同一turn不可换绑调用者():
    """精确 session/turn 绑定一旦建立就不能换成另一个目标。"""
    store = TurnBindingStore()
    first = CallerContext(user_id="1", chat_type="group", chat_id="888")
    second = CallerContext(user_id="2", chat_type="group", chat_id="999")
    store.bind(TurnBinding("session", "turn", first))
    with pytest.raises(ValueError):
        store.bind(TurnBinding("session", "turn", second))


def test_角色优先级和精确工具名():
    """trusted_user 只按 QQ 白名单和精确工具名生效。"""
    extra = {
        "roles": {
            "user": {"tools": ["qq_get_message"]},
            "trusted_user": {"users": ["200"], "tools": ["web_search", "terminal"]},
            "super_admin": {"tools": ["onebot_get_permissions"]},
        }
    }
    role_tools = build_role_tools(extra)
    assert role_tools["user"] == frozenset({"qq_get_message"})
    assert role_tools["trusted_user"] == frozenset({"web_search", "terminal"})
    assert "terminal" not in role_tools["user"]
    assert build_trusted_users(extra) == {"200"}
    assert role_for_user("200", set(), {"200"}) == "trusted_user"
    assert role_for_user("200", {"200"}, set()) == "super_admin"
    with pytest.raises(ValueError):
        build_role_tools({"roles": {"user": {"tools": ["browser_*"]}}})
    with pytest.raises(ValueError):
        build_role_tools({"roles": {"user": {"tools": ["delegate_task"]}}})


def test_通用Hermes工具也遵守角色快照():
    """非 qq 工具不再绕过同一精确工具名门禁。"""
    ctx = CallerContext(
        user_id="200",
        chat_type="group",
        chat_id="888",
        role="trusted_user",
        allowed_tools=frozenset({"web_search"}),
    )
    assert validate_tool_call("web_search", {}, ctx, set()) is None
    assert validate_tool_call("terminal", {}, ctx, set()) is not None
    assert validate_tool_call(next(iter(CONFIG_WRITE_TOOLS)), {}, ctx, set()) is not None


def test_OneBot工具命名空间未知名称也必须识别():
    """qq_ 和 onebot_ 工具都不能靠未知名称绕过 fail-closed。"""
    assert is_onebot_tool_name("qq_get_message")
    assert is_onebot_tool_name("onebot_set_role_tools")
    assert is_onebot_tool_name("onebot_unknown")
    assert not is_onebot_tool_name("web_search")
