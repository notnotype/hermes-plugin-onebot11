"""权限门禁测试：管理员列表 + 会话范围校验（群聊安全底线）。"""


import pytest

from onebot11.permissions import (
    CallerContext,
    ToolContext,
    TurnBinding,
    TurnBindingStore,
    parse_admin_list,
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
