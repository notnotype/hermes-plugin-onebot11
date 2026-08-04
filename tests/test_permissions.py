"""权限门禁测试：管理员列表 + 会话范围校验（群聊安全底线）。"""

import pytest

from onebot11.permissions import ToolContext, parse_admin_list, validate_tool_call


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


def test_私聊历史非admin被拒():
    """qq_get_friend_msg_history 非管理员拒绝。"""
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    err = validate_tool_call("qq_get_friend_msg_history", {}, ctx, {"999"})
    assert err is not None
    assert "管理员" in err


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


def test_管理员列表为空时全部放开():
    """admins 为空 = 所有已授权用户同权（开放模式）。"""
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    assert validate_tool_call("qq_get_friend_msg_history", {}, ctx, set()) is None


def test_查询单条消息普通可用():
    """qq_get_message 任何已授权会话可用。"""
    ctx = ToolContext(user_id="123", chat_type="group", chat_id="888")
    assert validate_tool_call("qq_get_message", {}, ctx, set()) is None


def test_未知工具默认放行():
    ctx = ToolContext(user_id="123", chat_type="dm", chat_id="123")
    assert validate_tool_call("qq_unknown", {}, ctx, set()) is None
