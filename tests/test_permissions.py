"""角色权限测试:角色解析 + 调用侧守卫。"""
from onebot11.permissions import (
    ToolContext,
    check_role_tool_call,
    role_of,
)


def test_角色解析():
    admins = {"10001"}
    assert role_of("10001", admins) == "admin"
    assert role_of("99999", admins) == "user"
    assert role_of("10001", set()) == "user"  # 管理员列表为空时无 admin


def test_admin工具普通用户被拒():
    ctx = ToolContext(user_id="99999", chat_type="group", chat_id="888")
    err = check_role_tool_call("qq_ban_member", ctx, admins={"10001"}, admin_tools={"qq_ban_member"})
    assert err and "仅管理员" in err


def test_admin工具管理员放行():
    ctx = ToolContext(user_id="10001", chat_type="group", chat_id="888")
    assert check_role_tool_call("qq_ban_member", ctx, admins={"10001"}, admin_tools={"qq_ban_member"}) is None


def test_非admin工具人人可用():
    ctx = ToolContext(user_id="99999", chat_type="group", chat_id="888")
    assert check_role_tool_call("qq_get_message", ctx, admins={"10001"}, admin_tools={"qq_ban_member"}) is None


def test_admin_tools为空时全部开放():
    ctx = ToolContext(user_id="99999", chat_type="group", chat_id="888")
    assert check_role_tool_call("qq_ban_member", ctx, admins={"10001"}, admin_tools=set()) is None
