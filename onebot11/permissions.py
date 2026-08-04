"""OneBot 11 权限模型：管理员列表 + 会话范围校验。

群聊是主场景,安全底线：工具只能作用于发起会话自身。
本模块零 Hermes 依赖,可独立测试。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ToolContext:
    """工具调用上下文（由 adapter 从 Hermes 会话注入）。

    - user_id: 发起者 QQ 号
    - chat_type: "group" | "dm"
    - chat_id: 群聊为 group_id,私聊为 user_id
    """

    user_id: str
    chat_type: str
    chat_id: str


def parse_admin_list(admins: str | None) -> set[str]:
    """解析 ONEBOT11_ADMINS（逗号分隔的 QQ 号）为集合。"""
    if not admins:
        return set()
    return {item.strip() for item in admins.split(",") if item.strip()}


def validate_tool_call(
    tool_name: str, params: dict, ctx: ToolContext, admins: set[str]
) -> str | None:
    """校验工具调用是否被允许,返回错误信息;None = 允许。

    规则：
    - qq_get_group_msg_history：仅限群聊（只能查当前群,群号由 adapter 注入）
    - qq_get_friend_msg_history：仅限私聊（只能查自己,QQ 由 adapter 注入）;
      管理员列表非空时还需是管理员
    - 其余工具（qq_get_message）：任何已授权会话可用
    - admins 为空 = 开放模式,只放宽管理员门槛;会话范围校验始终生效
    """
    if tool_name == "qq_get_group_msg_history":
        if ctx.chat_type != "group":
            return "该工具只能在群聊中使用"
        return None

    if tool_name == "qq_get_friend_msg_history":
        if ctx.chat_type != "dm":
            return "该工具只能在私聊中使用"
        if admins and ctx.user_id not in admins:
            return "该工具仅管理员可用"
        return None

    return None


def role_of(user_id: str, admins: set[str]) -> str:
    """角色解析:管理员列表命中 = admin,否则 = user。"""
    return "admin" if user_id in admins else "user"


def check_role_tool_call(
    tool_name: str, ctx: ToolContext, admins: set[str], admin_tools: set[str]
) -> str | None:
    """调用侧角色守卫:admin-only 工具被非管理员调用返回错误;None = 允许。

    与 validate_tool_call 组合使用(先范围校验,再角色守卫)。admin_tools 为空时
    所有工具对普通用户开放(向后兼容 v1 开放模式)。
    """
    if tool_name in admin_tools and ctx.user_id not in admins:
        return "权限不足:该工具仅管理员可用"
    return None
