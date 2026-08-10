"""OneBot 11 会话命令解析合同测试。"""

from onebot11.commands import ConversationCommand, parse_conversation_command


def test_解析new及可选标题():
    """``/new`` 可以没有标题，也可以保留标题中的空格。"""
    assert parse_conversation_command("/new") == ConversationCommand("new", None)
    assert parse_conversation_command("  /NEW   项目 讨论  ") == ConversationCommand(
        "new",
        "项目 讨论",
    )
    assert parse_conversation_command("/new\t项目\t讨论") == ConversationCommand(
        "new",
        "项目\t讨论",
    )


def test_解析reset和clear():
    """``/reset`` 与 ``/clear`` 是无参数会话命令。"""
    assert parse_conversation_command("/reset") == ConversationCommand("reset", None)
    assert parse_conversation_command("/clear") == ConversationCommand("clear", None)


def test_普通文本和onebot命令不属于会话命令():
    """普通消息和已有的 ``/onebot`` 管理命令交给其他入口处理。"""
    assert parse_conversation_command("你好") is None
    assert parse_conversation_command("/onebot status") is None
    assert parse_conversation_command("/newer") is None
    assert parse_conversation_command("") is None


def test_无参数命令拒绝多余参数():
    """reset/clear 不接受尾随参数，避免把误输入当成重置。"""
    assert parse_conversation_command("/reset now") is None
    assert parse_conversation_command("/clear all") is None
