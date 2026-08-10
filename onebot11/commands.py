"""OneBot 11 不依赖 Hermes 的会话级斜杠命令解析。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConversationCommand:
    """一个已识别的会话生命周期命令。"""

    name: str
    argument: str | None = None


def parse_conversation_command(text: str) -> ConversationCommand | None:
    """解析群内会话命令；管理命令和普通文本返回 ``None``。"""
    normalized = str(text or "").strip()
    if not normalized.startswith("/"):
        return None
    parts = normalized[1:].split(None, 1)
    command = parts[0] if parts else ""
    name = command.casefold()
    if name == "new":
        title = parts[1].strip() if len(parts) > 1 else ""
        return ConversationCommand(name, title or None)
    if name in {"reset", "clear"} and len(parts) == 1:
        return ConversationCommand(name)
    return None
