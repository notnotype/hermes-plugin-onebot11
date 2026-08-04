"""OneBot 11 协议逻辑包（零 Hermes 依赖,可独立测试）。"""

from . import events, http_api, message, permissions, tools, ws_server
from .message import ParsedMessage, parse_message_segments

__all__ = [
    "events",
    "http_api",
    "message",
    "permissions",
    "tools",
    "ws_server",
    "ParsedMessage",
    "parse_message_segments",
]
