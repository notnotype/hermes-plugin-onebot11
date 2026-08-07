"""OneBot 11 协议逻辑包（零 Hermes 依赖,可独立测试）。"""

from . import (
    audit,
    config,
    confirm,
    context,
    dispatch,
    events,
    http_api,
    message,
    permissions,
    queue,
    tools,
    triggers,
    ws_server,
)
from .context import AgentContextParts, build_agent_context, build_agent_context_parts
from .dispatch import ActiveTurn, GroupDispatcher
from .message import ParsedMessage, parse_message_segments
from .permissions import CallerContext, ChatTarget, TurnBinding, TurnBindingStore
from .queue import (
    OperationRecord,
    OperationStart,
    QueueLease,
    QueueMessage,
    QueueStore,
    TriggerRequest,
)

__all__ = [
    "events",
    "http_api",
    "message",
    "permissions",
    "queue",
    "dispatch",
    "triggers",
    "confirm",
    "audit",
    "config",
    "context",
    "tools",
    "ws_server",
    "ParsedMessage",
    "parse_message_segments",
    "CallerContext",
    "ChatTarget",
    "TurnBinding",
    "TurnBindingStore",
    "QueueLease",
    "QueueMessage",
    "QueueStore",
    "TriggerRequest",
    "OperationRecord",
    "OperationStart",
    "ActiveTurn",
    "GroupDispatcher",
    "build_agent_context",
    "build_agent_context_parts",
    "AgentContextParts",
]
