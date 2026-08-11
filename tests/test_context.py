"""OneBot11 群上下文分段和预算测试。"""

from onebot11.context import build_agent_context_parts
from onebot11.queue import QueueMessage


def _message(message_id: str, text: str) -> QueueMessage:
    """构造最小群消息。"""
    return QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id=message_id,
        user_id="123",
        user_name="小明",
        text=text,
        message_key=f"group:{message_id}",
    )


def test摘要作为临时提示而不是批次正文():
    """摘要和本轮消息必须可分别交给 Hermes 的不同生命周期。"""
    parts = build_agent_context_parts(
        "之前讨论过权限边界",
        (_message("1", "请继续"),),
        max_bytes=4096,
    )
    assert parts.summary_prompt is not None
    assert "之前讨论过权限边界" in parts.summary_prompt
    assert "之前讨论过权限边界" not in parts.batch_text
    assert "请继续" in parts.batch_text
    assert "message_id=1" in parts.batch_text
    assert "message_key=group:1" in parts.batch_text
    assert "user_id=123" in parts.batch_text
    assert "role=unknown" in parts.batch_text


def test预算不足仍保留最新消息():
    """裁剪时旧消息可以省略，但最新消息不能整体丢失。"""
    parts = build_agent_context_parts(
        "",
        (
            _message("1", "旧消息" * 100),
            _message("2", "最新消息"),
        ),
        max_bytes=512,
    )
    assert "最新消息" in parts.batch_text
    assert parts.omitted_messages >= 1
    assert parts.input_bytes <= 512


def test_segment_marker只展示一次():
    """上下文同时保留 segment marker，但不能重复消耗输入预算。"""
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1",
        user_id="123",
        user_name="小明",
        text="[图片]",
        metadata={"onebot11_markers": ["image", "reply"]},
        message_key="group:1",
    )
    parts = build_agent_context_parts("", (message,), max_bytes=4096)
    assert parts.batch_text.count("segments=image reply") == 1
