"""OneBot11 batch 上下文物化测试。"""

from onebot11.context import (
    build_agent_context,
    build_dynamic_context,
    build_queue_batch_summary,
)
from onebot11.queue import QueueMessage


def _message(seq: int, text: str, raw_text: str = "") -> QueueMessage:
    """构造带稳定序号的测试消息。"""
    return QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id=str(seq),
        user_id=str(seq),
        user_name=f"用户{seq}",
        text=text,
        raw_text=raw_text or text,
        message_key=f"group:{seq}",
        seq=seq,
    )


def test_batch摘要只包含当前批次较早消息():
    """已在 session 历史中的上一批摘要不会由构造器再次加入。"""
    messages = tuple(_message(index, f"消息-{index}") for index in range(1, 5))
    result = build_agent_context("", messages, max_bytes=4096, recent_originals=2)
    assert "消息-1" in result
    assert "消息-2" in result
    assert "消息-3" in result
    assert "消息-4" in result
    assert result.count("消息-1") == 1
    assert "[原文:" not in result


def test_recent消息保留规范化正文和原文():
    """最近消息保留 raw segment，早期摘要不重复保存 raw。"""
    messages = (
        _message(1, "早期正文", "[CQ:image,file=old]"),
        _message(2, "最近正文", "[CQ:image,file=recent]"),
    )
    result = build_agent_context("", messages, max_bytes=4096, recent_originals=1)
    assert "[CQ:image,file=recent]" in result
    assert "[CQ:image,file=old]" not in result


def test_context按UTF8字节预算裁剪():
    """中英文混合内容裁剪后仍不超过预算且不产生乱码。"""
    messages = tuple(_message(index, "很长的消息" * 300) for index in range(1, 4))
    result = build_agent_context("", messages, max_bytes=1024, recent_originals=1)
    assert len(result.encode("utf-8")) <= 1024
    assert "\ufffd" not in result
    assert build_queue_batch_summary(messages, max_bytes=512).encode("utf-8")


def test_dynamic_context是请求级文本():
    """动态上下文带有明确的非 transcript 合同。"""
    result = build_dynamic_context({"当前时间": "2026-08-06", "目标": "group:888"})
    assert "不写入会话历史" in result
    assert "2026-08-06" in result
