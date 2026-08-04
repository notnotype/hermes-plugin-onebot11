"""消息段解析测试：array 格式 → 纯文本 / 图片 / @ 提取。"""

import pytest

from onebot11.message import ParsedMessage, parse_message_segments


def test_纯文本消息():
    """只有 text 段时,提取出纯文本。"""
    result = parse_message_segments([{"type": "text", "data": {"text": "你好"}}])
    assert result.text == "你好"
    assert result.images == []
    assert result.mentioned_qq == []
    assert not result.mentioned_self


def test_文本加图片():
    """text + image 段：文本保留,图片 file 进 images。"""
    result = parse_message_segments(
        [
            {"type": "text", "data": {"text": "看这张图 "}},
            {"type": "image", "data": {"file": "abc.jpg"}},
        ]
    )
    assert result.text == "看这张图 "
    assert result.images == ["abc.jpg"]


def test_at自己标记mentioned_self():
    """at 段命中 self_id 时,mentioned_self 为 True,且不进 mentioned_qq。"""
    result = parse_message_segments(
        [
            {"type": "at", "data": {"qq": "3101482118"}},
            {"type": "text", "data": {"text": "在吗"}},
        ],
        self_id="3101482118",
    )
    assert result.mentioned_self
    assert result.mentioned_qq == []


def test_at他人进mentioned_qq():
    """at 段命中他人时,QQ 号进 mentioned_qq,文本不含 at。"""
    result = parse_message_segments(
        [
            {"type": "at", "data": {"qq": "123456789"}},
            {"type": "text", "data": {"text": "你也来"}},
        ],
        self_id="3101482118",
    )
    assert not result.mentioned_self
    assert result.mentioned_qq == ["123456789"]
    # at 段不进入正文
    assert result.text == "你也来"


def test_at全体不误判():
    """at 全体（qq=all）不算 @ 机器人,也不进 mentioned_qq。"""
    result = parse_message_segments(
        [{"type": "at", "data": {"qq": "all"}}, {"type": "text", "data": {"text": "大家"}}],
        self_id="3101482118",
    )
    assert not result.mentioned_self
    assert result.mentioned_qq == []


def test_未知消息段忽略不崩():
    """未知段类型（如 face / reply 段）静默忽略。"""
    result = parse_message_segments(
        [
            {"type": "face", "data": {"id": "1"}},
            {"type": "text", "data": {"text": "ok"}},
        ]
    )
    assert result.text == "ok"


def test_image_用url兜底():
    """image 段没有 file 但有 url 时,取 url。"""
    result = parse_message_segments(
        [{"type": "image", "data": {"url": "https://example.com/a.png"}}]
    )
    assert result.images == ["https://example.com/a.png"]


def test_空段返回空():
    """空消息段列表返回空 ParsedMessage。"""
    result = parse_message_segments([])
    assert result == ParsedMessage(text="", images=[], mentioned_qq=[], mentioned_self=False)


def test_cq字符串格式暂不支持():
    """v1 只支持 array 格式,字符串格式抛 NotImplementedError。"""
    with pytest.raises(NotImplementedError):
        parse_message_segments("你好[CQ:image,file=abc.jpg]")
