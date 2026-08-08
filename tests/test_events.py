"""OneBot 11 事件 → 内部归一化事件测试。

events.py 返回自定义 InboundEvent（零 Hermes 依赖），由 adapter 层再转换为
Hermes 的 MessageEvent。群聊 chat_id=group_id、私聊 chat_id=user_id。
"""

from onebot11.events import build_inbound_event, normalize_auxiliary_event


def test_私聊消息事件():
    raw = {
        "post_type": "message",
        "message_type": "private",
        "message_id": 1001,
        "user_id": 123456789,
        "message": [{"type": "text", "data": {"text": "你好"}}],
        "sender": {"nickname": "小明"},
    }
    event = build_inbound_event(raw, self_id="3101482118")
    assert event is not None
    assert event.text == "你好"
    assert event.chat_id == "123456789"  # 私聊 chat_id = user_id
    assert event.chat_type == "dm"
    assert event.user_id == "123456789"
    assert event.user_name == "小明"
    assert event.message_id == "1001"


def test_群聊消息事件():
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 2002,
        "group_id": 88888888,
        "user_id": 123456789,
        "message": [{"type": "text", "data": {"text": "大家好啊"}}],
        "sender": {"card": "群昵称", "nickname": "真名"},
    }
    event = build_inbound_event(raw, self_id="3101482118")
    assert event is not None
    assert event.chat_id == "88888888"  # 群聊 chat_id = group_id
    assert event.chat_type == "group"
    assert event.user_id == "123456789"
    # 群昵称(card)优先, 其次 nickname
    assert event.user_name == "群昵称"


def test_群聊无card用nickname():
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 2003,
        "group_id": 88888888,
        "user_id": 123456789,
        "message": [{"type": "text", "data": {"text": "hi"}}],
        "sender": {"nickname": "真名"},
    }
    event = build_inbound_event(raw, self_id="3101482118")
    assert event.user_name == "真名"


def test_图片进images():
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 2004,
        "group_id": 88888888,
        "user_id": 123456789,
        "message": [
            {"type": "text", "data": {"text": "看"}},
            {"type": "image", "data": {"file": "pic.jpg"}},
        ],
        "sender": {"nickname": "小明"},
    }
    event = build_inbound_event(raw, self_id="3101482118")
    assert event.images == ["pic.jpg"]


def test_at自己标记():
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 2005,
        "group_id": 88888888,
        "user_id": 123456789,
        "message": [{"type": "at", "data": {"qq": "3101482118"}}],
        "sender": {"nickname": "小明"},
    }
    event = build_inbound_event(raw, self_id="3101482118")
    assert event.mentioned_self


def test_心跳事件返回None():
    """meta_event（heartbeat）不进会话,返回 None。"""
    raw = {"post_type": "meta_event", "meta_event_type": "heartbeat", "time": 1700000000}
    assert build_inbound_event(raw, self_id="3101482118") is None


def test_通知事件返回None():
    """notice 事件 v1 不处理,返回 None。"""
    raw = {
        "post_type": "notice",
        "notice_type": "group_decrease",
        "group_id": 88888888,
        "user_id": 123456789,
    }
    assert build_inbound_event(raw, self_id="3101482118") is None


def test_缺少message字段返回None():
    raw = {"post_type": "message", "message_type": "private", "message_id": 1, "user_id": 2}
    assert build_inbound_event(raw, self_id="3101482118") is None


def test_缺少message_id时忽略时间戳生成稳定去重键():
    """同一重放事件的时间字段变化不能造成第二条队列消息。"""
    first = {
        "post_type": "message",
        "message_type": "group",
        "group_id": 888,
        "user_id": 123,
        "time": 100,
        "message": [{"type": "text", "data": {"text": "同一条"}}],
    }
    replay = {**first, "time": 101}
    first_event = build_inbound_event(first, self_id="1")
    replay_event = build_inbound_event(replay, self_id="1")
    assert first_event is not None and replay_event is not None
    assert first_event.message_id == replay_event.message_id == ""
    assert first_event.message_key == replay_event.message_key
    assert first_event.message_key.startswith("hash:")


def test_保留CQ原文供队列最近原文使用():
    """CQ raw_message 进入受限原文字段，不依赖完整 raw payload。"""
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 2006,
        "group_id": 888,
        "user_id": 123,
        "raw_message": "[CQ:face,id=1]你好",
        "message": "[CQ:face,id=1]你好",
        "sender": {"nickname": "小明"},
    }
    event = build_inbound_event(raw, self_id="1")
    assert event is not None
    assert event.text == "你好"
    assert event.raw_text == "[CQ:face,id=1]你好"


def test_notice摘要按UTF8字节限制():
    """辅助事件摘要的限制按存储字节而不是 Python 字符数计算。"""
    event = normalize_auxiliary_event(
        {"post_type": "notice", "notice_type": "group_upload", "group_id": 888, "time": "啊" * 1000},
        limit=64,
    )
    assert event is not None
    assert len(event.summary.encode("utf-8")) <= 64
