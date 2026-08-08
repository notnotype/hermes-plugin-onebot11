"""adapter.py 冒烟测试。

需要 hermes gateway 可导入（本地跑：用 hermes venv + PYTHONPATH）；
CI 环境没有 gateway 时自动跳过。
"""

import asyncio
import json
import os
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("gateway.platforms.base")

from aiohttp import web  # noqa: E402

# 镜像真实网关流程: register() 之前把平台注册进 registry,Platform("onebot11") 才能解析
from gateway.platform_registry import PlatformEntry, platform_registry  # noqa: E402

platform_registry.register(
    PlatformEntry(
        name="onebot11",
        label="OneBot 11 (QQ)",
        adapter_factory=lambda cfg: None,
        check_fn=lambda: True,
        source="plugin",
    )
)

from gateway.config import PlatformConfig  # noqa: E402
from gateway.platforms.base import BasePlatformAdapter, ProcessingOutcome, SendResult  # noqa: E402

import adapter as adapter_module  # noqa: E402
from adapter import OneBot11Adapter, check_requirements, register, validate_config  # noqa: E402
from onebot11.events import InboundEvent  # noqa: E402


def _make_adapter(monkeypatch, **env) -> OneBot11Adapter:
    # 默认用随机端口,避免与正在运行的网关(0.0.0.0:18880)撞端口
    env.setdefault("ONEBOT11_WS_PORT", "0")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return OneBot11Adapter(PlatformConfig(enabled=True, extra={}))


async def _async_result(value):
    """把同步测试值包装成可 await 的结果。"""
    return value


@pytest.fixture
async def fake_http_server():
    """记录 OneBot HTTP 请求的假服务。"""
    calls: list[dict] = []

    async def handler(request: web.Request) -> web.Response:
        body = await request.json()
        calls.append({"path": request.path, "params": body})
        return web.json_response({"status": "ok", "retcode": 0, "data": {"message_id": 7}})

    app = web.Application()
    app.router.add_post("/{action}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base: str = f"http://127.0.0.1:{runner.addresses[0][1]}"
    yield base, calls
    await runner.cleanup()


def test_继承自BasePlatformAdapter(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    assert isinstance(adapter, BasePlatformAdapter)
    assert adapter.platform.value == "onebot11"


async def test_连接生命周期(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    assert not adapter.is_connected
    await adapter.connect()
    assert adapter.is_connected
    await adapter.disconnect()
    assert not adapter.is_connected


async def test_缺HTTP配置时connect失败(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_SELF_ID="1")  # 没有 ONEBOT11_HTTP_API
    assert not await adapter.connect()
    await adapter.disconnect()


async def test_群聊事件转MessageEvent带前缀(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    event = await adapter._build_message_event(
        InboundEvent(
            text="大家好",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="小明",
            message_id="1001",
        )
    )
    assert event.text == "[小明] 大家好"
    assert event.source.chat_id == "888"
    assert event.source.chat_type == "group"
    assert event.source.user_id == "123"
    assert event.message_id == "1001"
    assert event.metadata["mentioned_self"] is False
    assert event.source.role_authorized is True


async def test_私聊事件不加前缀(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    event = await adapter._build_message_event(
        InboundEvent(text="在吗", chat_id="123", chat_type="dm", user_id="123", user_name="小明", message_id="1")
    )
    assert event.text == "在吗"
    assert event.source.chat_type == "dm"


async def test_真实工具handler可从当前binding补齐缺失turn_id(monkeypatch):
    """Hermes registry 未传 turn_id 时，handler 仍使用当前 task 的精确 binding。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    event = await adapter._build_message_event(
        InboundEvent(
            text="查询",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="小明",
            message_id="1001",
        )
    )
    caller = adapter._caller_for_event(event.source)
    event.metadata["onebot11_caller_context"] = adapter_module._serializable_caller(caller)

    async def fake_group_info(api, params, ctx):
        del api, params
        return {"status": "ok", "chat_id": ctx.chat_id, "user_id": ctx.user_id}

    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    monkeypatch.setitem(adapter_module._TOOL_HANDLERS, "qq_get_group_info", fake_group_info)
    adapter_module._pre_gateway_dispatch_hook(event)
    hook_result = adapter_module._pre_llm_call_hook(
        session_id="session-1", turn_id="turn-1", platform="onebot11"
    )
    assert hook_result is not None
    assert "角色目录" in hook_result["context"]
    assert "user ->" in hook_result["context"]
    assert "trusted_user ->" in hook_result["context"]
    assert "super_admin ->" in hook_result["context"]

    try:
        result = json.loads(
            await adapter._make_tool_handler("qq_get_group_info")({}, session_id="session-1")
        )
        assert result == {"status": "ok", "chat_id": "888", "user_id": "123"}
        denied = json.loads(
            await adapter._make_tool_handler("qq_get_group_info")({}, session_id="other-session")
        )
        assert denied["status"] == "permission_error"
        assert adapter_module._pre_tool_call_hook(
            tool_name="qq_get_group_info", session_id="session-1", turn_id="turn-1", args={}
        ) is None
        blocked = adapter_module._pre_tool_call_hook(
            tool_name="qq_get_group_info", session_id="session-1", turn_id="other-turn", args={}
        )
        assert blocked is not None
        assert blocked["action"] == "block"
    finally:
        adapter_module._CURRENT_BINDING.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        await adapter.disconnect()


def test_pre_llm非OneBot平台不注入拒绝提示(monkeypatch):
    """其他平台的 turn 不应被 OneBot hook 改写。"""
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: object())
    adapter_module._CURRENT_CALLER.set(None)
    adapter_module._CURRENT_BINDING.set(None)
    assert adapter_module._pre_llm_call_hook(session_id="cli", turn_id="turn", platform="cli") is None
    assert adapter_module._pre_llm_call_hook(session_id="cli", turn_id="turn", platform="") is None


def test_pre_llm明确OneBot但缺少caller时fail_closed(monkeypatch):
    """明确标记为 OneBot 的 turn 缺身份时必须拒绝工具上下文。"""
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: object())
    adapter_module._CURRENT_CALLER.set(None)
    adapter_module._CURRENT_BINDING.set(None)
    result = adapter_module._pre_llm_call_hook(
        session_id="onebot-session", turn_id="onebot-turn", platform="onebot11"
    )
    assert result == {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}


def test_Hermes真实session_key群共享且私聊隔离():
    """真实 Hermes session key 合同应与 OneBot adapter 的 shared 配置一致。"""
    from gateway.config import Platform
    from gateway.session import SessionSource, build_session_key

    alice = SessionSource(
        platform=Platform("onebot11"), chat_id="888", chat_type="group", user_id="100"
    )
    bob = SessionSource(
        platform=Platform("onebot11"), chat_id="888", chat_type="group", user_id="200"
    )
    assert build_session_key(alice, group_sessions_per_user=False) == build_session_key(
        bob, group_sessions_per_user=False
    )

    first_dm = SessionSource(
        platform=Platform("onebot11"), chat_id="100", chat_type="dm", user_id="100"
    )
    second_dm = SessionSource(
        platform=Platform("onebot11"), chat_id="200", chat_type="dm", user_id="200"
    )
    assert build_session_key(first_dm, group_sessions_per_user=False) != build_session_key(
        second_dm, group_sessions_per_user=False
    )


async def test_媒体孤儿目录跨重启按TTL清理(monkeypatch, tmp_path):
    """旧 Hermes home 中的 turn 媒体目录应由下一次 adapter 启动清理。"""
    hermes_home = tmp_path / "hermes-home"
    queue_db = tmp_path / "queue.sqlite3"
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("ONEBOT11_QUEUE_DB", str(queue_db))
    first = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    orphan = Path(first._media_dir)
    (orphan / "download.png").write_bytes(b"stale")
    await first.disconnect()

    stale = time.time() - first._media_orphan_ttl - 1
    os.utime(orphan, (stale, stale))
    second = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    try:
        assert not orphan.exists()
    finally:
        await second.disconnect()


async def test_入站群聊事件先进入持久队列(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    try:
        async def fake_notify(_chat_id: str) -> bool:
            return False

        monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
        raw = {
            "post_type": "message",
            "message_type": "group",
            "message_id": 1001,
            "group_id": 888,
            "user_id": 123,
            "message": [
                {"type": "at", "data": {"qq": "1"}},
                {"type": "text", "data": {"text": "在吗"}},
            ],
            "sender": {"card": "小明", "nickname": "真名"},
        }
        await adapter._on_ws_event(raw)
        messages = adapter._queue.peek("888")
        assert len(messages) == 1
        assert messages[0].text == "在吗"
        assert adapter._queue.status("888")["pending"] == 1
    finally:
        await adapter.disconnect()


async def test_纯图片消息不因空文本崩溃并保留file字段(monkeypatch):
    """纯媒体事件也必须先持久化，不能在命令解析阶段 IndexError。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 9001,
        "group_id": 888,
        "user_id": 123,
        "message": [{"type": "image", "data": {"file": "pic.jpg"}}],
        "sender": {"nickname": "小明"},
    }
    try:
        await adapter._on_ws_event(raw)
        messages = adapter._queue.peek("888")
        assert len(messages) == 1
        assert messages[0].text == ""
        assert messages[0].metadata["onebot11_image_files"] == ["pic.jpg"]
    finally:
        await adapter.disconnect()


async def test_缺失真实message_id使用hash_key且工具返回结构化错误(monkeypatch):
    """没有 message_id 的消息不能伪装成可传给 OneBot 的整数 ID。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    raw = {
        "post_type": "message",
        "message_type": "group",
        "group_id": 888,
        "user_id": 123,
        "message": [{"type": "text", "data": {"text": "没有真实 ID"}}],
        "sender": {"nickname": "小明"},
    }
    try:
        await adapter._on_ws_event(raw)
        messages = adapter._queue.peek("888")
        assert len(messages) == 1
        assert messages[0].message_id == ""
        assert messages[0].message_key.startswith("hash:")
        assert adapter_module._proto.tools._unqueryable_message_id(
            messages[0].message_key
        )["error_code"] == "message_id_unavailable"
    finally:
        await adapter.disconnect()


async def test_get_image本地文件只允许显式媒体根(monkeypatch, tmp_path):
    """OneBot get_image 返回路径时，只复制配置根目录内的真实图片。"""
    source_root = tmp_path / "source"
    source_root.mkdir()
    source = source_root / "image.png"
    source.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00\x00IEND\xaeB`\x82")
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "media_source_roots": [str(source_root)],
            },
        )
    )
    monkeypatch.setattr(adapter._api, "get_image", lambda _file: _async_result({"file": str(source)}))
    try:
        destination = await adapter._download_image("file-id", adapter._media_dir)
        assert destination is not None
        assert Path(destination).read_bytes().startswith(b"\x89PNG")
        outside = tmp_path / "outside.png"
        outside.write_bytes(source.read_bytes())
        monkeypatch.setattr(
            adapter._api,
            "get_image",
            lambda _file: _async_result({"file": str(outside)}),
        )
        assert await adapter._download_image("outside", adapter._media_dir) is None
    finally:
        await adapter.disconnect()


async def test_群turn给触发消息加眼睛并在收尾移除(monkeypatch):
    """处理指示器绑定触发消息，并在 queue ack 前清理。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="-1001",
        user_id="123",
        user_name="小明",
        text="触发",
        message_key="group:-1001",
    )
    second = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="456",
        user_name="小红",
        text="后续上下文",
        message_key="group:1002",
    )
    adapter._queue.enqueue(
        first,
        adapter_module.TriggerRequest.create("888", "group:-1001", "mention", "123", "小明"),
    )
    adapter._queue.enqueue(second)
    reaction_calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        reaction_calls.append((message_id, emoji_id, enabled))

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    try:
        assert await adapter._dispatcher.notify("888")
        active = adapter._dispatcher.active("888")
        assert active is not None
        event = SimpleNamespace(
            metadata={"onebot11_lease_id": active.lease.lease_id},
            media_urls=[],
        )
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert reaction_calls == [
            ("-1001", "128064", True),
            ("-1001", "128064", False),
        ]
        assert adapter._queue.status("888")["pending"] == 1
        assert [message.message_id for message in adapter._queue.peek("888")] == ["1002"]
    finally:
        await adapter.disconnect()


async def test_reaction落盘失败时不调用set_true(monkeypatch):
    """无法持久化清理目标时，不能先向 OneBot 添加 👀。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="触发",
        message_key="group:1001",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create("888", "group:1001", "mention", "123", "小明"),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    calls: list[bool] = []

    def broken_record(*_args, **_kwargs):
        raise OSError("disk full")

    async def fake_reaction(_message_id: str, _emoji_id: str, *, enabled: bool) -> None:
        calls.append(enabled)

    monkeypatch.setattr(adapter._queue, "record_reaction", broken_record)
    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    try:
        assert await adapter._set_processing_reaction(lease, enabled=True) is None
        assert calls == []
    finally:
        await adapter.disconnect()


async def test_queued_reaction添加成功但状态更新失败保留pending(monkeypatch):
    """远端 set 成功后本地状态写失败，不能删除 pending 清理事实。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    calls: list[bool] = []

    async def fake_reaction(_message_id: str, _emoji_id: str, *, enabled: bool) -> None:
        calls.append(enabled)

    def broken_mark(*_args, **_kwargs):
        raise OSError("database busy")

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    monkeypatch.setattr(adapter._queue, "mark_reaction_set", broken_mark)
    try:
        assert await adapter._set_queued_reaction("anchor-queued", "888", "1001")
        assert calls == [True]
        record = adapter._queue.reaction("anchor-queued", reaction_kind="queued")
        assert record is not None
        assert record.state == "pending"
        assert await adapter._set_queued_reaction("anchor-queued", "888", "1001")
        assert calls == [True]
    finally:
        await adapter.disconnect()


async def test_lease失效后processing_reaction不再set(monkeypatch):
    """第二次 fencing 检查失败时，旧 turn 不能产生 👀 出站副作用。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="触发",
        message_key="group:1001",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create("888", "group:1001", "mention", "123", "小明"),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    checks = 0
    calls: list[bool] = []

    def fenced_after_record(_lease) -> bool:
        nonlocal checks
        checks += 1
        return checks == 1

    async def fake_reaction(_message_id: str, _emoji_id: str, *, enabled: bool) -> None:
        calls.append(enabled)

    monkeypatch.setattr(adapter._queue, "is_lease_current", fenced_after_record)
    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    try:
        assert await adapter._set_processing_reaction(lease, enabled=True) is None
        assert calls == []
        assert checks == 2
    finally:
        await adapter.disconnect()


async def test_精确锚点先设置排队符号_claim时切换为眼睛(monkeypatch):
    """⏳ 表示 durable queued，👀 只表示当前正在处理的 anchor。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    async def fake_notify(_chat_id: str) -> bool:
        return False

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    try:
        await adapter._on_ws_event(_group_raw(888, text="帮我查一下", at_self=True))
        anchors = adapter._queue.list_anchors("888")
        assert len(anchors) == 1
        assert calls == [("1", "9203", True)]

        lease = adapter._queue.claim("888")
        assert lease is not None
        monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
        await adapter._start_queue_turn(lease)
        assert calls == [
            ("1", "9203", True),
            ("1", "128064", True),
            ("1", "9203", False),
        ]
    finally:
        await adapter.disconnect()


async def test_processing_reaction失败时保留queued(monkeypatch):
    """处理中 👀 确定失败时，持久化的 queued 状态不能先被清掉。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="触发",
        message_key="group:1001",
    )
    anchor = adapter_module.TriggerRequest.create(
        "888",
        "group:1001",
        "mention",
        "123",
        "小明",
    )
    adapter._queue.enqueue(message, anchor)
    adapter._queue.record_reaction(
        "",
        "888",
        "1001",
        anchor_id=anchor.request_id,
        reaction_kind="queued",
        emoji_id="9203",
    )
    adapter._queue.mark_reaction_set(anchor.request_id, reaction_kind="queued")
    lease = adapter._queue.claim("888")
    assert lease is not None
    calls: list[tuple[str, str, bool]] = []

    async def broken_processing_reaction(
        message_id: str,
        emoji_id: str,
        *,
        enabled: bool,
    ) -> None:
        calls.append((message_id, emoji_id, enabled))
        raise adapter_module.OneBotApiError(
            "set_msg_emoji_like",
            "failed",
            100,
            unknown_outcome=False,
        )

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", broken_processing_reaction)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    try:
        await adapter._start_queue_turn(lease)
        queued = adapter._queue.reaction(anchor.request_id, reaction_kind="queued")
        assert queued is not None
        assert calls == [("1001", "128064", True)]
    finally:
        await adapter.disconnect()


async def test_operator_anchor_reaction优先使用控制消息(monkeypatch):
    """/onebot flush 的 👀 必须跟随管理员命令，而不是被处理消息抢走。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="待处理",
        message_key="group:1001",
    )
    adapter._queue.enqueue(message)
    request_id = adapter._queue.create_operator_anchor(
        "888",
        "admin_flush",
        "999",
        "管理员",
        control_message_id="9001",
    )
    assert request_id is not None
    lease = adapter._queue.claim("888")
    assert lease is not None
    try:
        assert adapter._reaction_message_id(lease) == "9001"
    finally:
        await adapter.disconnect()


async def test_已知失败释放后恢复queued_reaction(monkeypatch):
    """turn 明确失败回 pending 后，UI 应恢复 ⏳ 而不是静默积压。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._processing_reaction_enabled = False
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="触发",
        message_key="group:1001",
    )
    anchor = adapter_module.TriggerRequest.create(
        "888",
        "group:1001",
        "mention",
        "123",
        "小明",
    )
    adapter._queue.enqueue(message, anchor)

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    async def fake_notify(_chat_id: str) -> bool:
        return False

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    assert await adapter._dispatcher.notify("888")
    active = adapter._dispatcher.active("888")
    assert active is not None
    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    event = SimpleNamespace(
        message_id="1001",
        source=SimpleNamespace(chat_id="888"),
        metadata={
            "onebot11_lease_id": active.lease.lease_id,
            "onebot11_anchor_id": anchor.request_id,
            "onebot11_anchor_message_id": "1001",
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
        },
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.FAILURE)
        queued = adapter._queue.reaction(anchor.request_id, reaction_kind="queued")
        assert queued is not None
        assert calls == [("1001", "9203", True)]
    finally:
        await adapter.disconnect()


async def test_completion先清理旧reaction再通知下一anchor(monkeypatch):
    """下一 anchor 必须在旧 turn 的 👀 清理尝试之后才启动。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    order: list[str] = []

    monkeypatch.setattr(
        adapter._queue,
        "status_for_lease",
        lambda _lease_id: {"lease_phase": "agent_running", "outbound_started": False},
    )

    async def fake_complete(
        _lease_id: str,
        *,
        outcome: str,
        unknown: bool,
        known_failure: bool = False,
        reason: str | None = None,
    ) -> bool:
        del outcome, unknown, known_failure, reason
        return True

    async def fake_clear(_lease_id: str) -> None:
        order.append("clear")

    async def fake_notify(_chat_id: str) -> bool:
        order.append("notify")
        return True

    monkeypatch.setattr(adapter._dispatcher, "complete", fake_complete)
    monkeypatch.setattr(adapter, "_clear_processing_reaction", fake_clear)
    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    event = SimpleNamespace(
        source=SimpleNamespace(chat_id="888"),
        metadata={
            "onebot11_lease_id": "lease-order",
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
        },
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert order == ["clear", "notify"]
    finally:
        await adapter.disconnect()


async def test_reaction_unset成功但删除状态失败使用有限退避(monkeypatch):
    """远端 unset 成功而本地删除失败时，不能无限立即重复 unset。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._queue.record_reaction("lease-reaction", "888", "1001")
    adapter._queue.mark_reaction_set("lease-reaction")

    async def fake_reaction(_message_id: str, _emoji_id: str, *, enabled: bool) -> None:
        assert enabled is False

    def broken_delete(_lease_id: str) -> bool:
        raise OSError("state delete failed")

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    monkeypatch.setattr(adapter._queue, "delete_reaction", broken_delete)
    try:
        await adapter._unset_reaction(adapter._queue.reaction("lease-reaction"))
        record = adapter._queue.reaction("lease-reaction")
        assert record is not None
        assert record.attempts == 1
        assert record.next_attempt_at is not None
    finally:
        await adapter.disconnect()


async def test_llm_false的同一批消息只判断一次(monkeypatch):
    """持久游标阻止恢复轮询对相同 pending 批次反复调用旁路模型。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "sidecar",
                    "model": "judge",
                    "groups": ["888"],
                },
            },
        )
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="llm-false",
        user_id="123",
        user_name="小明",
        text="这条需要机器人回复吗？",
        message_key="group:llm-false",
    )
    adapter._queue.enqueue(message)
    calls = 0

    async def fake_call(**_kwargs):
        nonlocal calls
        calls += 1
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content='{"anchor_seq":null,"reason_code":"no_request"}'
                    )
                )
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "agent.auxiliary_client",
        SimpleNamespace(async_call_llm=fake_call),
    )
    try:
        await adapter._judge_llm_trigger("888")
        await adapter._judge_llm_trigger("888")
        assert calls == 1
        assert adapter._queue.status("888")["llm_judged_seq"] == 1
    finally:
        await adapter.disconnect()


async def test_llm_selector推进到实际观察游标并等待新消息(monkeypatch):
    """选择第一条后，同批未选消息不重复判断；新消息才开启下一批判断。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "require_mention": False,
                "llm_trigger": {
                    "enabled": True,
                    "provider": "sidecar",
                    "model": "judge",
                    "groups": ["888"],
                },
            },
        )
    )
    for index in (1, 2):
        adapter._queue.enqueue(
            adapter_module.QueueMessage(
                chat_id="888",
                chat_type="group",
                message_id=f"llm-anchor-{index}",
                user_id=str(100 + index),
                user_name=f"用户{index}",
                text=f"机器人，请处理任务{index}",
                message_key=f"group:llm-anchor-{index}",
            )
        )
    prompts: list[str] = []

    async def fake_call(**kwargs):
        prompts.append(str(kwargs["messages"][1]["content"]))
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=(
                            '{"anchor_seq":1,"reason_code":"automatic_request"}'
                            if len(prompts) == 1
                            else '{"anchor_seq":null,"reason_code":"no_request"}'
                        )
                    )
                )
            ]
        )

    async def no_reaction(*_args, **_kwargs):
        return None

    async def no_notify(_chat_id: str) -> bool:
        return False

    scheduled: list[str] = []

    monkeypatch.setitem(
        sys.modules,
        "agent.auxiliary_client",
        SimpleNamespace(async_call_llm=fake_call),
    )
    monkeypatch.setattr(adapter, "_set_queued_reaction", no_reaction)
    monkeypatch.setattr(adapter._dispatcher, "notify", no_notify)
    monkeypatch.setattr(
        adapter,
        "_schedule_llm_trigger",
        lambda chat_id, **_kwargs: scheduled.append(str(chat_id)),
    )
    try:
        await adapter._judge_llm_trigger("888")
        assert adapter._queue.status("888")["llm_judged_seq"] == 2
        assert scheduled == []
        await adapter._judge_llm_trigger("888")
        assert len(prompts) == 1
        assert '"seq":1' in prompts[0]
        assert '"seq":2' in prompts[0]

        adapter._queue.enqueue(
            adapter_module.QueueMessage(
                chat_id="888",
                chat_type="group",
                message_id="llm-anchor-3",
                user_id="103",
                user_name="用户3",
                text="机器人，请处理任务3",
                message_key="group:llm-anchor-3",
            )
        )
        await adapter._judge_llm_trigger("888")
        assert len(prompts) == 2
        assert '"seq":3' in prompts[1]
        assert '"seq":1' not in prompts[1]
        assert '"seq":2' not in prompts[1]
        assert adapter._queue.status("888")["llm_judged_seq"] == 3
    finally:
        await adapter.disconnect()


async def test_llm_selector大消息截断后续消息不会积压(monkeypatch):
    """prompt 只覆盖前一条时，后续 seq 不能被错误标记为已判断。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "require_mention": False,
                "llm_trigger": {
                    "enabled": True,
                    "provider": "sidecar",
                    "model": "judge",
                    "groups": ["888"],
                },
            },
        )
    )
    for index, text in (
        (1, "第一条很长的消息" * 5000),
        (2, "第二条消息"),
    ):
        adapter._queue.enqueue(
            adapter_module.QueueMessage(
                chat_id="888",
                chat_type="group",
                message_id=f"llm-truncate-{index}",
                user_id=str(100 + index),
                user_name=f"用户{index}",
                text=text,
                message_key=f"group:llm-truncate-{index}",
            )
        )
    prompts: list[str] = []

    async def fake_call(**kwargs):
        prompts.append(str(kwargs["messages"][1]["content"]))
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content='{"anchor_seq":null,"reason_code":"no_request"}'
                    )
                )
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "agent.auxiliary_client",
        SimpleNamespace(async_call_llm=fake_call),
    )
    monkeypatch.setattr(adapter, "_schedule_llm_trigger", lambda *_args, **_kwargs: None)
    try:
        await adapter._judge_llm_trigger("888")
        assert adapter._queue.status("888")["llm_judged_seq"] == 1
        await adapter._judge_llm_trigger("888")
        assert len(prompts) == 2
        assert '"seq":2' in prompts[1]
        assert '"seq":1' not in prompts[1]
        assert adapter._queue.status("888")["llm_judged_seq"] == 2
    finally:
        await adapter.disconnect()


async def test_llm_trigger失败使用持久退避(monkeypatch):
    """旁路模型失败不会被恢复轮询快速打爆，退避状态跨读取保留。"""
    now = [1000.0]
    monkeypatch.setattr(adapter_module.time, "time", lambda: now[0])
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "sidecar",
                    "model": "judge",
                    "groups": ["888"],
                },
            },
        )
    )
    adapter._queue.enqueue(
        adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="llm-error",
            user_id="123",
            user_name="小明",
            text="机器人能否处理这条消息？",
            message_key="group:llm-error",
        )
    )
    calls = 0

    async def broken_call(**_kwargs):
        nonlocal calls
        calls += 1
        raise TimeoutError("judge timeout")

    monkeypatch.setitem(
        sys.modules,
        "agent.auxiliary_client",
        SimpleNamespace(async_call_llm=broken_call),
    )
    try:
        await adapter._judge_llm_trigger("888")
        await adapter._judge_llm_trigger("888")
        assert calls == 1
        assert adapter._queue.status("888")["llm_judged_seq"] == 0
        assert adapter._queue.status("888")["llm_failure_count"] == 1
        now[0] = 1002.1
        await adapter._judge_llm_trigger("888")
        assert calls == 2
    finally:
        await adapter.disconnect()


async def test_llm_trigger失败期间新增消息会清除失败退避(monkeypatch):
    """失败状态落盘后，新消息不能被旧 selector 退避压住。"""
    now = [1000.0]
    monkeypatch.setattr(adapter_module.time, "time", lambda: now[0])
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "require_mention": False,
                "llm_trigger": {
                    "enabled": True,
                    "provider": "sidecar",
                    "model": "judge",
                    "groups": ["888"],
                },
            },
        )
    )
    adapter._queue.enqueue(
        adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="llm-error-first",
            user_id="123",
            user_name="小明",
            text="机器人能否处理第一条？",
            message_key="group:llm-error-first",
        )
    )
    scheduled: list[str] = []
    calls = 0

    async def broken_call(**_kwargs):
        nonlocal calls
        calls += 1
        adapter._queue.enqueue(
            adapter_module.QueueMessage(
                chat_id="888",
                chat_type="group",
                message_id="llm-error-second",
                user_id="456",
                user_name="小红",
                text="机器人能否处理第二条？",
                message_key="group:llm-error-second",
            )
        )
        raise TimeoutError("judge timeout")

    monkeypatch.setitem(
        sys.modules,
        "agent.auxiliary_client",
        SimpleNamespace(async_call_llm=broken_call),
    )
    monkeypatch.setattr(
        adapter,
        "_schedule_llm_trigger",
        lambda chat_id, **_kwargs: scheduled.append(str(chat_id)),
    )
    try:
        await adapter._judge_llm_trigger("888")
        state = adapter._queue.status("888")
        assert calls == 1
        assert scheduled == ["888"]
        assert state["llm_failure_count"] == 0
        assert state["llm_next_attempt_at"] is None
        assert state["llm_judged_seq"] == 0
    finally:
        await adapter.disconnect()


async def test_新消息会取消selector延迟等待并立即唤醒(monkeypatch):
    """新消息到达时，cooldown/失败退避不能阻止 selector 立即重新评估。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter.trigger_config = replace(
        adapter.trigger_config,
        llm_enabled=True,
        llm_provider="sidecar",
        llm_model="judge",
        llm_allowed_groups=frozenset({"888"}),
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_judge(chat_id: str) -> None:
        assert chat_id == "888"
        started.set()
        await release.wait()

    monkeypatch.setattr(adapter, "_judge_llm_trigger", fake_judge)
    try:
        adapter._schedule_llm_trigger_after("888", 60)
        await asyncio.sleep(0)
        delayed = adapter._llm_trigger_delayed_tasks["888"]
        adapter._schedule_llm_trigger("888", wake=True)
        await asyncio.wait_for(started.wait(), timeout=1)
        assert delayed.cancelled() or delayed.done()
        assert "888" not in adapter._llm_trigger_delayed_tasks
        release.set()
        active = adapter._llm_trigger_tasks.get("888")
        if active is not None:
            await asyncio.wait_for(active, timeout=1)
    finally:
        release.set()
        await adapter.disconnect()


async def test_require_mention兼容模式会评估普通陈述(monkeypatch):
    """兼容模式不能被问题启发式静默跳过，但 authority 仍由 selector 选择。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "require_mention": False,
                "llm_trigger": {
                    "enabled": True,
                    "provider": "sidecar",
                    "model": "judge",
                    "groups": ["888"],
                },
            },
        )
    )
    adapter._queue.enqueue(
        adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="ordinary",
            user_id="123",
            user_name="小明",
            text="帮我留意一下这个事情",
            message_key="group:ordinary",
        )
    )
    calls = 0

    async def fake_call(**_kwargs):
        nonlocal calls
        calls += 1
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content='{"anchor_seq":null,"reason_code":"no_request"}'
                    )
                )
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "agent.auxiliary_client",
        SimpleNamespace(async_call_llm=fake_call),
    )
    try:
        await adapter._judge_llm_trigger("888")
        assert calls == 1
        assert adapter._queue.status("888")["pending_trigger_requests"] == 0
    finally:
        await adapter.disconnect()


async def test_未授权群恢复不修改持久触发状态(monkeypatch):
    """恢复路径在白名单过滤后不能为旧群创建 cooldown trigger。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ALLOWED_GROUPS="888",
    )
    message = adapter_module.QueueMessage(
        chat_id="777",
        chat_type="group",
        message_id="old",
        user_id="123",
        user_name="小明",
        text="旧消息",
        message_key="group:old",
    )
    adapter._queue.enqueue(message)
    try:
        assert await adapter._dispatcher.recover() == []
        status = adapter._queue.status("777")
        assert status["pending"] == 1
        assert status["pending_trigger_requests"] == 0
    finally:
        await adapter.disconnect()


async def test_adapter关闭后工具入口fail_closed(monkeypatch):
    """disconnect 一开始 fencing 后，旧 turn 不能再调用工具。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    adapter_module._CURRENT_CALLER.set(
        adapter_module.CallerContext(user_id="123", chat_type="group", chat_id="888")
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    await adapter.disconnect()
    try:
        hook_result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="session",
            turn_id="turn",
            args={},
        )
        handler_result = json.loads(
            await adapter._make_tool_handler("qq_get_group_info")(
                {}, session_id="session", turn_id="turn"
            )
        )
        assert hook_result is not None and hook_result["action"] == "block"
        assert handler_result["status"] == "permission_error"
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)


async def test_adapter关闭等待已进入的to_thread_queue操作(monkeypatch):
    """disconnect 不能先关 SQLite 再让已进入线程继续访问。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    entered = threading.Event()
    release = threading.Event()
    original_now = adapter._queue._now

    def slow_now():
        entered.set()
        release.wait(timeout=2)
        return original_now()

    monkeypatch.setattr(adapter._queue, "_now", slow_now)
    operation = asyncio.create_task(asyncio.to_thread(adapter._queue.peek, "888"))
    assert await asyncio.to_thread(entered.wait, 1)
    disconnect = asyncio.create_task(adapter.disconnect())
    await asyncio.sleep(0.05)
    assert not disconnect.done()
    release.set()
    assert await operation == ()
    await disconnect


def test_image同时含file和url只选择一个下载源():
    """诊断字段保留两者，但实际 images 列表只保留 URL。"""
    from onebot11.message import parse_message_segments

    result = parse_message_segments(
        [{"type": "image", "data": {"file": "file-id", "url": "https://example.com/a.png"}}]
    )
    assert result.images == ["https://example.com/a.png"]
    assert result.image_urls == ["https://example.com/a.png"]
    assert result.image_files == ["file-id"]


async def test_私聊策略disabled不进入(monkeypatch):
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_DM_POLICY="disabled",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    raw = {
        "post_type": "message",
        "message_type": "private",
        "message_id": 1,
        "user_id": 123,
        "message": [{"type": "text", "data": {"text": "hi"}}],
        "sender": {"nickname": "小明"},
    }
    await adapter._on_ws_event(raw)
    assert recorded == []
    await adapter.disconnect()


async def test_私聊策略allowlist白名单外拒绝(monkeypatch):
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_DM_POLICY="allowlist",
        ONEBOT11_ALLOWED_USERS="999",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    raw = {
        "post_type": "message",
        "message_type": "private",
        "message_id": 1,
        "user_id": 123,
        "message": [{"type": "text", "data": {"text": "hi"}}],
        "sender": {"nickname": "小明"},
    }
    await adapter._on_ws_event(raw)
    assert recorded == []
    await adapter.disconnect()


def _group_raw(group_id: int, text: str = "在吗", at_self: bool = True) -> dict:
    """构造群消息事件;at_self=True 时附带 @ 机器人段（测试用 SELF_ID=1）。"""
    message: list[dict] = []
    if at_self:
        message.append({"type": "at", "data": {"qq": "1"}})
    message.append({"type": "text", "data": {"text": text}})
    return {
        "post_type": "message",
        "message_type": "group",
        "message_id": 1,
        "group_id": group_id,
        "user_id": 123,
        "message": message,
        "sender": {"card": "小明", "nickname": "真名"},
    }


async def test_群聊默认需要at才响应(monkeypatch):
    """require_mention 默认开启: 未 @ 机器人仍入队但不创建 trigger。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    await adapter._on_ws_event(_group_raw(888, at_self=False))
    status = adapter._queue.status("888")
    assert status["pending"] == 1
    assert status["trigger_requests"] == 0
    await adapter.disconnect()


async def test_群slash只读命令旁路且不入队(monkeypatch):
    """群 slash command 在队列前消费，不创建共享 session 输入。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    responses: list[str] = []

    async def fake_send_direct(_event, text: str) -> None:
        responses.append(text)

    monkeypatch.setattr(adapter, "_send_direct", fake_send_direct)
    await adapter._on_ws_event(_group_raw(888, text="/status", at_self=False))
    assert adapter._queue.peek("888") == ()
    assert responses and '"chat_id": "888"' in responses[0]
    assert '"chat_type": "group"' in responses[0]
    assert '"summary"' not in responses[0]
    await adapter.disconnect()


async def test_群危险slash明确拒绝且不入队(monkeypatch):
    """会话重置、模型和压缩命令不交给群 Agent。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    responses: list[str] = []

    async def fake_send_direct(_event, text: str) -> None:
        responses.append(text)

    monkeypatch.setattr(adapter, "_send_direct", fake_send_direct)
    await adapter._on_ws_event(_group_raw(888, text="/reset", at_self=False))
    assert adapter._queue.peek("888") == ()
    assert responses and "不会进入 Agent session" in responses[0]
    await adapter.disconnect()


async def test_相似onebot前缀消息不会被命令吞掉(monkeypatch):
    """只有独立的 /onebot token 才是管理命令。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    notified: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notified.append(chat_id)
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    await adapter._on_ws_event(_group_raw(888, text="/onebotfoo", at_self=True))
    assert notified == ["888"]
    assert adapter._queue.peek("888")
    await adapter.disconnect()


async def test_权限配置工具只写roles子树(monkeypatch, tmp_path):
    """管理员配置工具不能覆盖 token、白名单或其他 Hermes 配置。"""
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "model:\n  provider: demo\nplatforms:\n  onebot11:\n    extra:\n      access_token: keep-me\n      roles:\n        user:\n          tools: [qq_get_message]\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="100",
    )
    result = adapter._save_permission_change(
        "onebot_set_role_tools",
        {"role": "trusted_user", "tools": ["web_search", "browser_navigate"]},
    )
    assert result["status"] == "ok"
    from hermes_cli.config import read_user_config_raw

    saved = read_user_config_raw()
    assert saved["model"]["provider"] == "demo"
    assert saved["platforms"]["onebot11"]["extra"]["access_token"] == "keep-me"
    assert saved["platforms"]["onebot11"]["extra"]["roles"]["trusted_user"]["tools"] == [
        "browser_navigate",
        "web_search",
    ]
    assert adapter.trusted_users == set()
    await adapter.disconnect()


async def test_群聊at机器人放行(monkeypatch):
    """@ 了机器人的群消息正常进入会话。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    notified: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notified.append(chat_id)
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    await adapter._on_ws_event(_group_raw(888, at_self=True))
    assert notified == ["888"]
    assert adapter._queue.status("888")["trigger_requests"] == 1
    await adapter.disconnect()


async def test_关闭require_mention后无at也放行(monkeypatch):
    """兼容模式不再把任意发送者直接当 authority，只保留消息供自动选择。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_REQUIRE_MENTION="false",
    )
    async def fake_notify(chat_id: str) -> bool:
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    await adapter._on_ws_event(_group_raw(888, at_self=False))
    status = adapter._queue.status("888")
    assert status["pending"] == 1
    assert status["trigger_requests"] == 0
    await adapter.disconnect()


async def test_require_mention不影响私聊(monkeypatch):
    """私聊消息不受 require_mention 限制。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ALLOW_ALL_USERS="true",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    raw = {
        "post_type": "message",
        "message_type": "private",
        "message_id": 1,
        "user_id": 123,
        "message": [{"type": "text", "data": {"text": "hi"}}],
        "sender": {"nickname": "小明"},
    }
    await adapter._on_ws_event(raw)
    assert len(recorded) == 1
    assert recorded[0].source.role_authorized is True
    await adapter.disconnect()


async def test_LLM判断期间新增消息会再次判断(monkeypatch):
    """旁路模型判断期间到达的新消息不能静默留在 pending。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "sidecar",
                    "model": "judge",
                    "groups": ["888"],
                },
            },
        )
    )
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1",
        user_id="123",
        user_name="小明",
        text="第一条需要机器人回答吗？",
        message_key="group:1",
    )
    second = replace(first, message_id="2", message_key="group:2", text="第二条也需要回答吗？")
    adapter._queue.enqueue(first)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fake_call(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            content='{"anchor_seq":null,"reason_code":"no_request"}'
                        )
                    )
                ]
            )

    monkeypatch.setitem(
        sys.modules,
        "agent.auxiliary_client",
        SimpleNamespace(async_call_llm=fake_call),
    )
    try:
        task = asyncio.create_task(adapter._judge_llm_trigger("888"))
        await asyncio.wait_for(started.wait(), timeout=1)
        adapter._queue.enqueue(second)
        release.set()
        await asyncio.wait_for(task, timeout=1)
        for _ in range(20):
            retry = adapter._llm_trigger_tasks.get("888")
            if retry is None:
                await asyncio.sleep(0.01)
                continue
            await asyncio.wait_for(retry, timeout=1)
            break
        assert calls == 2
        assert adapter._queue.status("888")["pending_trigger_requests"] == 0
    finally:
        await adapter.disconnect()


async def test_群白名单内群放行(monkeypatch):
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ALLOWED_GROUPS="888,999",
    )
    async def fake_notify(_chat_id: str) -> bool:
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    await adapter._on_ws_event(_group_raw(888))
    assert adapter._queue.status("888")["pending"] == 1
    await adapter.disconnect()


async def test_群白名单外群被过滤(monkeypatch):
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ALLOWED_GROUPS="888,999",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(777))
    assert recorded == []
    await adapter.disconnect()


async def test_群白名单为空不限制(monkeypatch):
    """ONEBOT11_ALLOWED_GROUPS 未设置时,所有群都放行。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    async def fake_notify(_chat_id: str) -> bool:
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    await adapter._on_ws_event(_group_raw(777))
    assert adapter._queue.status("777")["pending"] == 1
    await adapter.disconnect()


async def test_白名单收紧后恢复不会启动旧群lease(monkeypatch):
    """重启恢复必须重新执行当前 allowed_groups，而不是信任旧队列身份。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ALLOWED_GROUPS="888",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1",
        user_id="123",
        user_name="小明",
        text="待恢复",
        message_key="group:1",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create("888", "group:1", "mention", "123", "小明"),
    )
    adapter.allowed_groups = {"999"}
    assert await adapter._dispatcher.recover() == []
    assert adapter._dispatcher.active("888") is None
    assert adapter._queue.status("888")["pending"] == 1
    await adapter.disconnect()


async def test_send走HTTP并返回SendResult(monkeypatch, fake_http_server):
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    try:
        adapter._chat_types["888"] = "group"
        result = await adapter.send("888", "你好")
        assert isinstance(result, SendResult)
        assert result.success
        assert calls[0]["path"] == "/send_group_msg"
        assert calls[0]["params"]["group_id"] == 888
    finally:
        await adapter.disconnect()


async def test_send未连接返回失败(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    result = await adapter.send("888", "你好")
    assert not result.success


async def test_send分块之间lease失效不会发送后续块(monkeypatch, fake_http_server):
    """第一块成功后 fencing，第二块不能继续访问 OneBot API。"""
    base, _calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="触发",
        message_key="group:1001",
    )
    adapter._queue.enqueue(message, adapter_module.TriggerRequest.create(
        "888", "group:1001", "mention", "123", "小明"
    ))
    lease = adapter._queue.claim("888")
    assert lease is not None
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id=lease.lease_id,
    )
    binding = adapter_module.TurnBinding("session", "turn", caller, lease.lease_id)
    adapter._bindings.bind(binding)
    adapter._targets["888"] = adapter_module.ChatTarget("group", "888")
    checks = 0
    api_calls: list[str] = []

    def lease_current(_lease_id: str) -> bool:
        nonlocal checks
        checks += 1
        return checks <= 3

    async def send_message(_chat_id: str, text: str, **_kwargs: object) -> str:
        api_calls.append(text)
        return str(len(api_calls))

    monkeypatch.setattr(adapter._queue, "is_lease_current", lease_current)
    monkeypatch.setattr(adapter._api, "send_message", send_message)
    monkeypatch.setattr(adapter, "max_message_length_for_chat", lambda _chat_id: 2)
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(binding)
    try:
        result = await adapter.send("888", "abcd")
        assert not result.success
        assert api_calls == ["ab"]
        assert lease.lease_id in adapter._unknown_leases
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        await adapter.disconnect()


async def test_直接handle_message仍执行访问策略(monkeypatch):
    """绕过 WS 入口调用 adapter 时也不能把未授权私聊送进 Hermes。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_DM_POLICY="allowlist",
        ONEBOT11_ALLOWED_USERS="999",
    )
    event = await adapter._build_message_event(
        InboundEvent(
            text="不应进入",
            chat_id="123",
            chat_type="dm",
            user_id="123",
            user_name="小明",
            message_id="1",
        )
    )
    called = False

    async def fail_if_called(_event):
        nonlocal called
        called = True

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fail_if_called)
    await adapter.handle_message(event)
    assert called is False
    await adapter.disconnect()


async def test_未开始OneBot请求的turn成功也release(monkeypatch):
    """WS 已断开时没有出站请求，不能把队列 lease 错误确认掉。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id="lease-not-sent",
    )
    binding = adapter_module.TurnBinding("session-1", "turn-1", caller, "lease-not-sent")
    adapter._bindings.bind(binding)
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(binding)
    completed: list[tuple[str, str, bool]] = []

    async def fake_complete(lease_id: str, *, outcome: str, unknown: bool) -> bool:
        completed.append((lease_id, outcome, unknown))
        return True

    monkeypatch.setattr(adapter._dispatcher, "complete", fake_complete)
    try:
        result = await adapter.send("888", "你好")
        assert not result.success
        assert "lease-not-sent" in adapter._outbound_known_failure
        event = SimpleNamespace(
            metadata={"onebot11_lease_id": "lease-not-sent"},
            media_urls=[],
        )
        await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        assert completed == [("lease-not-sent", "failure", False)]
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        await adapter.disconnect()


async def test_completion状态读取瞬时失败后重试并继续下一anchor(monkeypatch):
    """SQLite 状态读取短暂失败不能让活动群永久停在旧 lease。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    monkeypatch.setattr(
        adapter_module,
        "_COMPLETION_RETRY_DELAYS",
        (0.0,),
        raising=False,
    )
    status_reads = 0
    completions: list[tuple[str, str, bool]] = []
    notifications: list[str] = []

    def flaky_status(_lease_id: str) -> dict[str, object]:
        nonlocal status_reads
        status_reads += 1
        if status_reads == 1:
            raise OSError("database is temporarily busy")
        return {"lease_phase": "agent_running", "outbound_started": False}

    async def fake_complete(lease_id: str, *, outcome: str, unknown: bool) -> bool:
        completions.append((lease_id, outcome, unknown))
        return True

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(chat_id)
        return True

    monkeypatch.setattr(adapter._queue, "status_for_lease", flaky_status)
    monkeypatch.setattr(adapter._dispatcher, "complete", fake_complete)
    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    event = SimpleNamespace(
        source=SimpleNamespace(chat_id="888"),
        metadata={"onebot11_lease_id": "lease-transient-status"},
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert status_reads == 2
        assert completions == [("lease-transient-status", "success", False)]
        assert notifications == ["888"]
    finally:
        await adapter.disconnect()


async def test_completion状态转换瞬时失败后重试并继续下一anchor(monkeypatch):
    """dispatcher.complete 短暂失败后仍应完成同一 lease 并推进群队列。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    monkeypatch.setattr(adapter_module, "_COMPLETION_RETRY_DELAYS", (0.0,))
    completion_attempts = 0
    notifications: list[str] = []

    monkeypatch.setattr(
        adapter._queue,
        "status_for_lease",
        lambda _lease_id: {"lease_phase": "agent_running", "outbound_started": False},
    )

    async def flaky_complete(_lease_id: str, *, outcome: str, unknown: bool) -> bool:
        nonlocal completion_attempts
        del outcome, unknown
        completion_attempts += 1
        if completion_attempts == 1:
            raise OSError("database is temporarily busy")
        return True

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(chat_id)
        return True

    monkeypatch.setattr(adapter._dispatcher, "complete", flaky_complete)
    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    event = SimpleNamespace(
        source=SimpleNamespace(chat_id="888"),
        metadata={"onebot11_lease_id": "lease-transient-complete"},
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert completion_attempts == 2
        assert notifications == ["888"]
    finally:
        await adapter.disconnect()


async def test_completion重试使用2_4_8秒有界退避(monkeypatch):
    """默认 completion 重试应只等待三次，并使用固定指数退避。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    delays: list[float] = []

    def broken_status(_lease_id: str) -> dict[str, object]:
        raise OSError("database remains busy")

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(adapter._queue, "status_for_lease", broken_status)
    monkeypatch.setattr(adapter_module.asyncio, "sleep", fake_sleep)
    event = SimpleNamespace(
        source=SimpleNamespace(chat_id="888"),
        metadata={"onebot11_lease_id": "lease-retry-schedule"},
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert delays == [2.0, 4.0, 8.0]
    finally:
        await adapter.disconnect()


async def test_completion重试耗尽后停止续租并fence旧turn(monkeypatch):
    """无法提交 completion 时必须放弃本进程 lease，让持久租约自然过期恢复。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._processing_reaction_enabled = False
    monkeypatch.setattr(adapter_module, "_COMPLETION_RETRY_DELAYS", (0.0, 0.0, 0.0))
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="@bot 查询",
        message_key="group:1001",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            "group:1001",
            "mention",
            "123",
            "小明",
        ),
    )

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    assert await adapter._dispatcher.notify("888")
    active = adapter._dispatcher.active("888")
    assert active is not None
    lease_id = active.lease.lease_id
    ack_attempts = 0

    def broken_ack(_lease) -> bool:
        nonlocal ack_attempts
        ack_attempts += 1
        raise OSError("database remains busy")

    monkeypatch.setattr(adapter._queue, "ack", broken_ack)
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id=lease_id,
    )
    binding = adapter_module.TurnBinding("session-1", "turn-1", caller, lease_id)
    adapter._bindings.bind(binding)
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(binding)
    adapter._pending_completions[lease_id] = (
        ProcessingOutcome.SUCCESS,
        False,
        False,
        None,
    )
    adapter._processing_reaction_message_ids[lease_id] = "1001"
    media_dir = adapter._new_media_dir()
    media_path = Path(media_dir) / "input.png"
    media_path.write_bytes(b"temporary")
    event = SimpleNamespace(
        source=SimpleNamespace(chat_id="888"),
        metadata={
            "onebot11_lease_id": lease_id,
            "onebot11_media_paths": [str(media_path)],
            "onebot11_media_dir": media_dir,
        },
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert ack_attempts == 4
        assert adapter._dispatcher.active("888") is None
        assert lease_id in adapter._fenced_leases
        assert not adapter._lease_is_current(lease_id)
        assert adapter._queue.status("888")["leased"] == 1
        assert adapter._bindings.get("session-1", "turn-1") is None
        assert lease_id not in adapter._pending_completions
        assert lease_id not in adapter._processing_reaction_message_ids
        assert not media_path.exists()
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        await adapter.disconnect()


async def test_completion重试在shutdown时取消且不访问已关闭queue(monkeypatch):
    """disconnect 应立即取消 completion 退避，并在 QueueStore 关闭前完成清理。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    monkeypatch.setattr(adapter_module, "_COMPLETION_RETRY_DELAYS", (60.0, 60.0, 60.0))
    lease_id = "lease-shutdown-retry"
    status_reads = 0
    entered_retry = asyncio.Event()

    def broken_status(_lease_id: str) -> dict[str, object]:
        nonlocal status_reads
        assert not adapter._queue._closed
        status_reads += 1
        entered_retry.set()
        raise OSError("database is temporarily busy")

    monkeypatch.setattr(adapter._queue, "status_for_lease", broken_status)
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id=lease_id,
    )
    binding = adapter_module.TurnBinding("session-1", "turn-1", caller, lease_id)
    adapter._bindings.bind(binding)
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(binding)
    adapter._pending_completions[lease_id] = (
        ProcessingOutcome.SUCCESS,
        False,
        False,
        None,
    )
    adapter._processing_reaction_message_ids[lease_id] = "1001"
    media_dir = adapter._new_media_dir()
    media_path = Path(media_dir) / "input.png"
    media_path.write_bytes(b"temporary")
    event = SimpleNamespace(
        source=SimpleNamespace(chat_id="888"),
        metadata={
            "onebot11_lease_id": lease_id,
            "onebot11_media_paths": [str(media_path)],
            "onebot11_media_dir": media_dir,
        },
        media_urls=[],
    )
    completion_task = asyncio.create_task(
        adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
    )
    adapter._background_tasks.add(completion_task)
    try:
        await asyncio.wait_for(entered_retry.wait(), timeout=1)
        await adapter.disconnect()
        assert completion_task.cancelled()
        assert status_reads == 1
        assert adapter._bindings.get("session-1", "turn-1") is None
        assert lease_id not in adapter._pending_completions
        assert lease_id not in adapter._processing_reaction_message_ids
        assert not media_path.exists()
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        if not adapter._closed:
            await adapter.disconnect()


async def test_已出站后turn失败进入uncertain而不是重放(monkeypatch):
    """部分或全部回复已发送后，整体失败不能自动重新执行 Agent。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    completed: list[tuple[str, str, bool]] = []

    async def fake_complete(lease_id: str, *, outcome: str, unknown: bool) -> bool:
        completed.append((lease_id, outcome, unknown))
        return True

    monkeypatch.setattr(adapter._dispatcher, "complete", fake_complete)
    adapter._outbound_started.add("lease-1")
    adapter._outbound_successful.add("lease-1")
    event = SimpleNamespace(metadata={"onebot11_lease_id": "lease-1"}, media_urls=[])
    await adapter.on_processing_complete(event, ProcessingOutcome.FAILURE)
    assert completed == [("lease-1", "failure", True)]
    assert "lease-1" not in adapter._outbound_successful
    await adapter.disconnect()


async def test_出站marker后明确失败也进入uncertain(monkeypatch):
    """请求一旦开始，即使返回业务错误也不自动重放整轮。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    completed: list[tuple[str, str, bool]] = []

    async def fake_complete(lease_id: str, *, outcome: str, unknown: bool) -> bool:
        completed.append((lease_id, outcome, unknown))
        return True

    monkeypatch.setattr(adapter._dispatcher, "complete", fake_complete)
    adapter._outbound_started.add("lease-2")
    adapter._outbound_known_failure.add("lease-2")
    event = SimpleNamespace(metadata={"onebot11_lease_id": "lease-2"}, media_urls=[])
    await adapter.on_processing_complete(event, ProcessingOutcome.FAILURE)
    assert completed == [("lease-2", "failure", True)]
    await adapter.disconnect()


async def test_完整出站成功不会被误判为unknown(monkeypatch):
    """所有分块明确成功时，队列 completion 必须走 ack。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._outbound_started.add("lease-success")
    adapter._outbound_successful.add("lease-success")
    decision = adapter._queue_completion_decision("lease-success", ProcessingOutcome.SUCCESS)
    assert decision == (True, False, False, None)
    await adapter.disconnect()


async def test_写工具按锚点权限直接执行且unknown后同turn禁止重复(monkeypatch):
    """写工具不再发确认令牌；unknown 后仍不能由模型在同一 turn 重放。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1",
        user_id="123",
        user_name="管理员",
        text="@bot 禁言用户 456",
        message_key="group:1",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888", "group:1", "mention", "123", "管理员"
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="super_admin",
        allowed_tools=adapter.role_tools["super_admin"],
        lease_id=lease.lease_id,
    )
    binding = adapter_module.TurnBinding("session", "turn", caller, lease.lease_id)
    adapter._bindings.bind(binding)
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(binding)
    calls = 0

    async def unknown_write(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise adapter_module.OneBotApiError(
            "set_group_ban",
            "network",
            -1,
            unknown_outcome=True,
            error_kind="unknown",
        )

    monkeypatch.setattr(adapter_module, "handle_write_action", unknown_write)
    handler = adapter._make_tool_handler("qq_set_group_ban")
    params = {"user_id": "456", "duration": 60}
    try:
        first = json.loads(
            await handler(params, session_id="session", turn_id="turn")
        )
        second = json.loads(
            await handler(params, session_id="session", turn_id="turn")
        )
        assert first["status"] == "unknown"
        assert second["status"] == "unknown"
        assert "禁止自动重复" in second["error"]
        assert calls == 1
    finally:
        adapter_module._CURRENT_BINDING.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        await adapter.disconnect()


async def test_turn权限快照不受执行中配置变化影响(monkeypatch):
    """角色配置更新只影响下一个 anchor；当前 binding 继续使用旧精确工具集。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="trusted_user",
        allowed_tools=frozenset({"web_search"}),
    )
    adapter.role_tools["trusted_user"] = frozenset()
    try:
        assert adapter._tool_allowed_now(caller, "web_search")
        assert not adapter._tool_allowed_now(caller, "terminal")
    finally:
        await adapter.disconnect()


async def test_metadata权限快照不被pre_gateway重新计算(monkeypatch):
    """synthetic event 已携带的 turn-start 工具集不能被新配置替换。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="trusted_user",
        allowed_tools=frozenset({"web_search"}),
    )
    event = SimpleNamespace(
        source=SimpleNamespace(
            platform=adapter_module._platform(),
            chat_type="group",
            chat_id="888",
            user_id="123",
        ),
        metadata={
            "onebot11_caller_context": adapter_module._serializable_caller(caller),
            "onebot11_managed_context": True,
        },
    )
    adapter.role_tools["trusted_user"] = frozenset()
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    try:
        adapter_module._pre_gateway_dispatch_hook(event)
        result = adapter_module._pre_llm_call_hook(
            session_id="snapshot-session",
            turn_id="snapshot-turn",
            platform="onebot11",
        )
        assert result is not None
        current = adapter_module._CURRENT_CALLER.get()
        assert current is not None
        assert current.allowed_tools == frozenset({"web_search"})
    finally:
        adapter_module._CURRENT_BINDING.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        await adapter.disconnect()


async def test_默认队列落在Hermes_home而不是临时文件(monkeypatch, tmp_path):
    """未显式配置时，群消息队列必须跨 adapter 重启保留。"""
    hermes_home = tmp_path / "hermes-home"
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    try:
        assert Path(adapter._queue.path) == hermes_home / "onebot11" / "queue.sqlite3"
        assert Path(adapter._queue.path).exists()
    finally:
        await adapter.disconnect()


async def test_同号群和私聊目标必须显式区分(monkeypatch):
    """普通模糊发送不能在群号和 QQ 号冲突时猜目标。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._targets["42"] = None
    adapter._ambiguous_targets.add("42")
    assert adapter._resolve_target("42", None) is None
    assert adapter._resolve_target(
        "42",
        {"onebot11_target": {"chat_type": "group", "chat_id": "42"}},
    ) is None
    assert adapter._resolve_target(
        "42",
        {
            "onebot11_target": {"chat_type": "group", "chat_id": "42"},
            "onebot11_trusted_target": True,
        },
    ) == adapter_module.ChatTarget("group", "42")
    assert adapter._resolve_target(
        "42",
        {
            "onebot11_target": {"chat_type": "group", "chat_id": "42"},
            "onebot11_trusted_target": "true",
        },
    ) is None
    assert adapter._resolve_target(
        "43",
        {
            "onebot11_target": {"chat_type": "group", "chat_id": "43"},
            "onebot11_trusted_target": True,
        },
    ) is None
    await adapter.disconnect()


def test_未知管理动作指纹包含目标群(monkeypatch):
    """同一参数在不同群不是同一个可能重复执行的动作。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    params = {"user_id": "123", "duration": 60}
    first = adapter._operation_fingerprint(
        "qq_set_group_ban", params, chat_type="group", chat_id="888"
    )
    second = adapter._operation_fingerprint(
        "qq_set_group_ban", params, chat_type="group", chat_id="999"
    )
    assert first != second


async def test_未知qq工具hook直接拒绝(monkeypatch):
    """没有注册的 OneBot 工具不能绕过 pre_tool_call。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    result = adapter_module._pre_tool_call_hook(
        tool_name="qq_unknown", session_id="session", turn_id="turn", args={}
    )
    assert result == {"action": "block", "message": "权限错误: 未知 OneBot11 工具"}
    await adapter.disconnect()


async def test_普通工具在adapter缺失时fail_closed(monkeypatch):
    """OneBot caller 存在但 live adapter 消失时，普通宿主工具也不能放行。"""
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
    )
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(None)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: None)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="session",
            turn_id="turn",
            args={},
        )
        assert result is not None
        assert result["action"] == "block"
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)


async def test_pre_tool_call优先按session_turn查找binding(monkeypatch):
    """ContextVar 丢失时，精确 session/turn 绑定仍不能让普通工具绕过门禁。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
    )
    adapter._bindings.bind(adapter_module.TurnBinding("session", "turn", caller))
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    adapter_module._CURRENT_CALLER.set(None)
    adapter_module._CURRENT_BINDING.set(None)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="session",
            turn_id="turn",
            args={},
        )
        assert result is not None
        assert result["action"] == "block"
        assert "无权" in result["message"]
    finally:
        await adapter.disconnect()


async def test_pre_tool_call_context与显式binding冲突时拒绝(monkeypatch):
    """ContextVar 不能覆盖显式 session/turn 的另一份调用者身份。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    first = adapter_module.CallerContext(user_id="123", chat_type="group", chat_id="888")
    second = adapter_module.CallerContext(user_id="456", chat_type="group", chat_id="888")
    adapter._bindings.bind(adapter_module.TurnBinding("session", "turn", first))
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    adapter_module._CURRENT_CALLER.set(second)
    adapter_module._CURRENT_BINDING.set(None)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="session",
            turn_id="turn",
            args={},
        )
        assert result is not None
        assert "冲突" in result["message"]
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        await adapter.disconnect()


async def test_审计写失败不改变权限拒绝结果(monkeypatch):
    """审计磁盘故障不能让拒绝 hook 抛异常或 fail-open。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    event = await adapter._build_message_event(
        InboundEvent(
            text="查询",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="小明",
            message_id="1001",
        )
    )
    event.metadata["onebot11_caller_context"] = adapter_module._serializable_caller(
        adapter._caller_for_event(event.source)
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    adapter_module._pre_gateway_dispatch_hook(event)
    adapter_module._pre_llm_call_hook(
        session_id="session-a",
        turn_id="turn-a",
        platform="onebot11",
    )

    def broken_record(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(adapter._audit, "record", broken_record)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="session-a",
            turn_id="turn-a",
            args={},
        )
        assert result is not None
        assert result["action"] == "block"
    finally:
        adapter_module._CURRENT_BINDING.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        await adapter.disconnect()


async def test_delegate_task禁止配置和运行(monkeypatch):
    """在 Hermes 尚未支持 per-turn 子代理授权前，OneBot 不接受 delegate_task。"""
    with pytest.raises(ValueError):
        OneBot11Adapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "http_api": "http://127.0.0.1:3000",
                    "self_id": "1",
                    "roles": {"user": {"tools": ["delegate_task"]}},
                },
            )
        )
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter_module._CURRENT_CALLER.set(
        adapter_module.CallerContext(user_id="123", chat_type="group", chat_id="888")
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="delegate_task",
            session_id="s",
            turn_id="t",
            args={},
        )
        assert result is not None
        assert result["action"] == "block"
    finally:
        adapter_module._CURRENT_BINDING.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        await adapter.disconnect()


async def test_delegate_task不阻断其他platform(monkeypatch):
    """OneBot 禁止 delegate_task 不能误伤没有 OneBot caller 的其他平台。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    try:
        assert adapter_module._pre_tool_call_hook(
            tool_name="delegate_task",
            session_id="discord-session",
            turn_id="discord-turn",
            platform="discord",
            args={},
        ) is None
    finally:
        await adapter.disconnect()


async def test_未知onebot工具hook直接拒绝(monkeypatch):
    """没有注册的 onebot_ 工具同样不能绕过 pre_tool_call。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    result = adapter_module._pre_tool_call_hook(
        tool_name="onebot_unknown", session_id="session", turn_id="turn", args={}
    )
    assert result == {"action": "block", "message": "权限错误: 未知 OneBot11 工具"}
    await adapter.disconnect()


async def test_其他平台不能继承OneBot调用者权限(monkeypatch):
    """OneBot caller 不能把当前权限带到 subagent 或其他 platform。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    event = await adapter._build_message_event(
        InboundEvent(
            text="查询",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="小明",
            message_id="1001",
        )
    )
    caller = adapter._caller_for_event(event.source)
    event.metadata["onebot11_caller_context"] = adapter_module._serializable_caller(caller)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    adapter_module._pre_gateway_dispatch_hook(event)
    assert adapter_module._pre_llm_call_hook(
        session_id="same-session", turn_id="same-turn", platform="onebot11"
    ) is not None
    blocked_cross_platform = adapter_module._pre_tool_call_hook(
        tool_name="web_search",
        session_id="same-session",
        turn_id="same-turn",
        platform="discord",
        args={},
    )
    assert blocked_cross_platform is not None
    assert blocked_cross_platform["action"] == "block"
    child_result = json.loads(
        await adapter._make_tool_handler("qq_get_group_info")(
            {},
            session_id="same-session",
            turn_id="same-turn",
            platform="subagent",
        )
    )
    assert child_result["status"] == "permission_error"
    adapter_module._CURRENT_BINDING.set(None)
    adapter_module._CURRENT_CALLER.set(None)
    await adapter.disconnect()


async def test_trusted用户metadata路径保留角色(monkeypatch):
    """合成事件从 metadata 恢复时，trusted_user 不能降级为 user。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ALLOWED_GROUPS="888",
    )
    adapter.trusted_users = {"123"}
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller = adapter_module._caller_from_metadata(
        {
            "user_id": "123",
            "chat_type": "group",
            "chat_id": "888",
        }
    )
    assert caller is not None
    assert caller.role == "trusted_user"
    await adapter.disconnect()


async def test_lease不能跨群注入caller(monkeypatch):
    """合成事件携带其他群 lease 时，pre-gateway 身份解析必须 fail-closed。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1",
        user_id="123",
        user_name="小明",
        text="hi",
        message_key="group:1",
    )
    adapter._queue.enqueue(message, adapter_module.TriggerRequest.create("888", "group:1", "mention", "123", "小明"))
    lease = adapter._queue.claim("888")
    assert lease is not None
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller = adapter_module._caller_from_metadata(
        {"user_id": "123", "chat_type": "group", "chat_id": "999", "lease_id": lease.lease_id}
    )
    assert caller is None
    await adapter.disconnect()


async def test_私聊运维命令拒绝群队列操作(monkeypatch):
    """status/clear/pause/resolve 不能把私聊 QQ 号当成群号。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    responses: list[str] = []

    async def fake_send(_event, text: str) -> None:
        responses.append(text)

    monkeypatch.setattr(adapter, "_send_direct", fake_send)
    for command in ("status", "clear", "pause", "resolve retry"):
        await adapter._handle_admin_command(
            InboundEvent(
                text=f"/onebot {command}",
                chat_id="123",
                chat_type="dm",
                user_id="123",
                user_name="管理员",
                message_id=command,
            )
        )
    assert len(responses) == 4
    assert all(
        "只能作用于当前群队列" in response or "访问策略" in response
        for response in responses
    )
    await adapter.disconnect()


async def test_resolve_retry缺少anchor时保持legacy_hold(monkeypatch):
    """旧消息缺失 anchor 时保持 hold，不能借管理员身份执行。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1",
        user_id="123",
        user_name="管理员",
        text="未知结果",
        message_key="group:1",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create("888", "group:1", "mention", "123", "管理员"),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    assert adapter._queue.mark_uncertain(lease, "出站未知")
    adapter._queue._conn.execute("DELETE FROM onebot_queue_trigger WHERE chat_id=?", ("888",))
    adapter._queue._conn.commit()
    started: list[str] = []
    responses: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        started.append(chat_id)
        return True

    async def fake_send_direct(_event, text: str) -> None:
        responses.append(text)

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    monkeypatch.setattr(adapter, "_send_direct", fake_send_direct)
    await adapter._handle_admin_command(
        InboundEvent(
            text="/onebot resolve retry",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="管理员",
            message_id="command",
        )
    )
    assert started == []
    status = adapter._queue.status("888")
    assert status["uncertain"] == 1
    assert status["pending_trigger_requests"] == 0
    assert responses and "新的明确触发" in responses[0]
    await adapter.disconnect()


def test_check_requirements(monkeypatch):
    """依赖检查不读取部署配置；配置合同由 validate_config 负责。"""
    assert check_requirements()


def test_validate_config支持YAML配置且拒绝非法HTTP地址(monkeypatch):
    """部署配置可以只来自 YAML，但 malformed endpoint 必须 fail-closed。"""
    monkeypatch.delenv("ONEBOT11_HTTP_API", raising=False)
    monkeypatch.delenv("ONEBOT11_SELF_ID", raising=False)
    config = SimpleNamespace(
        extra={"http_api": "http://127.0.0.1:3000", "self_id": "1"}
    )
    assert validate_config(config)
    config.extra["http_api"] = "http://user:pass@127.0.0.1:3000"
    assert not validate_config(config)


async def test_空列表环境变量覆盖YAML权限配置(monkeypatch):
    """显式清空管理员和群白名单不能回退到旧 YAML 值。"""
    monkeypatch.setenv("ONEBOT11_HTTP_API", "http://127.0.0.1:3000")
    monkeypatch.setenv("ONEBOT11_SELF_ID", "1")
    monkeypatch.setenv("ONEBOT11_SUPER_ADMINS", "")
    monkeypatch.setenv("ONEBOT11_ALLOWED_GROUPS", "")
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={"super_admins": ["10001"], "allowed_groups": ["888"]},
        )
    )
    assert adapter.super_admins == set()
    assert adapter.allowed_groups == set()
    await adapter.disconnect()


async def test_get_chat_info重新检查当前访问策略(monkeypatch):
    """旧目标登记不能绕过后来收紧的群白名单。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._chat_types["888"] = "group"
    adapter.allowed_groups = {"999"}
    with pytest.raises(ValueError):
        await adapter.get_chat_info("888")
    await adapter.disconnect()


async def test_cron独立发送复用OneBot白名单(monkeypatch):
    """cron 不能绕过 adapter 的群白名单和私聊策略。"""
    monkeypatch.setenv("ONEBOT11_ALLOWED_GROUPS", "1072992996")
    result = await adapter_module._standalone_send(
        SimpleNamespace(
            extra={
                "http_api": "http://127.0.0.1:3000",
                "home_channel_type": "group",
            }
        ),
        "786830134",
        "不应发送",
    )
    assert result["status"] == "permission_error"


def test_register注册平台与工具():
    class FakeCtx:
        def __init__(self):
            self.platform_kwargs = None
            self.tools: list[dict] = []
            self.hooks: dict[str, object] = {}

        def register_platform(self, **kwargs):
            self.platform_kwargs = kwargs

        def register_tool(self, **kwargs):
            self.tools.append(kwargs)

        def register_hook(self, name, handler):
            self.hooks[name] = handler

    ctx = FakeCtx()
    register(ctx)
    assert ctx.platform_kwargs["name"] == "onebot11"
    assert ctx.platform_kwargs["cron_deliver_env_var"] == "ONEBOT11_HOME_CHANNEL"
    yaml_bridge = ctx.platform_kwargs["apply_yaml_config_fn"]
    assert yaml_bridge({}, {}) == {"group_sessions_per_user": False}
    assert yaml_bridge({}, {"extra": {"group_sessions_per_user": True}}) == {
        "group_sessions_per_user": True
    }
    names = {t["name"] for t in ctx.tools}
    assert names == {
        "qq_get_message",
        "qq_get_group_msg_history",
        "qq_get_friend_msg_history",
        "qq_get_group_info",
        "qq_get_group_member_info",
        "qq_delete_message",
        "qq_set_group_ban",
        "qq_set_group_kick",
        "qq_set_group_whole_ban",
        "onebot_get_permissions",
        "onebot_set_role_tools",
        "onebot_set_trusted_users",
    }
    for t in ctx.tools:
        assert t["toolset"] == "onebot11"
        assert t["is_async"] is True
    assert {"pre_gateway_dispatch", "pre_llm_call", "pre_tool_call"} <= set(ctx.hooks)


def test_register缺少关键hook时拒绝启用(monkeypatch):
    """Hermes 无法提供权限门禁 hook 时不能静默注册 OneBot。"""
    monkeypatch.setitem(
        sys.modules,
        "hermes_cli.plugins",
        SimpleNamespace(VALID_HOOKS={"pre_llm_call"}),
    )

    class FakeCtx:
        def register_hook(self, _name, _handler):
            raise AssertionError("缺少关键 hook 时不应继续注册")

    with pytest.raises(RuntimeError, match="缺少关键 hooks"):
        register(FakeCtx())
