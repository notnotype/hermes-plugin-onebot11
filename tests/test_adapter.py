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


def test_连接生命周期(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    assert not adapter.is_connected
    asyncio.get_event_loop().run_until_complete(adapter.connect())
    assert adapter.is_connected
    asyncio.get_event_loop().run_until_complete(adapter.disconnect())
    assert not adapter.is_connected


def test_缺HTTP配置时connect失败(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_SELF_ID="1")  # 没有 ONEBOT11_HTTP_API
    assert not asyncio.get_event_loop().run_until_complete(adapter.connect())


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
        assert adapter._queue.status("888")["pending"] == 0
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
        text="不需要回复",
        message_key="group:llm-false",
    )
    adapter._queue.enqueue(message)
    calls = 0

    async def fake_call(**_kwargs):
        nonlocal calls
        calls += 1
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="false"))]
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
            text="等待模型",
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
        assert adapter._queue.status("888")["llm_failure_count"] == 1
        now[0] = 1002.1
        await adapter._judge_llm_trigger("888")
        assert calls == 2
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
        metadata={"onebot11_cooldown_candidate": True},
    )
    adapter._queue.enqueue(message)
    try:
        assert await adapter._dispatcher.recover() == []
        status = adapter._queue.status("777")
        assert status["pending"] == 1
        assert status["pending_trigger_requests"] == 0
    finally:
        await adapter.disconnect()


async def test_unknown后新确认令牌允许管理员再次明确执行(monkeypatch):
    """unknown 不自动重试，但新的人工确认令牌可以再次执行。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    calls = 0

    async def fake_write(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"status": "unknown", "error": "timeout"}
        return {"status": "ok"}

    monkeypatch.setattr(adapter_module, "handle_write_action", fake_write)

    def issue_and_consume():
        issued = adapter._confirmations.issue(
            "qq_set_group_ban",
            {"user_id": "456", "duration": 60},
            user_id="123",
            chat_type="group",
            chat_id="888",
        )
        return adapter._confirmations.consume(
            issued.token,
            user_id="123",
            chat_type="group",
            chat_id="888",
        )

    try:
        first = await adapter._execute_confirmed(issue_and_consume())
        second = await adapter._execute_confirmed(issue_and_consume())
        assert first["status"] == "unknown"
        assert "warning" in first
        assert second["status"] == "ok"
        assert calls == 2
    finally:
        await adapter.disconnect()


async def test_adapter关闭后工具和确认入口都fail_closed(monkeypatch):
    """disconnect 一开始 fencing 后，旧 turn 不能再调用工具或执行确认。"""
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
    confirmation = adapter._confirmations.issue(
        "qq_set_group_ban",
        {"user_id": "456", "duration": 60},
        user_id="123",
        chat_type="group",
        chat_id="888",
    )
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
        confirmed = await adapter._execute_confirmed(confirmation)
        assert hook_result is not None and hook_result["action"] == "block"
        assert handler_result["status"] == "permission_error"
        assert confirmed["status"] == "permission_error"
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


async def test_关闭require_mention后无at也放行(monkeypatch):
    """ONEBOT11_REQUIRE_MENTION=false 时, 群里所有消息都创建 trigger。"""
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
    assert adapter._queue.status("888")["trigger_requests"] == 1


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
        text="第一条",
        message_key="group:1",
    )
    second = replace(first, message_id="2", message_key="group:2", text="第二条")
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
            choices=[SimpleNamespace(message=SimpleNamespace(content="false"))]
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


async def test_resolve_retry缺少trigger时补建管理员触发(monkeypatch):
    """即使旧文件只剩 uncertain 消息，管理员 retry 也能重新启动群 turn。"""
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
    assert started == ["888"]
    assert adapter._queue.status("888")["pending_trigger_requests"] == 1
    assert responses and "retry" in responses[0]
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

        def register_platform(self, **kwargs):
            self.platform_kwargs = kwargs

        def register_tool(self, **kwargs):
            self.tools.append(kwargs)

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
