"""adapter.py 冒烟测试。

需要 hermes gateway 可导入（本地跑：用 hermes venv + PYTHONPATH）；
CI 环境没有 gateway 时自动跳过。
"""

import asyncio
import json
import os
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
from onebot11.dispatch import ActiveTurn  # noqa: E402
from onebot11.events import InboundEvent  # noqa: E402
from onebot11.queue import QueueMessage  # noqa: E402


def _make_adapter(monkeypatch, **env) -> OneBot11Adapter:
    # 默认用随机端口,避免与正在运行的网关(0.0.0.0:18880)撞端口
    env.setdefault("ONEBOT11_WS_PORT", "0")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return OneBot11Adapter(PlatformConfig(enabled=True, extra={}))


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


async def test_同一adapter断开后可以重连并继续使用队列(monkeypatch, tmp_path):
    """Hermes 重用同一 adapter 时，queue/dispatcher 不能保留 closed 状态。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_QUEUE_DB=str(tmp_path / "queue.sqlite3"),
    )
    await adapter.connect()
    await adapter.disconnect()
    assert adapter._queue.closed

    await adapter.connect(is_reconnect=True)
    assert not adapter._queue.closed
    assert not adapter._dispatcher._closed
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="reconnect-message",
        user_id="123",
        user_name="小明",
        text="重连后继续入队",
        message_key="group:reconnect-message",
    )
    assert adapter._queue.enqueue(message).inserted
    assert adapter._queue.peek("888")[0].text == "重连后继续入队"
    await adapter.disconnect()


async def test_活动turn重连时旧heartbeat和managed_send被fence(monkeypatch, tmp_path):
    """同实例 reconnect 必须结算旧 lease，旧 task 不能在新 owner 下出站。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_QUEUE_DB=str(tmp_path / "queue.sqlite3"),
    )
    await adapter.connect()
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="active-reconnect",
        user_id="123",
        user_name="小明",
        text="重连前活动",
        message_key="group:active-reconnect",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            "group:active-reconnect",
            "mention",
            "123",
            "小明",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    adapter._queue.set_paused("888", True)
    adapter._dispatcher._active["888"] = ActiveTurn(
        lease=lease,
        started_at=lease.claimed_at,
    )
    heartbeat = asyncio.create_task(adapter._dispatcher._heartbeat(lease))
    adapter._dispatcher._heartbeat_tasks["888"] = heartbeat
    old_event = SimpleNamespace(
        metadata={
            "onebot11_managed_context": True,
            "onebot11_lease_id": lease.lease_id,
        }
    )
    adapter_module._CURRENT_EVENT.set(old_event)
    try:
        await adapter.connect(is_reconnect=True)
        assert heartbeat.done()
        assert adapter._dispatcher.active("888") is None
        assert adapter._queue.status("888")["pending"] == 1
        result = await adapter.send(
            "888",
            "旧 task 不得发送",
            metadata=old_event.metadata,
        )
        assert not result.success
        assert result.error_kind == "fenced"
    finally:
        adapter_module._CURRENT_EVENT.set(None)
        await adapter.disconnect()


def test_缺HTTP配置时connect失败(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_SELF_ID="1")  # 没有 ONEBOT11_HTTP_API
    assert not asyncio.run(adapter.connect())


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


async def test_lease在工具访问HTTP前失效时拒绝新请求(monkeypatch):
    """第一次检查通过、真正访问 API 前 lease 失效时，不能发起新查询。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id="lease-race",
        self_id="1",
    )
    binding = adapter_module.TurnBinding("session-race", "turn-race", caller, "lease-race")
    adapter._bindings.bind(binding)
    calls: list[dict] = []

    async def fake_group_info(api, params, ctx):
        del api, params, ctx
        calls.append({})
        return {"status": "ok"}

    monkeypatch.setitem(adapter_module._TOOL_HANDLERS, "qq_get_group_info", fake_group_info)
    checks = iter((True, False))
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: next(checks))
    try:
        result = json.loads(
            await adapter._make_tool_handler("qq_get_group_info")(
                {},
                session_id="session-race",
                turn_id="turn-race",
            )
        )
        assert result == {"status": "permission_error", "error": "当前 turn lease 已失效"}
        assert calls == []
    finally:
        await adapter.disconnect()


async def test_写工具只生成确认预览不访问OneBot(monkeypatch):
    """Agent 不能在 turn 内直接执行群管理写操作。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="super_admin",
        allowed_tools=adapter.role_tools["super_admin"],
        self_id="1",
    )
    binding = adapter_module.TurnBinding("session-write", "turn-write", caller, None)
    adapter._bindings.bind(binding)
    calls: list[dict] = []

    async def fail_if_called(api, tool_name, args, ctx):
        del api, tool_name, args, ctx
        calls.append({})
        return {"status": "ok"}

    monkeypatch.setattr(adapter_module, "handle_write_action", fail_if_called)
    try:
        result = json.loads(
            await adapter._make_tool_handler("qq_set_group_ban")(
                {"user_id": "456", "duration": 60},
                session_id="session-write",
                turn_id="turn-write",
            )
        )
        assert result["status"] == "confirmation_required"
        assert result["command"].startswith("/onebot confirm ")
        assert calls == []
    finally:
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


def test_pre_llm缺身份时清理旧binding(monkeypatch):
    """新的 OneBot turn 缺身份时，不能继承上一个 turn 的 binding。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    binding = adapter_module.TurnBinding("old-session", "old-turn", caller)
    adapter._bindings.bind(binding)
    adapter_module._CURRENT_CALLER.set(None)
    adapter_module._CURRENT_BINDING.set(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    try:
        result = adapter_module._pre_llm_call_hook(
            session_id="new-session",
            turn_id="new-turn",
            platform="onebot11",
        )
        assert result == {
            "context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."
        }
        assert adapter_module._CURRENT_CALLER.get() is None
        assert adapter_module._CURRENT_BINDING.get() is None
        assert adapter._bindings.get("old-session", "old-turn") is None
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        asyncio.run(adapter.disconnect())


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


async def test_DM完成按事件中的精确binding清理(monkeypatch):
    """DM completion 不能依赖最近的 ContextVar 才能释放 caller。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="dm",
        chat_id="123",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    binding = adapter_module.TurnBinding("dm-session", "dm-turn", caller)
    adapter._bindings.bind(binding)
    event = SimpleNamespace(
        metadata={
            "onebot11_managed_context": True,
            "onebot11_binding_key": {
                "session_id": "dm-session",
                "turn_id": "dm-turn",
            },
        },
        media_urls=[],
    )
    adapter_module._CURRENT_CALLER.set(None)
    adapter_module._CURRENT_BINDING.set(None)
    try:
        await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        assert adapter._bindings.get("dm-session", "dm-turn") is None
    finally:
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        await adapter.disconnect()


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
        adapter._outbound_started.add(active.lease.lease_id)
        adapter._outbound_successful.add(active.lease.lease_id)
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


async def test_turn媒体达到总上限后不下载后续消息(monkeypatch):
    """一个 turn 的媒体达到总上限后，后续队列消息不再发起下载。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        max_image_total_bytes="1024",
    )
    adapter._processing_reaction_enabled = False
    adapter._max_media_total_bytes = 1024
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="第一张",
        metadata={"onebot11_images": ["http://media.invalid/first.png"]},
        message_key="group:1001",
    )
    second = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="456",
        user_name="小红",
        text="第二张",
        metadata={"onebot11_images": ["http://media.invalid/second.png"]},
        message_key="group:1002",
    )
    adapter._queue.enqueue(
        first,
        adapter_module.TriggerRequest.create("888", "group:1001", "mention", "123", "小明"),
    )
    adapter._queue.enqueue(second)
    downloaded: list[str] = []
    media_dirs: list[str] = []

    async def fake_download(image: str, dest_dir: str | None = None) -> str:
        downloaded.append(image)
        assert dest_dir is not None
        media_dirs.append(dest_dir)
        path = Path(dest_dir) / f"{len(downloaded)}.png"
        path.write_bytes(b"x" * 1024)
        return str(path)

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    monkeypatch.setattr(adapter, "_download_image", fake_download)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    try:
        assert await adapter._dispatcher.notify("888")
        assert downloaded == ["http://media.invalid/first.png"]
        active = adapter._dispatcher.active("888")
        assert active is not None
        await adapter._finish_queue_turn(
            SimpleNamespace(
                metadata={
                    "onebot11_lease_id": active.lease.lease_id,
                    "onebot11_media_dir": media_dirs[0],
                },
                media_urls=[],
            ),
            ProcessingOutcome.SUCCESS,
        )
    finally:
        await adapter.disconnect()


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


async def test_ws事件self_id不匹配只记录拒绝不入队(monkeypatch):
    """adapter 层要把 raw self_id mismatch 变成可审计拒绝。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    raw = _group_raw(888)
    raw["self_id"] = "999"
    await adapter._on_ws_event(raw)
    assert adapter._queue.status("888")["pending"] == 0
    await adapter.disconnect()


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


async def test_LLM候选入队不会在群触发锁上死锁(monkeypatch):
    """启用旁路判断时，候选消息入队不能再次等待同一群的不可重入锁。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "test-provider",
                    "model": "test-model",
                    "groups": ["888"],
                },
            },
        )
    )
    adapter._llm_trigger_api_supported = True
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="question-1",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:question-1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        await asyncio.wait_for(
            adapter._enqueue_group_message(
                message,
                mentioned_self=False,
                caller=caller,
                user_name="小明",
            ),
            timeout=0.2,
        )
        assert adapter._trigger_states["888"].mode == "debounce"
        assert adapter._queue.status("888")["pending"] == 1
    finally:
        await adapter.disconnect()


async def test_LLM_trigger的reaction锚定候选批次最新消息(monkeypatch):
    """旁路判断使用最新候选消息时，👀 不能回到队列最早消息。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "test-provider",
                    "model": "test-model",
                    "groups": ["888"],
                },
            },
        )
    )
    first = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="前一条",
        message_key="group:1001",
    )
    latest = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="456",
        user_name="小红",
        text="这个问题怎么处理？",
        message_key="group:1002",
    )
    adapter._queue.enqueue(first)
    adapter._queue.enqueue(latest)
    try:
        assert await adapter._create_llm_trigger("888")
        active = adapter._dispatcher.active("888")
        assert active is not None
        assert adapter._reaction_message_id(active.lease) == "1002"
    finally:
        await adapter.disconnect()


async def test_shared_session摘要优先使用临时channel_prompt(monkeypatch):
    """当前 batch 进入 transcript，滚动摘要不再重复写入 user message。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._processing_reaction_enabled = False
    first = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="summary-1",
        user_id="123",
        user_name="小明",
        text="历史内容",
        message_key="group:summary-1",
    )
    adapter._queue.enqueue(
        first,
        adapter_module.TriggerRequest.create(
            "888", "group:summary-1", "mention", "123", "小明"
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    assert adapter._queue.ack(lease)
    second = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="summary-2",
        user_id="123",
        user_name="小明",
        text="本轮问题",
        message_key="group:summary-2",
    )
    adapter._queue.enqueue(
        second,
        adapter_module.TriggerRequest.create(
            "888", "group:summary-2", "mention", "123", "小明"
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    captured: list[object] = []

    async def capture_handle(_adapter, event) -> None:
        captured.append(event)

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", capture_handle)
    try:
        await adapter._start_queue_turn(lease)
        assert captured
        event = captured[0]
        assert event.text and "本轮问题" in event.text
        assert "历史内容" not in event.text
        assert getattr(event, "channel_prompt", None)
        assert "历史内容" in event.channel_prompt
    finally:
        adapter._queue.release(lease, reason="test cleanup")
        await adapter.disconnect()


async def test_旧Hermes辅助API会安全禁用LLM触发(monkeypatch):
    """旧 Hermes 没有严格旁路参数时，插件不应偷偷改用主模型。"""
    import agent.auxiliary_client as auxiliary_client

    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )

    async def old_async_call_llm(*, task=None, messages):
        del task, messages
        raise AssertionError("旧 API 不应被真正调用")

    monkeypatch.setattr(auxiliary_client, "async_call_llm", old_async_call_llm)
    adapter._llm_trigger_api_supported = None
    try:
        assert adapter._llm_trigger_api_ready() is False
        assert adapter._llm_trigger_api_ready() is False
        assert adapter._llm_trigger_api_audited is True
    finally:
        await adapter.disconnect()


async def test_pause会取消旁路判断_resume恢复持久触发(monkeypatch):
    """暂停不启动 Agent/旁路模型，恢复后再从 durable trigger 继续。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="pause-1",
        user_id="123",
        user_name="管理员",
        text="待处理",
        message_key="group:pause-1",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            "group:pause-1",
            "mention",
            "123",
            "管理员",
        ),
    )
    state = adapter._trigger_state_for("888")
    state.observe_message(
        chat_type="group",
        text="这个问题怎么处理？",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=0,
    )
    timer = asyncio.create_task(asyncio.sleep(60))
    adapter._trigger_timer_tasks["888"] = timer
    notified: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notified.append(chat_id)
        return True

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    try:
        assert await adapter._set_group_paused("888", True)
        assert adapter._queue.status("888")["paused"] is True
        assert state.mode == "idle"
        await asyncio.sleep(0)
        assert timer.cancelled() or timer.done()
        assert notified == []

        assert await adapter._set_group_paused("888", False)
        assert adapter._queue.status("888")["paused"] is False
        assert notified == ["888"]
    finally:
        await adapter.disconnect()


async def test_flush在群锁内创建触发且取消旧判断(monkeypatch):
    """flush 与 LLM 判断竞争时只留下一个 durable trigger。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="flush-1",
        user_id="456",
        user_name="成员",
        text="待 flush",
        message_key="group:flush-1",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    state.observe_message(
        chat_type="group",
        text="这个问题怎么处理？",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=0,
    )
    cancelled: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        cancelled.append(chat_id)
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    try:
        has_request, started, paused = await adapter._flush_group(
            "888",
            caller_user_id="123",
            caller_user_name="管理员",
        )
        assert (has_request, started, paused) == (True, False, False)
        assert adapter._queue.status("888")["pending_trigger_requests"] == 1
        assert state.mode == "idle"
        assert cancelled == ["888"]
    finally:
        await adapter.disconnect()


async def test_clear同时失效旧的活跃触发状态(monkeypatch):
    """清空队列后不能让旧 debounce 或 engaged 状态影响下一条消息。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="clear-state",
        user_id="456",
        user_name="成员",
        text="这个问题怎么处理？",
        message_key="group:clear-state",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    action = state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=0,
    )
    assert action.kind == "schedule"
    timer = asyncio.create_task(asyncio.sleep(60))
    adapter._trigger_timer_tasks["888"] = timer
    try:
        assert await adapter._clear_group("888") == 1
        assert state.mode == "idle"
        assert adapter._queue.status("888")["pending"] == 0
        await asyncio.sleep(0)
        assert timer.cancelled() or timer.done()
    finally:
        await adapter.disconnect()


async def test_completion不覆盖期间新消息的候选状态(monkeypatch):
    """消息在 ack 前抵达时，旧 turn 收尾保留新的 debounce。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "test-provider",
                    "model": "test-model",
                    "groups": ["888"],
                },
            },
        )
    )
    adapter._llm_trigger_api_supported = True
    adapter._processing_reaction_enabled = False
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="complete-1",
        user_id="123",
        user_name="成员",
        text="原始触发",
        message_key="group:complete-1",
    )
    adapter._queue.enqueue(
        first,
        adapter_module.TriggerRequest.create(
            "888",
            "group:complete-1",
            "mention",
            "123",
            "成员",
        ),
    )

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    original_complete = adapter._dispatcher.complete

    async def complete_after_new_message(
        lease_id: str,
        *,
        outcome: str,
        unknown: bool,
        **kwargs,
    ) -> bool:
        new_message = adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="complete-2",
            user_id="456",
            user_name="成员2",
            text="新消息怎么处理？",
            message_key="group:complete-2",
        )
        await adapter._enqueue_group_message(
            new_message,
            mentioned_self=False,
            caller=adapter_module.CallerContext(
                user_id="456",
                chat_type="group",
                chat_id="888",
                role="user",
                allowed_tools=adapter_module.READ_ONLY_TOOLS,
                self_id="1",
            ),
            user_name="成员2",
        )
        return await original_complete(
            lease_id,
            outcome=outcome,
            unknown=unknown,
            **kwargs,
        )

    monkeypatch.setattr(adapter._dispatcher, "complete", complete_after_new_message)
    try:
        assert await adapter._dispatcher.notify("888")
        active = adapter._dispatcher.active("888")
        assert active is not None
        adapter._outbound_started.add(active.lease.lease_id)
        adapter._outbound_successful.add(active.lease.lease_id)
        await adapter._finish_queue_turn(
            SimpleNamespace(
                metadata={
                    "onebot11_lease_id": active.lease.lease_id,
                    "onebot11_lease_revision": active.lease.revision,
                    "onebot11_target": {"chat_type": "group", "chat_id": "888"},
                },
                media_urls=[],
            ),
            ProcessingOutcome.SUCCESS,
        )
        assert adapter._trigger_states["888"].mode == "debounce"
        assert adapter._queue.status("888")["pending"] == 1
    finally:
        await adapter.disconnect()


async def test_completion后普通消息进入engaged_debounce(monkeypatch):
    """Agent turn 期间收到的普通消息在成功收口后也应进入活跃窗口 debounce。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "test-provider",
                    "model": "test-model",
                    "groups": ["888"],
                },
            },
        )
    )
    adapter._llm_trigger_api_supported = True
    adapter._processing_reaction_enabled = False
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="complete-ordinary-1",
        user_id="123",
        user_name="成员",
        text="原始触发",
        message_key="group:complete-ordinary-1",
    )
    adapter._queue.enqueue(
        first,
        adapter_module.TriggerRequest.create(
            "888",
            "group:complete-ordinary-1",
            "mention",
            "123",
            "成员",
        ),
    )

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    original_complete = adapter._dispatcher.complete

    async def complete_after_new_message(
        lease_id: str,
        *,
        outcome: str,
        unknown: bool,
        **kwargs,
    ) -> bool:
        new_message = adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="complete-ordinary-2",
            user_id="456",
            user_name="成员2",
            text="普通闲聊",
            message_key="group:complete-ordinary-2",
        )
        await adapter._enqueue_group_message(
            new_message,
            mentioned_self=False,
            caller=adapter_module.CallerContext(
                user_id="456",
                chat_type="group",
                chat_id="888",
                role="user",
                allowed_tools=adapter_module.READ_ONLY_TOOLS,
                self_id="1",
            ),
            user_name="成员2",
        )
        return await original_complete(
            lease_id,
            outcome=outcome,
            unknown=unknown,
            **kwargs,
        )

    monkeypatch.setattr(adapter._dispatcher, "complete", complete_after_new_message)
    try:
        assert await adapter._dispatcher.notify("888")
        active = adapter._dispatcher.active("888")
        assert active is not None
        adapter._outbound_started.add(active.lease.lease_id)
        adapter._outbound_successful.add(active.lease.lease_id)
        await adapter._finish_queue_turn(
            SimpleNamespace(
                metadata={
                    "onebot11_lease_id": active.lease.lease_id,
                    "onebot11_lease_revision": active.lease.revision,
                    "onebot11_target": {"chat_type": "group", "chat_id": "888"},
                },
                media_urls=[],
            ),
            ProcessingOutcome.SUCCESS,
        )
        state = adapter._trigger_states["888"]
        assert state.mode == "debounce"
        assert state.debounce_due is not None
        assert adapter._queue.status("888")["pending"] == 1
    finally:
        await adapter.disconnect()


async def test_llm忽略后仍为engaged重新挂载活跃窗口定时器(monkeypatch):
    """旁路 ignore 不能让 engaged 状态失去到期计时。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "test-provider",
                    "model": "test-model",
                    "groups": ["888"],
                },
            },
        )
    )
    scheduled: list[str] = []
    monkeypatch.setattr(
        adapter,
        "_schedule_trigger_timer",
        lambda chat_id: scheduled.append(str(chat_id)),
    )
    adapter._queue.enqueue(
        adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="engaged-ignore-1",
            user_id="123",
            user_name="成员",
            text="这个怎么处理？",
            message_key="group:engaged-ignore-1",
        )
    )
    state = adapter._trigger_state_for("888")
    base_time = time.monotonic()
    state.on_turn_complete(success=True, now=base_time)
    state.observe_message(
        chat_type="group",
        text="这个怎么处理？",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=base_time + 1,
    )
    action = state.on_timer(now=base_time + 6)
    assert action.kind == "judge"

    try:
        async with adapter._trigger_lock_for("888"):
            result_action, notify, failure = await adapter._apply_llm_result_locked(
                "888",
                action,
                decision="ignore",
                wait_seconds=0,
                observed_revision=1,
            )
        assert result_action is not None
        assert result_action.reason == "llm_ignore"
        assert notify is False
        assert failure is None
        assert state.mode == "engaged"
        assert scheduled == ["888"]
    finally:
        await adapter.disconnect()


async def test_llm失败后仍为engaged重新挂载活跃窗口定时器(monkeypatch):
    """旁路失败恢复到 engaged 时同样不能丢失到期计时。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "llm_trigger": {
                    "enabled": True,
                    "provider": "test-provider",
                    "model": "test-model",
                    "groups": ["888"],
                },
            },
        )
    )
    scheduled: list[str] = []
    monkeypatch.setattr(
        adapter,
        "_schedule_trigger_timer",
        lambda chat_id: scheduled.append(str(chat_id)),
    )
    adapter._queue.enqueue(
        adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="engaged-failure-1",
            user_id="123",
            user_name="成员",
            text="这个怎么处理？",
            message_key="group:engaged-failure-1",
        )
    )
    state = adapter._trigger_state_for("888")
    base_time = time.monotonic()
    state.on_turn_complete(success=True, now=base_time)
    state.observe_message(
        chat_type="group",
        text="这个怎么处理？",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=base_time + 1,
    )
    action = state.on_timer(now=base_time + 6)
    assert action.kind == "judge"

    try:
        await adapter._apply_llm_failure("888", action, failure="timeout")
        assert state.mode == "engaged"
        assert scheduled == ["888"]
    finally:
        await adapter.disconnect()


async def test_deferred_completion不提前清理媒体(monkeypatch):
    """Hermes deferred turn 的媒体只在最终 queue 收尾时清理。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    media_dir = Path(adapter._new_media_dir())
    media_file = media_dir / "image.png"
    media_file.write_bytes(b"image")
    event = SimpleNamespace(
        metadata={
            "onebot11_lease_id": "deferred-lease",
            "onebot11_defer_completion": True,
            "onebot11_media_dir": str(media_dir),
            "onebot11_media_paths": [str(media_file)],
        },
        media_urls=[],
    )
    try:
        await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        assert media_file.exists()
        adapter._cleanup_media([str(media_file)], media_dir=str(media_dir))
        assert not media_dir.exists()
    finally:
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


async def test_standalone_cron只发送到明确允许的home群(monkeypatch):
    """live cron 必须使用明确的群目标，并重新应用 OneBot 访问策略。"""
    calls: list[tuple[str, str, str]] = []

    async def fake_send_message(
        _api,
        target_id: str,
        content: str,
        *,
        chat_type: str,
        reply_to: str | None = None,
    ) -> str:
        del reply_to
        calls.append((target_id, content, chat_type))
        return "cron-message-1"

    monkeypatch.setattr(adapter_module.OneBotHttpApi, "send_message", fake_send_message)
    result = await adapter_module._standalone_send(
        SimpleNamespace(
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "home_channel": "1072992996",
                "home_channel_type": "group",
                "allowed_groups": ["1072992996"],
            }
        ),
        "1072992996",
        "cron 内容",
    )
    assert result == {"success": True, "message_id": "cron-message-1"}
    assert calls == [("1072992996", "cron 内容", "group")]


async def test_standalone_cron缺少目标类型时拒绝且不出站(monkeypatch):
    """cron 不能根据目标号码形状猜测群或私聊。"""
    called = False

    async def fail_if_called(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("非法 cron 目标不应访问 OneBot")

    monkeypatch.setattr(adapter_module.OneBotHttpApi, "send_message", fail_if_called)
    result = await adapter_module._standalone_send(
        SimpleNamespace(
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "home_channel": "1072992996",
                "allowed_groups": ["1072992996"],
            }
        ),
        "1072992996",
        "cron 内容",
    )
    assert "home_channel_type" in result["error"]
    assert called is False


async def test_standalone_cron缺少message_id标记unknown(monkeypatch):
    """OneBot 成功响应缺少 message_id 时不能假报成功。"""
    async def empty_message_id(*_args, **_kwargs) -> str:
        return ""

    monkeypatch.setattr(adapter_module.OneBotHttpApi, "send_message", empty_message_id)
    result = await adapter_module._standalone_send(
        SimpleNamespace(
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "home_channel": "2056963663",
                "home_channel_type": "dm",
                "dm_policy": "allowlist",
                "allowed_users": ["2056963663"],
            }
        ),
        "2056963663",
        "cron 内容",
    )
    assert result["status"] == "unknown"
    assert "message_id" in result["error"]


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


async def test_Agent成功但没有出站仍释放队列lease(monkeypatch):
    """Hermes 报告成功不等于 OneBot 已发送；没有出站必须保留消息待重试。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="no-send",
        user_id="123",
        user_name="小明",
        text="没有回复",
        message_key="group:no-send",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888", "group:no-send", "mention", "123", "小明"
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    decision = adapter._queue_completion_decision(
        lease.lease_id, ProcessingOutcome.SUCCESS
    )
    assert decision[0:3] == (False, False, False)
    assert "没有成功出站" in (decision[3] or "")
    await adapter.disconnect()


async def test_收尾异常仍清理binding和媒体(monkeypatch):
    """reaction 收尾失败不能阻断 queue completion 或泄漏 turn 资源。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._processing_reaction_enabled = False
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="cleanup",
        user_id="123",
        user_name="小明",
        text="清理",
        message_key="group:cleanup",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888", "group:cleanup", "mention", "123", "小明"
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    assert adapter._queue.mark_outbound_started(lease)
    adapter._outbound_successful.add(lease.lease_id)
    media_dir = Path(adapter._new_media_dir())
    media_file = media_dir / "image.png"
    media_file.write_bytes(b"image")
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id=lease.lease_id,
        self_id="1",
    )
    binding = adapter_module.TurnBinding("session-cleanup", "turn-cleanup", caller, lease.lease_id)
    adapter._bindings.bind(binding)

    async def fail_reaction(_lease_id: str) -> None:
        raise RuntimeError("reaction cleanup failed")

    monkeypatch.setattr(adapter, "_clear_processing_reaction", fail_reaction)
    event = SimpleNamespace(
        metadata={
            "onebot11_lease_id": lease.lease_id,
            "onebot11_media_dir": str(media_dir),
            "onebot11_media_paths": [str(media_file)],
        },
        media_urls=[],
    )
    await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
    assert adapter._queue.status("888")["pending"] == 0
    assert adapter._bindings.get("session-cleanup", "turn-cleanup") is None
    assert not media_dir.exists()
    await adapter.disconnect()


async def test_触发状态更新异常仍清理turn资源(monkeypatch):
    """队列完成后触发状态或下一轮通知失败，也不能泄漏 binding 和媒体。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter.trigger_config = replace(
        adapter.trigger_config,
        llm_enabled=True,
        llm_allowed_groups=frozenset({"888"}),
    )
    message = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="cleanup-trigger-error",
        user_id="123",
        user_name="小明",
        text="触发状态异常",
        message_key="group:cleanup-trigger-error",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888", "group:cleanup-trigger-error", "mention", "123", "小明"
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    assert adapter._queue.mark_outbound_started(lease)
    adapter._outbound_successful.add(lease.lease_id)
    adapter._dispatcher._active["888"] = ActiveTurn(
        lease=lease,
        started_at=lease.claimed_at,
    )
    media_dir = Path(adapter._new_media_dir())
    media_file = media_dir / "image.png"
    media_file.write_bytes(b"image")
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id=lease.lease_id,
        self_id="1",
    )
    binding = adapter_module.TurnBinding(
        "session-trigger-error",
        "turn-trigger-error",
        caller,
        lease.lease_id,
    )
    adapter._bindings.bind(binding)

    def fail_status(_chat_id: str) -> dict:
        raise RuntimeError("trigger status failed")

    monkeypatch.setattr(adapter._queue, "status", fail_status)
    event = SimpleNamespace(
        metadata={
            "onebot11_lease_id": lease.lease_id,
            "onebot11_lease_revision": lease.revision,
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
            "onebot11_media_dir": str(media_dir),
            "onebot11_media_paths": [str(media_file)],
        },
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert adapter._bindings.get("session-trigger-error", "turn-trigger-error") is None
        assert not media_dir.exists()
    finally:
        await adapter.disconnect()


async def test_ack后触发状态异常为新pending消息补durable_recovery_trigger(monkeypatch):
    """ack 已完成但后续状态更新失败时，普通新消息不能失去自动处理入口。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter.trigger_config = replace(
        adapter.trigger_config,
        llm_enabled=True,
        llm_allowed_groups=frozenset({"888"}),
    )
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="completion-recovery-1",
        user_id="123",
        user_name="小明",
        text="原始触发",
        message_key="group:completion-recovery-1",
    )
    second = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="completion-recovery-2",
        user_id="456",
        user_name="小红",
        text="ack 前抵达的普通消息",
        message_key="group:completion-recovery-2",
    )
    adapter._queue.enqueue(
        first,
        adapter_module.TriggerRequest.create(
            "888",
            "group:completion-recovery-1",
            "mention",
            "123",
            "小明",
        ),
    )
    adapter._processing_reaction_enabled = False
    lease = adapter._queue.claim("888")
    assert lease is not None
    adapter._dispatcher._active["888"] = ActiveTurn(
        lease=lease,
        started_at=lease.claimed_at,
    )
    adapter._outbound_started.add(lease.lease_id)
    adapter._outbound_successful.add(lease.lease_id)
    adapter._queue.enqueue(second)
    assert adapter._queue.status("888")["pending"] == 1

    class BrokenTriggerState:
        """只模拟 completion 后状态投影失败，不影响队列本身。"""

        mode = "idle"
        debounce_due = None
        wait_until = None
        engaged_until = None

        def on_turn_complete(self, **_kwargs: object) -> None:
            """抛出投影异常，触发持久 recovery 路径。"""
            raise RuntimeError("trigger state projection failed")

    monkeypatch.setattr(adapter, "_trigger_state_for", lambda _chat_id: BrokenTriggerState())
    async def suppress_recovery_dispatch(_chat_id: str) -> bool:
        """只验证 durable trigger 已落库，不在测试中启动第二个 turn。"""
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", suppress_recovery_dispatch)
    event = SimpleNamespace(
        metadata={
            "onebot11_lease_id": lease.lease_id,
            "onebot11_lease_revision": lease.revision,
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
        },
        media_urls=[],
    )
    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        status = adapter._queue.status("888")
        assert status["pending"] == 1
        assert status["pending_trigger_requests"] == 1
        assert adapter._queue.peek("888")[0].message_id == "completion-recovery-2"
    finally:
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
        {
            "user_id": "123",
            "chat_type": "group",
            "chat_id": "999",
            "lease_id": lease.lease_id,
            "self_id": "1",
        }
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


async def test_resolve_action只解除管理动作阻断不直接重放(monkeypatch):
    """operation retry 只 armed，后续仍需新的预览和确认。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    started = adapter._queue.start_operation(
        fingerprint="operation-fingerprint",
        tool_name="qq_set_group_ban",
        chat_type="group",
        chat_id="888",
        caller_user_id="123",
        params={"user_id": "456", "duration": 60},
    )
    assert started.started
    assert adapter._queue.finish_operation(
        started.operation.operation_id,
        "unknown",
        reason="网络响应未知",
    )
    responses: list[str] = []

    async def fake_send_direct(_event, text: str) -> None:
        responses.append(text)

    monkeypatch.setattr(adapter, "_send_direct", fake_send_direct)
    await adapter._handle_admin_command(
        InboundEvent(
            text=f"/onebot resolve action retry {started.operation.operation_id}",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="管理员",
            message_id="resolve-action",
        )
    )
    assert responses and "重新让 Hermes 生成预览" in responses[0]
    record = adapter._queue.operation_records("888")[0]
    assert record.status == "retry_armed"
    assert adapter._queue.unknown_operation_count("888") == 0
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


def test_validate_config与构造函数共享数值合同(monkeypatch):
    """validate_config 不能接受构造函数会拒绝的 queue 数值。"""
    monkeypatch.delenv("ONEBOT11_HTTP_API", raising=False)
    monkeypatch.delenv("ONEBOT11_SELF_ID", raising=False)
    config = SimpleNamespace(
        extra={
            "http_api": "http://127.0.0.1:3000",
            "self_id": "1",
            "queue_lease_seconds": "not-a-number",
        }
    )
    assert not validate_config(config)
    with pytest.raises(ValueError):
        OneBot11Adapter(PlatformConfig(enabled=True, extra=config.extra))


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
    }
    for t in ctx.tools:
        assert t["toolset"] == "onebot11"
        assert t["is_async"] is True
