"""adapter.py 冒烟测试。

需要 hermes gateway 可导入（本地跑：用 hermes venv + PYTHONPATH）；
CI 环境没有 gateway 时自动跳过。
"""

import asyncio
import base64
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
from gateway.platforms.base import (  # noqa: E402
    BasePlatformAdapter,
    MessageEvent,
    ProcessingOutcome,
    SendResult,
)
from gateway.stream_events import ToolCallChunk  # noqa: E402

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


async def test_policy_reload原子替换权限并清理旧确认令牌(monkeypatch):
    """reload 只替换热策略，旧确认令牌不能跨配置继续执行。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ALLOWED_GROUPS="888",
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    old_caller = adapter._caller_for_event(
        SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
    )
    old_metadata = adapter_module._serializable_caller(old_caller)
    confirmation = adapter._confirmations.issue(
        "qq_set_group_whole_ban",
        {"enable": True},
        user_id="1",
        chat_type="group",
        chat_id="888",
    )
    candidate = dict(adapter._runtime_config.extra)
    candidate.update(
        {
            "allowed_groups": ["999"],
            "long_running_notice_seconds": 12,
            "roles": {
                "user": {"tools": []},
                "trusted_user": {"tools": []},
                "super_admin": {"tools": ["qq_get_message"]},
            },
        }
    )
    # 环境变量是 YAML 的覆盖层；这里清掉构造 adapter 时注入的部署值，
    # 才能单独验证 Hermes 配置 reload 的热更新结果。
    monkeypatch.delenv("ONEBOT11_ALLOWED_GROUPS", raising=False)
    monkeypatch.setattr(adapter, "_load_reload_extra", lambda: candidate)

    success, message = await adapter.reload_policy()

    assert success is True
    assert "version=2" in message
    assert adapter.policy_snapshot.version == 2
    assert adapter.allowed_groups == {"999"}
    assert adapter.policy_snapshot.long_running_notice_seconds == 12
    assert adapter.role_tools["user"] == frozenset()
    assert adapter._confirmations.consume_any(confirmation.token) is None
    # 已创建的 turn 继续使用创建时的工具快照，但目标白名单仍由当前策略校验。
    assert adapter_module._caller_from_metadata(old_metadata) is None

    candidate["allowed_groups"] = ["888"]
    monkeypatch.setattr(adapter, "_load_reload_extra", lambda: candidate)
    success, _message = await adapter.reload_policy()
    assert success is True
    restored = adapter_module._caller_from_metadata(old_metadata)
    assert restored is not None
    assert restored.allowed_tools == old_caller.allowed_tools
    await adapter.disconnect()


async def test_自定义roles路径纳入配置签名(monkeypatch, tmp_path):
    """roles_file 使用自定义路径时，签名必须监视该文件而不是默认位置。"""
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text("gateway: {}\n", encoding="utf-8")
    roles_path = tmp_path / "custom-roles.yaml"
    roles_path.write_text(
        "super_admins: ['1']\nroles: {}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("ONEBOT11_ROLES_FILE", str(roles_path))
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    try:
        first = adapter._policy_config_signature()
        assert first is not None
        roles_path.write_text(
            "super_admins: ['2']\nroles: {}\n",
            encoding="utf-8",
        )
        second = adapter._policy_config_signature()
        assert second is not None
        assert second != first
    finally:
        await adapter.disconnect()


async def test_policy_reload拒绝静态连接配置变化(monkeypatch):
    """HTTP/队列等静态字段变化必须明确要求重启。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    candidate = dict(adapter._runtime_config.extra)
    candidate["http_api"] = "http://127.0.0.1:4000"
    # 静态字段变化测试应来自配置文件，而不是被环境覆盖层遮蔽。
    monkeypatch.delenv("ONEBOT11_HTTP_API", raising=False)
    monkeypatch.setattr(adapter, "_load_reload_extra", lambda: candidate)

    success, message = await adapter.reload_policy()

    assert success is False
    assert "需要重启" in message
    assert adapter.policy_snapshot.version == 1
    assert adapter._policy_reload_error is not None
    await adapter.disconnect()


def test_reload支持Hermes_gateway的PlatformConfig结果():
    """运行时 reload 读取 Hermes 合并后的 GatewayConfig，而非猜 YAML 结构。"""
    gateway_config = SimpleNamespace(
        platforms={
            "onebot11": SimpleNamespace(
                extra={"allowed_groups": ["1072992996"], "plain_text_enabled": True}
            )
        }
    )
    assert adapter_module._extract_onebot_extra(gateway_config) == {
        "allowed_groups": ["1072992996"],
        "plain_text_enabled": True,
    }


async def test_reconnect使旧DM身份快照失效(monkeypatch, tmp_path):
    """同一 adapter 重连后，旧 DM task 不能重新建立权限绑定。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_DM_POLICY="allowlist",
        ONEBOT11_ALLOWED_USERS="123",
        ONEBOT11_QUEUE_DB=str(tmp_path / "queue.sqlite3"),
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    old_caller = adapter._caller_for_event(
        SimpleNamespace(user_id="123", chat_type="dm", chat_id="123")
    )
    old_metadata = adapter_module._serializable_caller(old_caller)
    await adapter.connect()
    await adapter.disconnect()
    await adapter.connect(is_reconnect=True)
    try:
        assert adapter_module._caller_from_metadata(old_metadata) is None
        new_caller = adapter._caller_for_event(
            SimpleNamespace(user_id="123", chat_type="dm", chat_id="123")
        )
        assert adapter_module._caller_from_metadata(
            adapter_module._serializable_caller(new_caller)
        ) is not None
    finally:
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
        result = await adapter._send_with_retry(
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


async def test_真实工具handler缺少turn_id时fail_closed(monkeypatch):
    """Hermes registry 缺少 turn_id 时，handler 不猜测当前 task 身份。"""
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
        assert result == {
            "status": "permission_error",
            "error": "当前 turn 身份绑定不存在",
        }
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


async def test_worker线程建立binding后async最终出站可恢复精确binding(monkeypatch):
    """Hermes worker hook 与 async final delivery 不共享 ContextVar 时仍可安全出站。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    event = await adapter._build_message_event(
        InboundEvent(
            text="最终回复",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="小明",
            message_id="1001",
        )
    )
    caller = adapter._caller_for_event(event.source)
    event.metadata["onebot11_managed_context"] = True
    event.metadata["onebot11_caller_context"] = adapter_module._serializable_caller(caller)
    calls: list[tuple[str, str]] = []

    async def fake_send_message(
        _target_id: str,
        content: str,
        *,
        chat_type: str,
        reply_to: str | None = None,
    ) -> str:
        del reply_to
        calls.append((content, chat_type))
        return "reply-1"

    monkeypatch.setattr(adapter._api, "send_message", fake_send_message)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    adapter_module._pre_gateway_dispatch_hook(event)

    # pre_llm_call 可能在 Hermes 的 worker thread 执行；async final
    # delivery 回到原 event loop 后，不能依赖 worker 的 ContextVar。
    await asyncio.to_thread(
        adapter_module._pre_llm_call_hook,
        session_id="session-1",
        turn_id="turn-1",
        platform="onebot11",
    )
    assert event.metadata["onebot11_binding_key"] == {
        "session_id": "session-1",
        "turn_id": "turn-1",
    }
    adapter_module._CURRENT_BINDING.set(None)

    try:
        result = await adapter._send_with_retry(
            "888",
            "最终回复",
            # Hermes final delivery 只传平台通用 metadata，不会复制
            # synthetic event 的 OneBot binding 字段。
            metadata={"notify": True},
        )
        assert result.success
        assert calls == [("最终回复", "group")]
    finally:
        adapter_module._CURRENT_EVENT.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        adapter_module._CURRENT_BINDING.set(None)
        await adapter.disconnect()


async def test_worker_binding丢失event_metadata时仍按当前活动lease恢复出站(monkeypatch):
    """Hermes status/interim callback 只带 chat_id 时仍恢复唯一活动 turn。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    adapter.show_interim_group = True
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="复杂任务",
        message_key="group:1001",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "123",
            "小明",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    adapter._dispatcher._active["888"] = ActiveTurn(
        lease=lease,
        started_at=lease.claimed_at,
    )
    caller = replace(
        adapter._caller_for_event(
            SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
        ),
        lease_id=lease.lease_id,
    )
    binding = adapter_module.TurnBinding(
        "worker-session",
        "worker-turn",
        caller,
        lease.lease_id,
    )
    adapter._bindings.bind(binding)
    calls: list[str] = []

    async def fake_send_message(
        _target_id: str,
        content: str,
        *,
        chat_type: str,
        reply_to: str | None = None,
    ) -> str:
        del chat_type, reply_to
        calls.append(content)
        return "reply-lease-fallback"

    monkeypatch.setattr(adapter._api, "send_message", fake_send_message)
    # 复现 Hermes 的 status/interim callback：它回到 gateway loop 时没有
    # worker 的 ContextVar，只保留 synthetic event 的 lease/目标 lineage。
    event = SimpleNamespace(
        metadata={
            "onebot11_managed_context": True,
            "onebot11_lease_id": lease.lease_id,
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
        }
    )
    event_token = adapter_module._CURRENT_EVENT.set(event)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    onebot_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(False)
    try:
        result = await adapter.send(
            "888",
            "中途进度",
            metadata={
                "onebot11_target": {"chat_type": "group", "chat_id": "888"},
            },
        )
        assert result.success
        assert calls == ["中途进度"]
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(onebot_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
        adapter._dispatcher._active.pop("888", None)
        await adapter.disconnect()


def test_delegated_child父lease结束后仍可执行项目工具但不能越权(monkeypatch):
    """后台子代理不应被父 QQ lease 的正常收尾误杀。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = replace(
        adapter._caller_for_event(
            SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
        ),
        role="super_admin",
        lease_id="parent-lease",
    )
    binding = adapter_module.TurnBinding(
        "parent-session",
        "parent-turn",
        caller,
        "parent-lease",
    )
    adapter._bindings.bind(binding)
    # 模拟父 turn wrapper 收尾：child 仍持有 ContextVar lineage，但 store 已清理。
    adapter._bindings.discard_if_matches(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    monkeypatch.setattr(adapter_module, "_is_delegated_child_turn", lambda _kwargs: True)
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: False)
    monkeypatch.setattr(adapter, "_chat_access_allowed", lambda *_args: True)

    binding_token = adapter_module._CURRENT_BINDING.set(binding)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        assert adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="child-session",
            turn_id="child-turn",
            args={"command": "pwd"},
        ) is None
        blocked_qq = adapter_module._pre_tool_call_hook(
            tool_name="qq_get_message",
            session_id="child-session",
            turn_id="child-turn",
            args={},
        )
        assert blocked_qq is not None
        assert blocked_qq["action"] == "block"
        blocked_send = adapter_module._pre_tool_call_hook(
            tool_name="send_message",
            session_id="child-session",
            turn_id="child-turn",
            args={},
        )
        assert blocked_send is not None
        assert blocked_send["action"] == "block"
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(context_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter._bindings.clear()
        asyncio.run(adapter.disconnect())


def test_delegated_child_pre_llm跳过父租约但保留父lineage(monkeypatch):
    """父 lease 和 binding store 清理后，child 仍须验证继承的父身份。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = replace(
        adapter._caller_for_event(
            SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
        ),
        role="super_admin",
        lease_id="parent-lease",
    )
    binding = adapter_module.TurnBinding(
        "parent-session",
        "parent-turn",
        caller,
        "parent-lease",
    )
    adapter._bindings.bind(binding)
    adapter._bindings.discard_if_matches(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    monkeypatch.setattr(adapter_module, "_is_delegated_child_turn", lambda _kwargs: True)
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: False)
    monkeypatch.setattr(adapter, "_chat_access_allowed", lambda *_args: True)
    binding_token = adapter_module._CURRENT_BINDING.set(binding)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        result = adapter_module._pre_llm_call_hook(
            session_id="child-session",
            turn_id="child-turn",
            platform="subagent",
        )
        assert result is not None
        assert adapter_module._CURRENT_BINDING.get() == binding
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(context_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter._bindings.clear()
        asyncio.run(adapter.disconnect())


@pytest.mark.parametrize("invalid_identity", ["self_id", "adapter_epoch", "access"])
def test_delegated_child身份变化后fail_closed(monkeypatch, invalid_identity):
    """self_id、epoch 或白名单变化不能让旧 child 继续获得权限。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = replace(
        adapter._caller_for_event(
            SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
        ),
        role="super_admin",
        lease_id="parent-lease",
    )
    if invalid_identity == "self_id":
        caller = replace(caller, self_id="other-bot")
    elif invalid_identity == "adapter_epoch":
        caller = replace(caller, adapter_epoch=adapter._adapter_epoch + 1)
    else:
        monkeypatch.setattr(adapter, "_chat_access_allowed", lambda *_args: False)
    binding = adapter_module.TurnBinding(
        "parent-session",
        "parent-turn",
        caller,
        "parent-lease",
    )
    adapter._bindings.bind(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    monkeypatch.setattr(adapter_module, "_is_delegated_child_turn", lambda _kwargs: True)
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: False)
    binding_token = adapter_module._CURRENT_BINDING.set(binding)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        blocked = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="child-session",
            turn_id="child-turn",
            args={"command": "pwd"},
        )
        assert blocked is not None
        assert blocked["action"] == "block"
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(context_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter._bindings.clear()
        asyncio.run(adapter.disconnect())


def test_delegated_child不能借用其他群binding(monkeypatch):
    """child 坐标命中另一个群的 binding 时必须拒绝跨群串用。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    parent_caller = replace(
        adapter._caller_for_event(
            SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
        ),
        role="super_admin",
        lease_id="parent-lease",
    )
    other_caller = replace(parent_caller, chat_id="999", lease_id="other-lease")
    parent_binding = adapter_module.TurnBinding(
        "parent-session",
        "parent-turn",
        parent_caller,
        "parent-lease",
    )
    other_binding = adapter_module.TurnBinding(
        "child-session",
        "child-turn",
        other_caller,
        "other-lease",
    )
    adapter._bindings.bind(parent_binding)
    adapter._bindings.bind(other_binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    monkeypatch.setattr(adapter_module, "_is_delegated_child_turn", lambda _kwargs: True)
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: False)
    monkeypatch.setattr(adapter, "_chat_access_allowed", lambda *_args: True)
    binding_token = adapter_module._CURRENT_BINDING.set(parent_binding)
    caller_token = adapter_module._CURRENT_CALLER.set(parent_caller)
    context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        blocked = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="child-session",
            turn_id="child-turn",
            args={"command": "pwd"},
        )
        assert blocked is not None
        assert blocked["action"] == "block"
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(context_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter._bindings.clear()
        asyncio.run(adapter.disconnect())


async def test出站binding缺失或session_turn不匹配时不访问OneBot(monkeypatch):
    """managed turn 没有精确 binding 时必须 fail-closed。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    calls: list[str] = []

    async def fail_if_called(*_args, **_kwargs):
        calls.append("called")
        raise AssertionError("binding 无效时不能访问 OneBot")

    monkeypatch.setattr(adapter._api, "send_message", fail_if_called)
    event = SimpleNamespace(
        metadata={
            "onebot11_managed_context": True,
            "onebot11_binding_key": {
                "session_id": "missing-session",
                "turn_id": "missing-turn",
            },
            "onebot11_lease_id": "missing-lease",
        }
    )
    event_token = adapter_module._CURRENT_EVENT.set(event)
    try:
        result = await adapter._send_with_retry("888", "不能发送", metadata=event.metadata)
        assert not result.success
        assert result.error_kind == "fenced"
        assert calls == []
    finally:
        adapter_module._CURRENT_EVENT.reset(event_token)
        await adapter.disconnect()


@pytest.mark.parametrize("invalid_field", ["self_id", "adapter_epoch", "lease"])
async def test_metadata_binding身份epoch或lease失效时不访问OneBot(
    monkeypatch,
    invalid_field: str,
):
    """跨线程恢复不能绕过 bot 身份、adapter epoch 或 lease fencing。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    async def fake_stop() -> None:
        return None

    adapter._ws = SimpleNamespace(stop=fake_stop)
    adapter._chat_types["888"] = "group"
    calls: list[str] = []

    async def fail_if_called(*_args, **_kwargs):
        calls.append("called")
        raise AssertionError("失效 binding 不能访问 OneBot")

    monkeypatch.setattr(adapter._api, "send_message", fail_if_called)
    caller_kwargs: dict[str, object] = {
        "user_id": "123",
        "chat_type": "group",
        "chat_id": "888",
        "role": "user",
        "allowed_tools": adapter_module.READ_ONLY_TOOLS,
        "self_id": "1",
        "adapter_epoch": adapter._adapter_epoch,
    }
    if invalid_field == "self_id":
        caller_kwargs["self_id"] = "999"
    elif invalid_field == "adapter_epoch":
        caller_kwargs["adapter_epoch"] = adapter._adapter_epoch + 1
    else:
        caller_kwargs["lease_id"] = "lease-invalid"
    caller = adapter_module.CallerContext(**caller_kwargs)
    binding = adapter_module.TurnBinding(
        "session-invalid",
        "turn-invalid",
        caller,
        caller.lease_id,
    )
    adapter._bindings.bind(binding)
    event = SimpleNamespace(
        metadata={
            "onebot11_managed_context": True,
            "onebot11_caller_context": adapter_module._serializable_caller(caller),
            "onebot11_binding_key": {
                "session_id": "session-invalid",
                "turn_id": "turn-invalid",
            },
            "onebot11_lease_id": caller.lease_id,
        }
    )
    event_token = adapter_module._CURRENT_EVENT.set(event)
    try:
        result = await adapter._send_with_retry("888", "不能发送", metadata={"notify": True})
        assert not result.success
        assert result.error_kind == "fenced"
        assert calls == []
    finally:
        adapter_module._CURRENT_EVENT.reset(event_token)
        await adapter.disconnect()


async def test文本图片和多图片出站复用同一metadata_binding(monkeypatch):
    """文本、单图和多图入口都必须使用同一条精确 turn binding。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    async def fake_stop() -> None:
        return None

    adapter._ws = SimpleNamespace(stop=fake_stop)
    adapter._chat_types["888"] = "group"
    event = SimpleNamespace(
        message_id="1001",
        metadata={"onebot11_managed_context": True},
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    binding = adapter_module.TurnBinding("session-media", "turn-media", caller)
    adapter._bindings.bind(binding)
    event.metadata.update(
        {
            "onebot11_caller_context": adapter_module._serializable_caller(caller),
            "onebot11_binding_key": {
                "session_id": "session-media",
                "turn_id": "turn-media",
            },
        }
    )
    image_payload = b"\x89PNG\r\n\x1a\nremote-image"
    calls: list[dict] = []

    async def fake_segments(
        target_id: str,
        segments: list[dict],
        *,
        chat_type: str,
    ) -> str:
        calls.append(
            {
                "target_id": target_id,
                "segments": segments,
                "chat_type": chat_type,
            }
        )
        return f"image-{len(calls)}"

    async def fake_download(_url: str, media_dir: str) -> str:
        path = Path(media_dir) / "downloaded.png"
        path.write_bytes(image_payload)
        return str(path)

    monkeypatch.setattr(adapter._api, "send_message_segments", fake_segments)
    monkeypatch.setattr(adapter._api, "download_to_temp", fake_download)
    first = Path(adapter._media_root) / "binding-first.png"
    second = Path(adapter._media_root) / "binding-second.png"
    first.write_bytes(b"\x89PNG\r\n\x1a\nfirst-image")
    second.write_bytes(b"\x89PNG\r\n\x1a\nsecond-image")
    event_token = adapter_module._CURRENT_EVENT.set(event)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    try:
        single = await adapter.send_image(
            "888",
            "https://example.invalid/image.png",
            metadata=event.metadata,
        )
        multiple = await adapter.send_multiple_images(
            "888",
            [(f"file://{first}", ""), (f"file://{second}", "")],
            metadata=event.metadata,
        )
        assert single.success
        assert [result.success for result in multiple] == [True, True]
        assert len(calls) == 3
    finally:
        first.unlink(missing_ok=True)
        second.unlink(missing_ok=True)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
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


async def test_群会话命令在入队前交给Hermes公共入口(monkeypatch):
    """授权的 /new 不生成 OneBot queue message，且不带群昵称前缀。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    captured: list[MessageEvent] = []

    async def fake_prepare(_chat_id: str, _event: InboundEvent) -> bool:
        return True

    async def fake_handle_message(_adapter, event: MessageEvent) -> None:
        captured.append(event)

    monkeypatch.setattr(adapter, "_prepare_conversation_reset", fake_prepare)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    await adapter._on_ws_event(_group_raw(888, "/new 新会话", at_self=False))

    assert adapter._queue.status("888")["pending"] == 0
    assert len(captured) == 1
    assert captured[0].text == "/new 新会话"
    assert captured[0].source.chat_type == "group"
    assert captured[0].source.chat_id == "888"
    assert captured[0].metadata["onebot11_conversation_command"] == "new"
    assert not captured[0].metadata.get("onebot11_managed_context")
    await adapter.disconnect()


async def test_非超级管理员会话命令不进入Hermes或队列(monkeypatch):
    """普通群成员不能借助 /new 清除队列或触发 Hermes。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="999",
    )
    direct_replies: list[str] = []
    captured: list[MessageEvent] = []

    async def fake_direct(_event: InboundEvent, text: str) -> None:
        direct_replies.append(text)

    async def fake_handle_message(_adapter, event: MessageEvent) -> None:
        captured.append(event)

    monkeypatch.setattr(adapter, "_send_direct", fake_direct)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    await adapter._on_ws_event(_group_raw(888, "/new", at_self=False))

    assert adapter._queue.status("888")["pending"] == 0
    assert captured == []
    assert direct_replies == ["仅超级管理员可执行群级会话命令"]
    await adapter.disconnect()


async def test_reset未能收口不会推进generation(monkeypatch):
    """被活动 lease 阻断的 reset 不得让当前 turn 变成旧 generation。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    adapter._conversation_reset_generations["888"] = 4

    async def reject_prepare(_chat_id: str, _event: InboundEvent) -> bool:
        return False

    direct_replies: list[str] = []

    async def capture_direct(_event: InboundEvent, text: str) -> None:
        direct_replies.append(text)

    monkeypatch.setattr(adapter, "_prepare_conversation_reset", reject_prepare)
    monkeypatch.setattr(adapter, "_send_direct", capture_direct)
    try:
        await adapter._on_ws_event(_group_raw(888, "/reset", at_self=False))
        assert adapter._conversation_reset_generations["888"] == 4
        assert direct_replies == ["当前群有未能安全收口的 turn，未执行会话重置；请稍后重试。"]
    finally:
        await adapter.disconnect()


async def test_群clear桥接为Hermes公共new并在reset_hook后清空队列(monkeypatch):
    """OneBot /clear 使用 Hermes 公共 /new，并在 reset hook 后清理队列。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    adapter._queue.enqueue(
        QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="queued-before-clear",
            user_id="456",
            user_name="小红",
            text="旧消息",
            message_key="group:queued-before-clear",
        )
    )
    captured: list[MessageEvent] = []

    async def fake_prepare(_chat_id: str, _event: InboundEvent) -> bool:
        return True

    async def fake_handle_message(_adapter, event: MessageEvent) -> None:
        captured.append(event)

    monkeypatch.setattr(adapter, "_prepare_conversation_reset", fake_prepare)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    await adapter._on_ws_event(_group_raw(888, "/clear", at_self=False))
    adapter._on_session_reset_hook(
        platform="onebot11",
        old_session_id=None,
        new_session_id="new-session",
    )
    await asyncio.sleep(0.05)

    assert captured[0].text == "/new"
    assert adapter._queue.status("888")["pending"] == 0
    assert "888" not in adapter._resetting_groups
    await adapter.disconnect()


async def test_reset期间普通消息不会被误清理(monkeypatch):
    """reset 尚未完成时，新消息得到明确提示而不是进入待清理队列。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    captured: list[MessageEvent] = []
    direct_replies: list[str] = []

    async def fake_handle_message(_adapter, event: MessageEvent) -> None:
        captured.append(event)

    async def fake_prepare(_chat_id: str, _event: InboundEvent) -> bool:
        return True

    async def fake_direct(_event: InboundEvent, text: str) -> None:
        direct_replies.append(text)

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    monkeypatch.setattr(adapter, "_prepare_conversation_reset", fake_prepare)
    monkeypatch.setattr(adapter, "_send_direct", fake_direct)

    await adapter._on_ws_event(_group_raw(888, "/new", at_self=False))
    await adapter._on_ws_event(_group_raw(888, "重置期间的新消息", at_self=False))

    assert len(captured) == 1
    assert direct_replies == ["当前群正在重置会话，请稍后重新发送这条消息。"]
    assert adapter._queue.status("888")["pending"] == 0
    await adapter.disconnect()


async def test_reset_hook使用当前命令上下文区分多个群(monkeypatch):
    """没有 session id 时也只能按当前命令上下文精确清理对应群。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    for chat_id in ("888", "889"):
        message = QueueMessage(
            chat_id=chat_id,
            chat_type="group",
            message_id=f"before-{chat_id}",
            user_id="456",
            user_name="小红",
            text=f"{chat_id} 旧消息",
            message_key=f"group:before-{chat_id}",
        )
        adapter._queue.enqueue(message)

    captured: list[MessageEvent] = []

    async def fake_prepare(_chat_id: str, _event: InboundEvent) -> bool:
        return True

    async def fake_handle_message(_adapter, event: MessageEvent) -> None:
        captured.append(event)

    monkeypatch.setattr(adapter, "_prepare_conversation_reset", fake_prepare)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    await asyncio.gather(
        adapter._on_ws_event(_group_raw(888, "/new", at_self=False)),
        adapter._on_ws_event(_group_raw(889, "/new", at_self=False)),
    )

    assert len(captured) == 2
    for event, chat_id in zip(captured, ("888", "889"), strict=True):
        token = adapter_module._CURRENT_EVENT.set(event)
        try:
            adapter._on_session_reset_hook(platform="onebot11")
        finally:
            adapter_module._CURRENT_EVENT.reset(token)
        await asyncio.sleep(0.02)
        assert adapter._queue.status(chat_id)["pending"] == 0

    assert adapter._resetting_groups == set()
    await adapter.disconnect()


async def test_reset_hook缺少身份时fail_closed(monkeypatch):
    """缺少 session identity 时不能猜测群并清理队列。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="missing-reset-identity",
        user_id="456",
        user_name="小红",
        text="旧消息",
        message_key="group:missing-reset-identity",
    )
    adapter._queue.enqueue(message)

    async def fake_prepare(_chat_id: str, _event: InboundEvent) -> bool:
        return True

    async def fake_handle_message(_adapter, _event: MessageEvent) -> None:
        return None

    monkeypatch.setattr(adapter, "_prepare_conversation_reset", fake_prepare)
    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    await adapter._on_ws_event(_group_raw(888, "/new", at_self=False))
    adapter._on_session_reset_hook(platform="onebot11")
    await asyncio.sleep(0.02)

    assert adapter._queue.status("888")["pending"] == 1
    assert "888" in adapter._resetting_groups
    await adapter.disconnect()


async def test_fenced旧turn不能在reset后重新进入engaged(monkeypatch):
    """旧 task 的 completion 只能收口 lease，不能污染新会话触发状态。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="fenced-old-turn",
        user_id="123",
        user_name="小明",
        text="旧 turn",
        message_key="group:fenced-old-turn",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            "group:fenced-old-turn",
            "mention",
            "123",
            "小明",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    adapter._dispatcher._active["888"] = ActiveTurn(
        lease=lease,
        started_at=time.time(),
    )
    adapter._fenced_leases.add(lease.lease_id)
    adapter._queue.mark_outbound_started(lease.lease_id)
    adapter._outbound_started.add(lease.lease_id)
    adapter._outbound_successful.add(lease.lease_id)

    source = adapter.build_source(
        chat_id="888",
        chat_name="888",
        chat_type="group",
        user_id="123",
        user_name="小明",
        message_id="fenced-old-turn",
        role_authorized=True,
    )
    event = MessageEvent(
        text="旧 turn",
        message_type="text",
        source=source,
        metadata={
            "onebot11_lease_id": lease.lease_id,
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
            "onebot11_managed_context": True,
        },
    )

    await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)

    state = adapter._trigger_states.get("888")
    assert state is None or state.mode == "idle"
    await adapter.disconnect()


@pytest.mark.parametrize(
    ("stale_field", "stale_value"),
    [
        ("onebot11_adapter_epoch", -1),
        ("onebot11_reset_generation", 1),
    ],
)
async def test_late_completion不会回写新runtime触发状态(
    monkeypatch,
    stale_field: str,
    stale_value: int,
):
    """旧 epoch 或旧 reset generation 的 late task 不能创建恢复入口。"""
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
    adapter._conversation_reset_generations["888"] = 2
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id=f"late-{stale_field}",
        user_id="123",
        user_name="小明",
        text="旧 runtime",
        message_key=f"group:late-{stale_field}",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "123",
            "小明",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    adapter._dispatcher._active["888"] = ActiveTurn(
        lease=lease,
        started_at=time.time(),
    )
    assert adapter._queue.mark_outbound_started(lease)
    adapter._outbound_started.add(lease.lease_id)
    adapter._outbound_successful.add(lease.lease_id)
    recovery_calls: list[str] = []

    async def record_recovery(chat_id: str) -> bool:
        recovery_calls.append(chat_id)
        return True

    monkeypatch.setattr(adapter, "_ensure_completion_recovery_trigger", record_recovery)
    metadata = {
        "onebot11_lease_id": lease.lease_id,
        "onebot11_lease_revision": lease.revision,
        "onebot11_target": {"chat_type": "group", "chat_id": "888"},
        "onebot11_adapter_epoch": adapter._adapter_epoch,
        "onebot11_reset_generation": 2,
        "onebot11_managed_context": True,
    }
    metadata[stale_field] = stale_value
    event = SimpleNamespace(metadata=metadata, media_urls=[])

    try:
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert recovery_calls == []
        assert adapter._trigger_states.get("888") is None
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
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
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
        adapter_module.TriggerRequest.create(
            "888",
            "group:-1001",
            "mention",
            "123",
            "小明",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
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
        reaction_record = adapter._queue.reaction_for_lease(active.lease.lease_id)
        assert reaction_record is not None
        assert reaction_record.state == "maybe_set"
        adapter._outbound_started.add(active.lease.lease_id)
        adapter._outbound_successful.add(active.lease.lease_id)
        event = SimpleNamespace(
            metadata={"onebot11_lease_id": active.lease.lease_id},
            media_urls=[],
        )
        await adapter._finish_queue_turn(event, ProcessingOutcome.SUCCESS)
        assert reaction_calls == [
            ("-1001", "128172", True),
            ("-1001", "128172", False),
        ]
        assert adapter._queue.status("888")["pending"] == 1
    finally:
        await adapter.disconnect()


async def test_fenced_reaction恢复只执行unset(monkeypatch):
    """旧 lease fencing 后，当前 recovery 仍可安全清理遗留 👀。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._queue.record_reaction("fenced-reaction", "888", "1001")
    adapter._queue.mark_reaction_set("fenced-reaction")
    adapter._fenced_leases.add("fenced-reaction")
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    try:
        await adapter._recover_processing_reactions_once()
        assert calls == [("1001", "128172", False)]
        assert adapter._queue.reaction_for_lease("fenced-reaction") is None
    finally:
        await adapter.disconnect()


async def test_reaction无持久目标时不使用内存message_id出站(monkeypatch):
    """清理缺少持久群目标时必须 fail-closed，不能猜测 unset 目标。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._processing_reaction_message_ids["stale-reaction"] = "1001"
    calls: list[bool] = []

    async def fake_reaction(_message_id: str, _emoji_id: str, *, enabled: bool) -> None:
        calls.append(enabled)

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    try:
        await adapter._clear_processing_reaction("stale-reaction", allow_recovery=True)
        assert calls == []
        assert "stale-reaction" not in adapter._processing_reaction_message_ids
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
        metadata={
            "onebot11_images": ["http://media.invalid/first.png"],
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            },
        },
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
        adapter_module.TriggerRequest.create(
            "888",
            "group:1001",
            "mention",
            "123",
            "小明",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
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


async def test_lease在队列turn媒体下载前失效不访问媒体(monkeypatch):
    """群 turn 在图片下载前失去 lease 时不能继续调用媒体源。"""
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
        text="看图",
        metadata={
            "onebot11_images": ["https://media.invalid/image.png"],
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            },
        },
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
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )
    downloads: list[str] = []

    async def fake_download(image: str, _dest_dir: str | None = None) -> None:
        downloads.append(image)
        return None

    monkeypatch.setattr(adapter, "_download_image", fake_download)
    monkeypatch.setattr(adapter._queue, "is_lease_current", lambda _lease: False)
    try:
        with pytest.raises(PermissionError):
            await adapter._dispatcher.notify("888")
        assert downloads == []
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


async def test_engaged短确认词统一走selector不创建trigger(monkeypatch):
    """活跃窗口中的短确认词没有特例，进入 selector 且不直接创建 trigger。"""
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
    state = adapter._trigger_state_for("888")
    state.on_turn_complete(success=True, now=time.monotonic())
    notifications: list[str] = []
    selector_calls: list[str] = []

    def fake_client_factory():
        class FakeClient:
            async def complete(self, prompt: str, timeout_seconds: float = 30):
                selector_calls.append(prompt)
                return '{"decision":"ignore","anchor_seq":null}'

        return FakeClient()

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(str(chat_id))
        return True

    monkeypatch.setattr(adapter, "_pi_ai_trigger_client", fake_client_factory)
    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="engaged-ack-1",
        user_id="123",
        user_name="小明",
        text="可以。",
        message_key="group:engaged-ack-1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        await adapter._enqueue_group_message(
            message,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        status = adapter._queue.status("888")
        assert status["pending"] == 1
        assert status["pending_trigger_requests"] == 0
        assert notifications == []
        assert adapter._trigger_states["888"].mode == "debounce"
        assert selector_calls == []
    finally:
        await adapter.disconnect()


async def test_selector候选添加并清理queued_reaction(monkeypatch):
    """LLM selector 等待期间使用 ⏳，创建 anchor 后移除它。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:1001",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        # 先制造活跃消息间隔：自适应 debounce 下第一条消息会立即判断，
        # 这里让候选消息落在 5 秒节流窗口内，保持固定等待节奏。
        adapter._trigger_state_for("888").last_message_at = time.monotonic() - 1
        await adapter._enqueue_group_message(
            message,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        await asyncio.sleep(0.05)
        state = adapter._trigger_states["888"]
        due_at = state.debounce_due
        assert due_at is not None
        assert calls == [("1001", "128064", True)]
        action = state.on_timer(now=due_at + 1)
        assert action.kind == "judge"
        async with adapter._trigger_lock_for("888"):
            result_action, notify, failure = await adapter._apply_llm_result_locked(
                "888",
                action,
                decision="trigger",
                anchor_seq=1,
                observed_revision=1,
                observed_seq=1,
            )
        assert result_action is not None
        assert result_action.reason == "llm"
        assert notify is True
        assert failure is None
        await asyncio.sleep(0.05)
        assert calls == [
            ("1001", "128064", True),
            ("1001", "128064", False),
        ]
    finally:
        await adapter.disconnect()


async def test_selector_ignore清理queued_reaction并保留pending(monkeypatch):
    """selector 明确不触发时移除 ⏳，但不删除待处理消息。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:1002",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    adapter._schedule_queued_reaction("888", message)
    await asyncio.sleep(0.05)
    action = state.on_timer(now=6)
    assert action.kind == "judge"
    async with adapter._trigger_lock_for("888"):
        result_action, notify, failure = await adapter._apply_llm_result_locked(
            "888",
            action,
            decision="ignore",
            anchor_seq=None,
            observed_revision=1,
            observed_seq=1,
        )
    assert result_action is not None
    assert result_action.reason == "llm_ignore"
    assert notify is False
    assert failure is None
    try:
        await asyncio.sleep(0.05)
        assert calls == [
            ("1002", "128064", True),
            ("1002", "128064", False),
        ]
        assert adapter._queue.status("888")["pending"] == 1
        assert adapter._queue.status("888")["pending_trigger_requests"] == 0
    finally:
        await adapter.disconnect()


async def test_selector_dirty_revision迁移queued_reaction到最新候选(monkeypatch):
    """selector 判断期间出现新消息时，⏳ 必须跟随最新候选而不是停在旧消息。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    first = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1003",
        user_id="123",
        user_name="小明",
        text="第一条问题怎么处理？",
        message_key="group:1003",
    )
    second = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1004",
        user_id="456",
        user_name="小红",
        text="补充的问题怎么处理？",
        message_key="group:1004",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        adapter._trigger_state_for("888").last_message_at = time.monotonic() - 1
        await adapter._enqueue_group_message(
            first,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        await asyncio.sleep(0.05)
        assert calls == [("1003", "128064", True)]

        state = adapter._trigger_states["888"]
        due_at = state.debounce_due
        assert due_at is not None
        judgement = state.on_timer(now=due_at + 1)
        assert judgement.kind == "judge"

        assert adapter._queue.enqueue(second).inserted
        dirty = state.observe_message(
            chat_type="group",
            text=second.text,
            mentioned_self=False,
            has_context=True,
            revision=2,
            now=time.monotonic(),
        )
        assert dirty.reason == "judging_dirty"

        async with adapter._trigger_lock_for("888"):
            result_action, notify, failure = await adapter._apply_llm_result_locked(
                "888",
                judgement,
                decision="ignore",
                anchor_seq=None,
                observed_revision=1,
                observed_seq=1,
            )
        assert result_action is not None
        assert result_action.reason == "queue_dirty"
        assert notify is False
        assert failure is None
        await asyncio.sleep(0.05)
        assert calls == [
            ("1003", "128064", True),
            ("1003", "128064", False),
            ("1004", "128064", True),
        ]
    finally:
        await adapter.disconnect()


async def test_restore_selector为待处理候选补回queued_reaction(monkeypatch):
    """重启恢复的候选也应显示 ⏳，但不创建 lease 或 Agent turn。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1005",
        user_id="123",
        user_name="小明",
        text="重启后这个问题怎么处理？",
        message_key="group:1005",
    )
    adapter._queue.enqueue(message)
    try:
        assert await adapter._restore_trigger_state("888") is False
        await asyncio.sleep(0.05)
        assert calls == [("1005", "128064", True)]
        status = adapter._queue.status("888")
        assert status["pending"] == 1
        assert status["pending_trigger_requests"] == 0
        assert adapter._dispatcher.active("888") is None
    finally:
        await adapter.disconnect()


async def test_selector_failure清理queued_reaction并保留pending(monkeypatch):
    """selector 超时/模型失败只清理 ⏳，不删除待处理消息。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1006",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:1006",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        adapter._trigger_state_for("888").last_message_at = time.monotonic() - 1
        await adapter._enqueue_group_message(
            message,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        await asyncio.sleep(0.05)
        state = adapter._trigger_states["888"]
        due_at = state.debounce_due
        assert due_at is not None
        action = state.on_timer(now=due_at + 1)
        assert action.kind == "judge"

        await adapter._apply_llm_failure(
            "888",
            action,
            failure="timeout",
            pending=1,
            input_bytes=128,
            duration_ms=10_000,
            model_call_started=True,
        )
        await asyncio.sleep(0.05)
        assert calls == [
            ("1006", "128064", True),
            ("1006", "128064", False),
        ]
        assert adapter._queue.status("888")["pending"] == 1
    finally:
        await adapter.disconnect()


async def test_selector_wait到期清理queued_reaction(monkeypatch):
    """selector 返回 wait 时暂留 ⏳，等待窗口真正到期后清理。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1007",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:1007",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    adapter._schedule_queued_reaction("888", message)
    await asyncio.sleep(0.05)
    action = state.on_timer(now=6)
    assert action.kind == "judge"
    async with adapter._trigger_lock_for("888"):
        result_action, notify, failure = await adapter._apply_llm_result_locked(
            "888",
            action,
            decision="wait",
            anchor_seq=None,
            observed_revision=1,
            observed_seq=1,
        )
    assert result_action is not None
    assert result_action.reason == "llm_wait"
    assert notify is False
    assert failure is None

    adapter._cancel_llm_judgement("888")
    state.wait_until = time.monotonic() + 0.01
    timer = asyncio.create_task(adapter._run_trigger_timer("888"))
    try:
        await asyncio.sleep(0.08)
        assert calls == [
            ("1007", "128064", True),
            ("1007", "128064", False),
        ]
    finally:
        timer.cancel()
        await asyncio.gather(timer, return_exceptions=True)
        await adapter.disconnect()


async def test_hard_trigger会清理旧候选queued_reaction(monkeypatch):
    """硬触发优先时，旧 selector 候选的 ⏳ 先清理，不迁移到硬触发消息。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    async def fake_notify(_chat_id: str) -> bool:
        return False

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    candidate = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1008",
        user_id="123",
        user_name="小明",
        text="候选问题怎么处理？",
        message_key="group:1008",
    )
    hard = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1009",
        user_id="123",
        user_name="小明",
        text="@bot 直接处理",
        message_key="group:1009",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        await adapter._enqueue_group_message(
            candidate,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        await asyncio.sleep(0.05)
        await adapter._enqueue_group_message(
            hard,
            mentioned_self=True,
            caller=caller,
            user_name="小明",
        )
        await asyncio.sleep(0.05)
        assert calls == [
            ("1008", "128064", True),
            ("1008", "128064", False),
        ]
        assert adapter._queue.status("888")["pending_trigger_requests"] == 1
    finally:
        await adapter.disconnect()


async def test_pause会清理queued_reaction(monkeypatch):
    """暂停群级自动触发时尽力移除当前 selector 的 ⏳。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1010",
        user_id="123",
        user_name="小明",
        text="暂停前的问题怎么处理？",
        message_key="group:1010",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    adapter._schedule_queued_reaction("888", message)
    await asyncio.sleep(0.05)
    try:
        assert await adapter._set_group_paused("888", True)
        await asyncio.sleep(0.05)
        assert calls == [
            ("1010", "128064", True),
            ("1010", "128064", False),
        ]
    finally:
        await adapter.disconnect()


async def test_clear会清理queued_reaction(monkeypatch):
    """/onebot clear 清理队列时同时移除 selector 等待提示。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1011",
        user_id="123",
        user_name="小明",
        text="清理前的问题怎么处理？",
        message_key="group:1011",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    adapter._schedule_queued_reaction("888", message)
    await asyncio.sleep(0.05)
    try:
        assert await adapter._clear_group("888") == 1
        await asyncio.sleep(0.05)
        assert calls == [
            ("1011", "128064", True),
            ("1011", "128064", False),
        ]
        assert adapter._queue.status("888")["pending"] == 0
    finally:
        await adapter.disconnect()


async def test_disconnect会清理queued_reaction(monkeypatch):
    """断开时尽力移除内存中残留的 selector 等待提示。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1012",
        user_id="123",
        user_name="小明",
        text="断开前的问题怎么处理？",
        message_key="group:1012",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    adapter._schedule_queued_reaction("888", message)
    await asyncio.sleep(0.05)
    assert calls == [("1012", "128064", True)]
    await adapter.disconnect()
    await asyncio.sleep(0.05)
    assert calls == [
        ("1012", "128064", True),
        ("1012", "128064", False),
    ]
    assert not adapter._queued_reaction_message_ids
    assert not adapter._queued_reaction_tasks


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
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": [],
                "self_id": "1",
            }
        },
        message_key="group:1001",
    )
    latest = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="456",
        user_name="小红",
        text="这个问题怎么处理？",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": [],
                "self_id": "1",
            }
        },
        message_key="group:1002",
    )
    adapter._queue.enqueue(first)
    adapter._queue.enqueue(latest)
    try:
        assert await adapter._create_llm_trigger("888", anchor_seq=2, observed_seq=2)
        active = adapter._dispatcher.active("888")
        assert active is not None
        assert adapter._reaction_message_id(active.lease) == "1002"
    finally:
        await adapter.disconnect()


async def test_TurnAnchor以真实锚点消息决定authority和reaction(monkeypatch):
    """即使 trigger caller 字段错误，当前 anchor 消息仍是权限和回复锚点来源。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    adapter._processing_reaction_enabled = False
    first = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="456",
        user_name="上下文用户",
        text="前一条",
        message_key="group:1001",
    )
    anchor = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="123",
        user_name="管理员",
        text="@机器人执行这个",
        metadata={
            "onebot11_authority": {
                "role": "super_admin",
                "allowed_tools": sorted(adapter.role_tools["super_admin"]),
                "self_id": "1",
            }
        },
        message_key="group:1002",
    )
    adapter._queue.enqueue(first)
    adapter._queue.enqueue(
        anchor,
        adapter_module.TriggerRequest.create(
            "888",
            "group:1002",
            "mention",
            "456",
            "错误的 caller",
            anchor_kind="hard",
            authority_role="super_admin",
            authority_tools=adapter.role_tools["super_admin"],
            authority_self_id="1",
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
        assert event.source.user_id == "123"
        assert event.source.user_name == "管理员"
        assert event.reply_to_message_id == "1002"
        assert event.metadata["onebot11_anchor_seq"] == 2
        assert event.metadata["onebot11_anchor_kind"] == "hard"
        assert event.metadata["onebot11_anchor_message_id"] == "1002"
        assert event.metadata["onebot11_caller_context"]["role"] == "super_admin"
        assert adapter._reaction_message_id(lease) == "1002"
    finally:
        adapter._queue.release(lease, reason="test cleanup")
        await adapter.disconnect()


async def test_TurnAnchor保留Hermes通用工具权限快照(monkeypatch):
    """trusted_user 的 generic Hermes 工具不能在建 turn 时被 OneBot 工具集合裁掉。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "ws_port": 0,
                "roles": {
                    "user": {"tools": []},
                    "trusted_user": {
                        "users": ["456"],
                        "tools": ["terminal"],
                    },
                    "super_admin": {"tools": []},
                },
            },
        )
    )
    adapter._processing_reaction_enabled = False
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="generic-authority",
        user_id="456",
        user_name="可信用户",
        text="请执行脚本",
        metadata={
            "onebot11_authority": {
                "role": "trusted_user",
                "allowed_tools": ["terminal"],
                "self_id": "1",
            }
        },
        message_key="group:generic-authority",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "456",
            "可信用户",
            anchor_kind="hard",
            authority_role="trusted_user",
            authority_tools=["terminal"],
            authority_self_id="1",
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
        assert event.metadata["onebot11_caller_context"]["allowed_tools"] == ["terminal"]
    finally:
        adapter._queue.release(lease, reason="test cleanup")
        await adapter.disconnect()


async def test_TurnAnchor权限快照与真实消息冲突时fail_closed(monkeypatch):
    """持久 trigger 不能把普通消息的 authority 提升成超级管理员。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    adapter._processing_reaction_enabled = False
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="authority-conflict",
        user_id="456",
        user_name="普通用户",
        text="@机器人执行",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
        message_key="group:authority-conflict",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "456",
            "普通用户",
            anchor_kind="hard",
            authority_role="super_admin",
            authority_tools=adapter.role_tools["super_admin"],
            authority_self_id="1",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    try:
        with pytest.raises(PermissionError, match="authority"):
            await adapter._start_queue_turn(lease)
        assert adapter._queue.status("888")["uncertain"] == 1
    finally:
        await adapter.disconnect()


async def test_跨机器人anchor_self_id进入uncertain而不启动Agent(monkeypatch, tmp_path):
    """持久 anchor 属于其他机器人时，adapter 必须 fail-closed。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_QUEUE_DB=str(tmp_path / "queue.sqlite3"),
    )
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="foreign-self-id",
        user_id="123",
        user_name="小明",
        text="旧 anchor",
        message_key="group:foreign-self-id",
    )
    trigger = adapter_module.TriggerRequest.create(
        "888",
        message.message_key,
        "mention",
        "123",
        "小明",
        authority_role="user",
        authority_tools=(),
        authority_self_id="999",
    )
    adapter._queue.enqueue(message, trigger)
    lease = adapter._queue.claim("888")
    assert lease is not None

    with pytest.raises(PermissionError, match="self_id"):
        await adapter._start_queue_turn(lease)

    assert adapter._queue.status("888")["uncertain"] == 1
    adapter._queue.close()


async def test_缺失anchor_authority进入uncertain而不启动Agent(monkeypatch, tmp_path):
    """旧 anchor 缺少 authority self_id 时不能按普通用户继续执行。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_QUEUE_DB=str(tmp_path / "queue.sqlite3"),
    )
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="missing-authority",
        user_id="123",
        user_name="小明",
        text="旧 anchor",
        message_key="group:missing-authority",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "123",
            "小明",
            authority_role="user",
            authority_tools=(),
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None

    with pytest.raises(PermissionError, match="self_id 缺失"):
        await adapter._start_queue_turn(lease)

    assert adapter._queue.status("888")["uncertain"] == 1
    adapter._queue.close()


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
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
        message_key="group:summary-1",
    )
    adapter._queue.enqueue(
        first,
            adapter_module.TriggerRequest.create(
                "888",
                "group:summary-1",
                "mention",
                "123",
                "小明",
                authority_role="user",
                authority_tools=adapter.role_tools["user"],
                authority_self_id="1",
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
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
        message_key="group:summary-2",
    )
    adapter._queue.enqueue(
        second,
            adapter_module.TriggerRequest.create(
                "888",
                "group:summary-2",
                "mention",
                "123",
                "小明",
                authority_role="user",
                authority_tools=adapter.role_tools["user"],
                authority_self_id="1",
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


async def test_pi_ai_selector成功后创建durable_trigger(monkeypatch):
    """旁路判断由插件自有 pi-ai client 完成，不依赖 Hermes auxiliary。"""
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
                "trigger_cooldown_seconds": 60,
            },
        )
    )
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="pi-ai-question",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
        message_key="group:pi-ai-question",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    action = state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    assert action.kind == "schedule"
    action = state.on_timer(now=6)
    assert action.kind == "judge"

    class FakeClient:
        """返回固定的严格 selector JSON。"""

        async def complete(self, prompt: str, *, timeout_seconds: float) -> str:
            """校验 helper 输入并返回 trigger 决策。"""
            assert "这个问题怎么处理" in prompt
            assert timeout_seconds > 0
            return '{"decision":"trigger","anchor_seq":1}'

    monkeypatch.setattr(adapter_module, "PiAiTriggerClient", lambda **_kwargs: FakeClient())
    notified: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notified.append(chat_id)
        return True

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    try:
        await adapter._start_llm_judgement("888", action)
        await adapter._llm_trigger_tasks["888"]
        status = adapter._queue.status("888")
        assert status["pending"] == 1
        assert status["pending_trigger_requests"] == 1
        assert notified == ["888"]
    finally:
        await adapter.disconnect()


async def test_provider_missing写入selector持久退避(monkeypatch):
    """旁路 provider 缺失也必须写入 wall-clock 退避，避免恢复时反复判断。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter.trigger_config = replace(
        adapter.trigger_config,
        llm_enabled=True,
        llm_provider="",
        llm_model="",
        llm_allowed_groups=frozenset({"888"}),
    )
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="provider-missing",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:provider-missing",
    )
    adapter._queue.enqueue(message)
    state = adapter._trigger_state_for("888")
    action = state.observe_message(
        chat_type="group",
        text=message.text,
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    assert action.kind == "schedule"
    action = state.on_timer(now=6)
    assert action.kind == "judge"
    try:
        await adapter._start_llm_judgement("888", action)
        persisted = adapter._queue.llm_state("888")
        assert persisted["llm_failure_count"] == 1
        assert persisted["llm_next_attempt_at"] is not None
        assert persisted["llm_last_error"] == "provider_missing"
        monkeypatch.setattr(
            adapter,
            "_pi_ai_trigger_client",
            lambda: pytest.fail("selector failure backoff has not expired"),
        )
        await adapter._apply_trigger_action_locked(
            "888",
            adapter_module.TriggerAction(
                "schedule",
                candidate_type="question",
                revision=1,
            ),
        )
        assert adapter._trigger_state_for("888").debounce_due is not None
    finally:
        await adapter.disconnect()


async def test_restore_selector遵守持久退避且不重复判断(monkeypatch):
    """重启恢复不能因为旧消息未推进 judged_seq 而绕过 selector backoff。"""
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
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="restore-backoff",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:restore-backoff",
    )
    adapter._queue.enqueue(message)
    adapter._queue.mark_llm_failure(
        "888",
        observed_seq=1,
        error="provider_missing",
        next_attempt_at=time.time() + 120,
    )
    try:
        assert await adapter._restore_trigger_state("888") is False
        assert adapter._queue.status("888")["pending_trigger_requests"] == 0
        assert "888" not in adapter._trigger_timer_tasks
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
    adapter._processing_reaction_enabled = False
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="complete-1",
        user_id="123",
        user_name="成员",
        text="原始触发",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
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
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
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
            metadata={
                "onebot11_authority": {
                    "role": "user",
                    "allowed_tools": sorted(adapter.role_tools["user"]),
                    "self_id": "1",
                }
            },
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
    adapter._processing_reaction_enabled = False
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="complete-ordinary-1",
        user_id="123",
        user_name="成员",
        text="原始触发",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
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
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
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
            metadata={
                "onebot11_authority": {
                    "role": "user",
                    "allowed_tools": sorted(adapter.role_tools["user"]),
                    "self_id": "1",
                }
            },
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


async def test_completion后短确认词followup进入selector不创建新lease(monkeypatch):
    """turn 期间入队的短确认词在成功收口后走 selector，不直接创建 trigger。"""
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
    adapter._processing_reaction_enabled = False
    first = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="complete-ack-1",
        user_id="123",
        user_name="成员",
        text="原始触发",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
        message_key="group:complete-ack-1",
    )
    adapter._queue.enqueue(
        first,
        adapter_module.TriggerRequest.create(
            "888",
            "group:complete-ack-1",
            "mention",
            "123",
            "成员",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )

    async def fake_handle_message(_adapter, _event) -> None:
        return None

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", fake_handle_message)
    original_complete = adapter._dispatcher.complete

    async def complete_after_ack(
        lease_id: str,
        *,
        outcome: str,
        unknown: bool,
        **kwargs,
    ) -> bool:
        ack_message = adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="complete-ack-2",
            user_id="123",
            user_name="成员",
            text="可以",
            metadata={
                "onebot11_authority": {
                    "role": "user",
                    "allowed_tools": sorted(adapter.role_tools["user"]),
                    "self_id": "1",
                }
            },
            message_key="group:complete-ack-2",
        )
        await adapter._enqueue_group_message(
            ack_message,
            mentioned_self=False,
            caller=adapter_module.CallerContext(
                user_id="123",
                chat_type="group",
                chat_id="888",
                role="user",
                allowed_tools=adapter_module.READ_ONLY_TOOLS,
                self_id="1",
            ),
            user_name="成员",
        )
        return await original_complete(
            lease_id,
            outcome=outcome,
            unknown=unknown,
            **kwargs,
        )

    monkeypatch.setattr(adapter._dispatcher, "complete", complete_after_ack)
    try:
        assert await adapter._dispatcher.notify("888")
        active = adapter._dispatcher.active("888")
        assert active is not None
        first_lease = active.lease.lease_id
        adapter._outbound_started.add(first_lease)
        adapter._outbound_successful.add(first_lease)
        await adapter._finish_queue_turn(
            SimpleNamespace(
                metadata={
                    "onebot11_lease_id": first_lease,
                    "onebot11_lease_revision": active.lease.revision,
                    "onebot11_target": {"chat_type": "group", "chat_id": "888"},
                },
                media_urls=[],
            ),
            ProcessingOutcome.SUCCESS,
        )
        # followup 没有硬触发，只能进入 debounce 等待 selector，不会创建新 lease。
        second_active = adapter._dispatcher.active("888")
        assert second_active is None
        status = adapter._queue.status("888")
        assert int(status.get("leased", 0)) == 0
        assert int(status.get("pending", 0)) == 1
        assert adapter._trigger_states["888"].mode == "debounce"
    finally:
        await adapter.disconnect()


async def test_debounce中短确认词继续等待不创建trigger(monkeypatch):
    """候选等待（debounce）期间活跃窗口内的短确认词继续合并等待，不创建 trigger。"""
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
    adapter._processing_reaction_enabled = False

    notifications: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(str(chat_id))
        return True

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    state = adapter._trigger_state_for("888")
    state.on_turn_complete(success=True, now=time.monotonic())
    state.last_message_at = time.monotonic() - 1
    candidate = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="debounce-ack-1",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:debounce-ack-1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        await adapter._enqueue_group_message(
            candidate,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        assert adapter._trigger_states["888"].mode == "debounce"
        ack = adapter_module.QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="debounce-ack-2",
            user_id="123",
            user_name="小明",
            text="可以",
            message_key="group:debounce-ack-2",
        )
        await adapter._enqueue_group_message(
            ack,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        status = adapter._queue.status("888")
        assert status["pending_trigger_requests"] == 0
        assert notifications == []
        assert adapter._trigger_states["888"].mode == "debounce"
    finally:
        await adapter.disconnect()


async def test_第一条候选消息立即进入判断(monkeypatch):
    """自适应 debounce：本群第一条候选消息不等待固定窗口，立即判断。"""
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
    adapter._processing_reaction_enabled = False
    candidate = adapter_module.QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="instant-judge-1",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:instant-judge-1",
    )
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
    )
    try:
        await adapter._enqueue_group_message(
            candidate,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        now = time.monotonic()
        state = adapter._trigger_states["888"]
        assert state.mode == "judging" or (
            state.debounce_due is not None and state.debounce_due - now < 0.5
        )
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
                anchor_seq=None,
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


async def test_context命令在入队前旁路处理(monkeypatch):
    """群 /context 只返回有界诊断，不创建 queue message 或 trigger。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    replies: list[str] = []

    async def fake_send_direct(_event, text: str) -> None:
        replies.append(text)

    monkeypatch.setattr(adapter, "_send_direct", fake_send_direct)
    try:
        raw = _group_raw(888, text="/context", at_self=False)
        await adapter._on_ws_event(raw)
        status = adapter._queue.status("888")
        assert status["pending"] == 0
        assert status["pending_trigger_requests"] == 0
        assert len(replies) == 1
        diagnostic = json.loads(replies[0])
        assert diagnostic["target"] == {"chat_type": "group", "chat_id": "888"}
        assert diagnostic["pending"] == 0
    finally:
        await adapter.disconnect()


async def test_群聊硬触发在冷却期间仍创建anchor(monkeypatch):
    """真实 adapter 路径不能把 cooldown 当成硬触发的拒绝。"""
    adapter = OneBot11Adapter(
        PlatformConfig(
            enabled=True,
            extra={
                "http_api": "http://127.0.0.1:3000",
                "self_id": "1",
                "ws_port": 0,
                "trigger_cooldown_seconds": 60,
            },
        )
    )
    adapter._last_trigger_at["888"] = time.monotonic()

    async def fake_notify(_chat_id: str) -> bool:
        return False

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    try:
        await adapter._on_ws_event(_group_raw(888, text="@机器人再次唤醒", at_self=True))
        assert adapter._queue.status("888")["pending_trigger_requests"] == 1
    finally:
        await adapter.disconnect()


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


def test_访问策略使用构造期RuntimeConfig快照(monkeypatch):
    """运行中环境变量变化不能悄悄改变已构造 adapter 的授权合同。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_DM_POLICY="open",
        ONEBOT11_ALLOW_ALL_USERS="true",
    )
    monkeypatch.setenv("ONEBOT11_ALLOW_ALL_USERS", "false")
    monkeypatch.setenv("GATEWAY_ALLOW_ALL_USERS", "false")

    assert adapter._chat_access_allowed("dm", "2056963663", "2056963663")


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


async def test_uncertain群不会继续消耗selector模型(monkeypatch):
    """blocked 群必须先人工 resolve，软触发不能继续调用旁路 LLM。"""
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
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="blocked-selector",
        user_id="123",
        user_name="小明",
        text="问题？",
        message_key="group:blocked-selector",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            "group:blocked-selector",
            "mention",
            "123",
            "小明",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    assert adapter._queue.mark_uncertain(lease, "unknown")
    state = adapter._trigger_state_for("888")
    state.mode = "debounce"
    state.debounce_due = 1.0

    def fail_if_called():
        raise AssertionError("uncertain 群不应调用 selector route")

    monkeypatch.setattr(
        adapter,
        "_pi_ai_trigger_client",
        fail_if_called,
    )
    await adapter._apply_trigger_action_locked(
        "888",
        adapter_module.TriggerAction("schedule", candidate_type="question"),
    )
    assert state.mode == "idle"
    await adapter.disconnect()


async def test_send走HTTP并返回SendResult(monkeypatch, fake_http_server):
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    try:
        adapter._chat_types["888"] = "group"
        result = await adapter._send_with_retry("888", "你好")
        assert isinstance(result, SendResult)
        assert result.success
        assert calls[0]["path"] == "/send_group_msg"
        assert calls[0]["params"]["group_id"] == 888
    finally:
        await adapter.disconnect()


async def test_send_image_file使用base64segment并保留reply(monkeypatch, fake_http_server):
    """图片出站不能依赖 LLBot 容器可见的宿主机路径。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    image_path = Path(adapter._media_root) / "reply.png"
    payload = b"\x89PNG\r\n\x1a\nimage"
    image_path.write_bytes(payload)
    try:
        adapter._chat_types["888"] = "group"
        result = await adapter.send_image_file(
            "888",
            str(image_path),
            caption="图片说明",
            reply_to="1001",
        )
        assert result.success
        assert calls[0]["path"] == "/send_group_msg"
        segments = calls[0]["params"]["message"]
        assert segments[0] == {"type": "reply", "data": {"id": "1001"}}
        assert segments[1]["type"] == "image"
        encoded = segments[1]["data"]["file"]
        assert base64.b64decode(encoded.removeprefix("base64://")) == payload
        assert segments[2] == {"type": "text", "data": {"text": "图片说明"}}
    finally:
        await adapter.disconnect()
async def test_validate_media_delivery_path拒绝仓库证据并接受Hermes媒体缓存(
    monkeypatch, tmp_path
):
    """MEDIA 只能引用 Hermes 媒体缓存，不能直接发送仓库 evidence 源文件。"""
    hermes_home = tmp_path / "hermes-home"
    cache_root = hermes_home / "cache" / "images"
    cache_root.mkdir(parents=True)
    source_path = tmp_path / "repo" / "evidence" / "evidence-settings-mobile.png"
    source_path.parent.mkdir(parents=True)
    payload = b"\x89PNG\r\n\x1a\nmedia-contract"
    source_path.write_bytes(payload)
    cache_path = cache_root / "run-mobile.png"
    cache_path.write_bytes(payload)
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    try:
        assert adapter.validate_media_delivery_path(str(source_path)) is None
        assert adapter.validate_media_delivery_path(str(cache_path)) == str(cache_path.resolve())
    finally:
        await adapter.disconnect()


async def test_send默认转换为纯文本并记录marker不可用(monkeypatch, fake_http_server):
    """OneBot 出站不应把 Markdown 语法或图片 marker 原样发送。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    audit_events: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        adapter._audit,
        "record",
        lambda event, data: audit_events.append((event, data)),
    )
    try:
        adapter._chat_types["888"] = "group"
        result = await adapter._send_with_retry(
            "888",
            "# 标题\n**重点**\n[[onebot11:markdown-image]]![图](https://example.invalid/a.png)[[/onebot11:markdown-image]]",
        )
        assert result.success
        sent_text = calls[0]["params"]["message"][0]["data"]["text"]
        assert sent_text == "标题\n重点\n图 (https://example.invalid/a.png)"
        assert "onebot11:markdown-image" not in sent_text
        assert any(event == "markdown_image_requested_unavailable" for event, _data in audit_events)
    finally:
        await adapter.disconnect()


async def test_明确控制面metadata不污染业务出站状态(monkeypatch, fake_http_server):
    """未来 Hermes heartbeat 只能凭显式 metadata 走控制面路径。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    try:
        adapter._chat_types["888"] = "group"
        result = await adapter.send(
            "888",
            "仍在处理",
            metadata={
                "hermes_control_plane": True,
                "hermes_control_kind": "long_running",
            },
        )
        assert result.success
        assert result.raw_response["control_plane"] is True
        assert adapter._outbound_started == set()
        assert calls[0]["path"] == "/send_group_msg"
    finally:
        await adapter.disconnect()


async def test_控制面通知同一turn只发送一次并兼容系统错误metadata(
    monkeypatch, fake_http_server
):
    """重复 heartbeat 不刷屏，未来系统错误标记也不污染业务出站状态。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    caller = adapter._caller_for_event(
        SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
    )
    binding = adapter_module.TurnBinding("session-control", "turn-control", caller)
    adapter._bindings.bind(binding)
    try:
        metadata = {
            "hermes_system_error_notice": True,
            "onebot11_binding_key": {
                "session_id": "session-control",
                "turn_id": "turn-control",
            },
        }
        first = await adapter.send("888", "系统提示", metadata=metadata)
        second = await adapter.send("888", "系统提示", metadata=metadata)
        assert first.success
        assert second.success
        assert second.raw_response["deduplicated"] is True
        assert len(calls) == 1
        assert adapter._outbound_started == set()
    finally:
        adapter._bindings.clear()
        await adapter.disconnect()


async def test_长时间运行提示只发送一次且不污染业务marker(
    monkeypatch, fake_http_server
):
    """活动 turn 超过延迟后只发送一次控制面提示。"""
    base, calls = fake_http_server
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API=base,
        ONEBOT11_SELF_ID="1",
        ONEBOT11_LONG_RUNNING_NOTICE_SECONDS="0.01",
    )
    await adapter.connect()
    caller = adapter._caller_for_event(
        SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
    )
    caller = replace(caller, lease_id="long-running-lease")
    binding = adapter_module.TurnBinding(
        "session-long-running",
        "turn-long-running",
        caller,
        "long-running-lease",
    )
    adapter._bindings.bind(binding)
    adapter._targets["888"] = adapter_module.ChatTarget("group", "888")
    adapter._chat_types["888"] = "group"
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: True)
    monkeypatch.setattr(adapter, "_lease_matches_target", lambda *_args: True)
    event = SimpleNamespace(
        metadata={
            "onebot11_lease_id": "long-running-lease",
            "onebot11_binding_key": {
                "session_id": "session-long-running",
                "turn_id": "turn-long-running",
            },
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
            "onebot11_anchor_message_id": "1001",
        }
    )
    try:
        adapter._schedule_long_running_notice(event, "long-running-lease")
        await asyncio.sleep(0.05)
        adapter._schedule_long_running_notice(event, "long-running-lease")
        await asyncio.sleep(0)
        assert len(calls) == 1
        assert calls[0]["params"]["message"][1]["data"]["text"] == (
            "仍在处理中，请稍候…"
        )
        assert adapter._outbound_started == set()
        assert adapter.SUPPORTS_MESSAGE_EDITING is False
        assert adapter.format_tool_event({"kind": "tool"}) is None
    finally:
        adapter._bindings.clear()
        await adapter.disconnect()


async def test_中间正文发送后重置长时间提示计时器(monkeypatch, fake_http_server):
    """interim 成功发送后必须重置 60s 提示计时器，不能照旧触发冗余提示。"""
    base, calls = fake_http_server
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API=base,
        ONEBOT11_SELF_ID="1",
        ONEBOT11_LONG_RUNNING_NOTICE_SECONDS="3600",
    )
    adapter.show_interim_group = True
    await adapter.connect()
    adapter._chat_types["888"] = "group"
    adapter._targets["888"] = adapter_module.ChatTarget("group", "888")
    # 模拟当前活动 turn：interim 发送时按 chat_id 找到 lease 才能重置。
    active_turn = SimpleNamespace(
        lease=SimpleNamespace(lease_id="interim-reset-lease"),
    )
    adapter._dispatcher._active["888"] = active_turn
    event = SimpleNamespace(
        metadata={
            "onebot11_lease_id": "interim-reset-lease",
            "onebot11_target": {"chat_type": "group", "chat_id": "888"},
            "onebot11_anchor_message_id": "1001",
        }
    )
    try:
        adapter._schedule_long_running_notice(event, "interim-reset-lease")
        first_task = adapter._long_running_notice_tasks["interim-reset-lease"]
        assert not first_task.done()

        result = await adapter.send("888", "正在生成图片，请稍候…")
        assert result.success
        second_task = adapter._long_running_notice_tasks["interim-reset-lease"]
        assert second_task is not first_task
        await asyncio.sleep(0)
        assert first_task.done()
        assert not second_task.done()
        # 中途正文发出去后，3600s 内不应出现"仍在处理中"提示。
        await asyncio.sleep(0.05)
        assert not second_task.done()
        sent_texts = [
            segment.get("data", {}).get("text", "")
            for call in calls
            for segment in call["params"].get("message", [])
            if segment.get("type") == "text"
        ]
        assert "仍在处理中，请稍候…" not in sent_texts
    finally:
        adapter._dispatcher._active.pop("888", None)
        adapter._bindings.clear()
        await adapter.disconnect()


async def test_权限hook审计失败仍然fail_closed(monkeypatch):
    """审计旁路异常不能让 pre_tool_call 失去阻断能力。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter._caller_for_event(
        SimpleNamespace(user_id="123", chat_type="group", chat_id="888")
    )
    adapter._bindings.bind(
        adapter_module.TurnBinding("session-audit", "turn-audit", caller, "lease-audit")
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    monkeypatch.setattr(
        adapter._audit,
        "record",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("audit down")),
    )
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: False)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="qq_get_message",
            session_id="session-audit",
            turn_id="turn-audit",
            args={},
        )
        assert result is not None
        assert result["action"] == "block"
    finally:
        await adapter.disconnect()


async def test_权限hook拦截terminal写敏感配置(monkeypatch):
    """super_admin 在群里也不能通过 terminal 写 Hermes 安全敏感配置。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller = adapter._caller_for_event(
        SimpleNamespace(user_id="2056963663", chat_type="group", chat_id="888")
    )
    # 测试环境没有部署配置，显式声明超管，验证"超管也不能写敏感配置"。
    monkeypatch.setattr(adapter, "super_admins", frozenset({"2056963663"}))
    caller = adapter._caller_for_event(
        SimpleNamespace(user_id="2056963663", chat_type="group", chat_id="888")
    )
    adapter._bindings.bind(
        adapter_module.TurnBinding(
            "session-term",
            "turn-term",
            caller,
            "lease-term",
        )
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: True)
    try:
        blocked = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="session-term",
            turn_id="turn-term",
            args={"command": "sed -i 's/a/b/' ~/.hermes/config.yaml"},
        )
        assert blocked is not None
        assert blocked["action"] == "block"
        assert "安全敏感配置" in blocked["message"]

        allowed = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="session-term",
            turn_id="turn-term",
            args={"command": "cat ~/.hermes/config.yaml"},
        )
        assert allowed is None
    finally:
        adapter._bindings.clear()
        await adapter.disconnect()


async def test_send_multiple_images同一turn相同路径只访问一次(monkeypatch):
    """Hermes 同轮重复提取同一文件时 OneBot 只收到一次图片请求。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    image_path = Path(adapter._media_root) / "duplicate-image.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\nsame-image")
    calls: list[list[dict]] = []

    async def send_segments(
        _target_id: str,
        segments: list[dict],
        *,
        chat_type: str,
    ) -> str:
        calls.append(segments)
        assert chat_type == "group"
        return str(len(calls))

    monkeypatch.setattr(adapter._api, "send_message_segments", send_segments)
    try:
        results = await adapter.send_multiple_images(
            "888",
            [(str(image_path), "第一次"), (f"file://{image_path}", "第二次")],
            metadata={"onebot11_message_id": "turn-message"},
        )
        assert len(calls) == 1
        assert [result.success for result in results] == [True, True]
        assert results[1].raw_response["deduplicated"] is True
    finally:
        image_path.unlink(missing_ok=True)
        await adapter.disconnect()


async def test_send_image失效lease不会先下载媒体(monkeypatch):
    """远程图片入口必须在媒体请求前完成 lease fencing。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        lease_id="stale-image-lease",
        self_id="1",
    )
    binding = adapter_module.TurnBinding(
        "image-session",
        "image-turn",
        caller,
        "stale-image-lease",
    )
    adapter._bindings.bind(binding)
    monkeypatch.setattr(adapter, "_lease_is_current", lambda _lease_id: False)
    downloads: list[str] = []

    async def record_download(image_url: str, _dest_dir: str | None = None) -> None:
        downloads.append(image_url)
        return None

    monkeypatch.setattr(adapter._api, "download_to_temp", record_download)
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(binding)
    try:
        result = await adapter.send_image("888", "https://media.invalid/image.png")
        assert not result.success
        assert result.error_kind == "fenced"
        assert downloads == []
    finally:
        adapter_module._CURRENT_BINDING.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        await adapter.disconnect()


async def test_send_multiple_images返回部分成功结果(monkeypatch):
    """多图预检失败时不能先发送前面的图片。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    first = Path(adapter._media_root) / "first.png"
    second = Path(adapter._media_root) / "second.png"
    first.write_bytes(b"\x89PNG\r\n\x1a\nfirst")
    second.write_bytes(b"not-an-image")

    async def fake_segments(*_args, **_kwargs):
        return "first-message"

    monkeypatch.setattr(adapter._api, "send_message_segments", fake_segments)
    results = await adapter.send_multiple_images(
        "888",
        [(f"file://{first}", ""), (f"file://{second}", "")],
    )
    assert [result.success for result in results] == [False, False]
    assert all(result.error_kind == "failed" for result in results)


async def test_入站file通过get_image复制到受控媒体目录(monkeypatch, tmp_path):
    """OneBot file 标识不直接读取返回路径，只复制允许根目录内的图片。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    source_root = tmp_path / "onebot-media"
    source_root.mkdir()
    source = source_root / "received.png"
    source.write_bytes(b"\x89PNG\r\n\x1a\nreceived")
    adapter._media_source_roots = (source_root.resolve(),)
    destination = tmp_path / "turn"
    calls: list[str] = []

    async def fake_get_image(file_id: str) -> str:
        calls.append(file_id)
        return str(source)

    monkeypatch.setattr(adapter._api, "get_image", fake_get_image)
    try:
        copied = await adapter._download_image("file-id-1", str(destination))
        assert copied is not None
        assert Path(copied).read_bytes() == source.read_bytes()
        assert calls == ["file-id-1"]

        adapter._media_source_roots = (tmp_path / "other").resolve(),
        assert await adapter._download_image("file-id-1", str(destination)) is None
    finally:
        await adapter.disconnect()


async def test_send_multiple_images预检总量超限不访问OneBot(monkeypatch):
    """总大小超限时，所有图片都只返回预检失败，不能发送一半。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_MAX_IMAGE_TOTAL_BYTES="1024",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    first = Path(adapter._media_root) / "total-first.png"
    second = Path(adapter._media_root) / "total-second.png"
    first.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 800)
    second.write_bytes(b"\x89PNG\r\n\x1a\n" + b"y" * 800)
    called = False

    async def fail_if_called(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("预检失败时不能访问 OneBot")

    monkeypatch.setattr(adapter._api, "send_message_segments", fail_if_called)
    results = await adapter.send_multiple_images(
        "888",
        [(f"file://{first}", ""), (f"file://{second}", "")],
    )
    assert len(results) == 2
    assert all(result.error_kind == "too_large" for result in results)
    assert called is False


async def test_send_multiple_images数量超限不访问OneBot(monkeypatch):
    """图片数量超限时，不能下载或访问 OneBot。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_MAX_IMAGES_PER_MESSAGE="1",
    )
    called = False

    async def fail_if_called(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("数量预检失败时不能访问 OneBot")

    monkeypatch.setattr(adapter._api, "send_message_segments", fail_if_called)
    results = await adapter.send_multiple_images(
        "888",
        [
            ("https://example.invalid/first.png", ""),
            ("https://example.invalid/second.png", ""),
        ],
    )
    assert len(results) == 2
    assert all(result.error_kind == "too_many" for result in results)
    assert called is False


async def test_send_multiple_images遇到unknown停止后续请求(monkeypatch):
    """图片结果未知时不再发起同一 turn 的后续非幂等请求。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    first = Path(adapter._media_root) / "unknown-first.png"
    second = Path(adapter._media_root) / "unknown-second.png"
    third = Path(adapter._media_root) / "unknown-third.png"
    first.write_bytes(b"\x89PNG\r\n\x1a\nfirst")
    second.write_bytes(b"\x89PNG\r\n\x1a\nsecond")
    third.write_bytes(b"\x89PNG\r\n\x1a\nthird")
    calls = 0

    async def return_unknown(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return ""

    monkeypatch.setattr(adapter._api, "send_message_segments", return_unknown)
    results = await adapter.send_multiple_images(
        "888",
        [(f"file://{first}", ""), (f"file://{second}", ""), (f"file://{third}", "")],
    )
    assert calls == 1
    assert len(results) == 3
    assert all(result.error_kind == "unknown" for result in results)


async def test_send未连接返回失败(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    result = await adapter.send("888", "你好")
    assert not result.success


async def test_文本分块已成功一块后ValueError进入unknown(monkeypatch):
    """已有文本块成功后，后续畸形响应不能按可安全重试的 known failure 处理。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._ws = object()
    adapter._chat_types["888"] = "group"
    monkeypatch.setattr(adapter, "max_message_length_for_chat", lambda _chat_id: 1)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="partial-text",
        user_id="123",
        user_name="小明",
        text="分块",
        message_key="group:partial-text",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "123",
            "小明",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
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
        "partial-session",
        "partial-turn",
        caller,
        lease.lease_id,
    )
    adapter._bindings.bind(binding)
    calls = 0

    async def flaky_send_message(
        _target_id: str,
        _content: str,
        *,
        chat_type: str,
        reply_to: str | None = None,
    ) -> str:
        nonlocal calls
        del chat_type, reply_to
        calls += 1
        if calls == 1:
            return "partial-message-1"
        raise ValueError("响应不是 JSON")

    monkeypatch.setattr(adapter._api, "send_message", flaky_send_message)
    adapter_module._CURRENT_CALLER.set(caller)
    adapter_module._CURRENT_BINDING.set(binding)
    try:
        result = await adapter._send_with_retry("888", "ab")
        assert not result.success
        assert result.error_kind == "unknown"
        assert lease.lease_id in adapter._unknown_leases
    finally:
        adapter_module._CURRENT_BINDING.set(None)
        adapter_module._CURRENT_CALLER.set(None)
        await adapter.disconnect()


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


async def test_standalone_cron也使用OneBot纯文本格式(monkeypatch):
    """独立 cron 出站不能绕过 OneBot 默认纯文本合同。"""
    calls: list[str] = []

    async def fake_send_message(
        _api,
        _target_id: str,
        content: str,
        *,
        chat_type: str,
        reply_to: str | None = None,
    ) -> str:
        del chat_type, reply_to
        calls.append(content)
        return "cron-plain-text"

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
        "**cron 内容**",
    )
    assert result == {"success": True, "message_id": "cron-plain-text"}
    assert calls == ["cron 内容"]


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
async def test_内部completion绕过普通群触发直接创建恢复trigger(monkeypatch):
    """Hermes 异步 completion 不能因没有 @ 机器人而永久停在 pending。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    notifications: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(str(chat_id))
        return True

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    event = MessageEvent(
        text="[ASYNC DELEGATION BATCH COMPLETE — deleg_test]\\n截图任务结果",
        message_type="text",
        source=adapter.build_source(
            chat_id="888",
            chat_name="888",
            chat_type="group",
            user_id="123",
            user_name="小明",
            role_authorized=True,
        ),
        internal=True,
        metadata={"gateway_session_id": "session-parent"},
    )
    await adapter.handle_message(event)

    status = adapter._queue.status("888")
    assert status["pending"] == 1
    assert status["pending_trigger_requests"] == 1
    assert notifications == ["888"]
    trigger = adapter._queue.recover_trigger_requests({"888"})[0]
    assert trigger.reason == "completion_recovery"
    assert trigger.anchor_kind == "recovery"
    await adapter.disconnect()


async def test_重启为已入队内部completion补建trigger(monkeypatch, tmp_path):
    """旧 Hermes 已把 completion 入队但进程重启时仍必须自动唤醒。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_QUEUE_DB=str(tmp_path / "queue.sqlite3"),
    )
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="",
        user_id="123",
        user_name="小明",
        text="[ASYNC DELEGATION COMPLETE — deleg_old]\\n未完成截图",
        metadata={"gateway_session_id": "session-parent", "onebot11_images": []},
        message_key="hash:old-completion",
    )
    adapter._queue.enqueue(message)
    notifications: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(str(chat_id))
        return True

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    await adapter._recover_internal_completion_triggers()

    status = adapter._queue.status("888")
    assert status["pending"] == 1
    assert status["pending_trigger_requests"] == 1
    assert notifications == ["888"]
    trigger = adapter._queue.recover_trigger_requests({"888"})[0]
    assert trigger.reason == "completion_recovery"
    await adapter.disconnect()

async def test_内部completion不提升原用户权限且重复事件只保留一个trigger(monkeypatch):
    """内部回流只能使用原消息快照，重复投递不得制造第二个 anchor。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="999",
    )
    notifications: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(str(chat_id))
        return True

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    source = adapter.build_source(
        chat_id="888",
        chat_name="888",
        chat_type="group",
        user_id="123",
        user_name="普通用户",
        role_authorized=True,
    )
    metadata = {
        "gateway_session_id": "session-parent",
        "onebot11_authority": {
            "role": "super_admin",
            "allowed_tools": sorted(adapter.role_tools["super_admin"]),
            "self_id": "1",
        },
    }
    for _ in range(2):
        await adapter.handle_message(
            MessageEvent(
                text="[ASYNC DELEGATION COMPLETE — deleg_duplicate]\\n结果",
                message_type="text",
                source=source,
                internal=True,
                metadata=dict(metadata),
            )
        )

    status = adapter._queue.status("888")
    assert status["pending"] == 1
    assert status["pending_trigger_requests"] == 1
    assert notifications == ["888", "888"]
    trigger = adapter._queue.recover_trigger_requests({"888"})[0]
    assert trigger.authority_role == "user"
    assert trigger.authority_tools == frozenset()
    await adapter.disconnect()
async def test_缺失completion权限快照仍以最小权限恢复(monkeypatch, tmp_path):
    """重启恢复的 completion 不得从当前超级管理员身份继承工具。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
        ONEBOT11_QUEUE_DB=str(tmp_path / "queue.sqlite3"),
    )
    adapter._queue.enqueue(
        QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="",
            user_id="123",
            user_name="管理员",
            text="[ASYNC DELEGATION COMPLETE — deleg_missing]\\n结果",
            metadata={"gateway_session_id": "session-parent"},
            message_key="hash:missing-authority-completion",
        )
    )
    notifications: list[str] = []

    async def fake_notify(chat_id: str) -> bool:
        notifications.append(str(chat_id))
        return True

    monkeypatch.setattr(adapter._dispatcher, "notify", fake_notify)
    await adapter._recover_internal_completion_triggers()
    trigger = adapter._queue.recover_trigger_requests({"888"})[0]
    assert trigger.authority_role == "user"
    assert trigger.authority_tools == frozenset()
    assert notifications == ["888"]
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
        result = await adapter._send_with_retry("888", "你好")
        assert not result.success
        assert "lease-not-sent" in adapter._outbound_known_failure
        event = SimpleNamespace(
            metadata={"onebot11_lease_id": "lease-not-sent"},
            media_urls=[],
        )
        await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        # binding/lease 已经在 send 前被 fencing；旧 turn 不能再调用
        # dispatcher.complete，避免对新 owner 的持久状态做收口。
        assert completed == []
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


async def test_群聊中间正文按配置隐藏但最终回复不受影响(monkeypatch, fake_http_server):
    """Hermes 直调 send() 的中间正文在群聊默认隐藏；_send_with_retry 的最终回复始终发送。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    try:
        adapter._chat_types["888"] = "group"
        interim = await adapter.send("888", "让我查一下群消息…")
        assert interim.success
        assert calls == []  # 群聊中间正文被隐藏，不访问 OneBot

        final = await adapter._send_with_retry("888", "查到了，最近有 3 条消息")
        assert final.success
        assert calls and calls[0]["path"] == "/send_group_msg"
    finally:
        await adapter.disconnect()


async def test_私聊中间正文默认展示(monkeypatch, fake_http_server):
    """Hermes 直调 send() 的中间正文在私聊默认展示。"""
    base, calls = fake_http_server
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API=base,
        ONEBOT11_SELF_ID="1",
        ONEBOT11_DM_POLICY="allowlist",
        ONEBOT11_ALLOWED_USERS="1001",
    )
    await adapter.connect()
    try:
        adapter._chat_types["1001"] = "dm"
        result = await adapter.send("1001", "好的，我来处理…")
        assert result.success
        assert calls and calls[0]["path"] == "/send_private_msg"
    finally:
        await adapter.disconnect()


async def test_群聊中间正文可配置为展示(monkeypatch, fake_http_server):
    """show_interim_group=true 时群聊中间正文正常发送。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    adapter.show_interim_group = True
    await adapter.connect()
    try:
        adapter._chat_types["888"] = "group"
        result = await adapter.send("888", "让我查一下…")
        assert result.success
        assert calls and calls[0]["path"] == "/send_group_msg"
    finally:
        await adapter.disconnect()


async def test_队列turn在Hermes启动前发送即时确认(monkeypatch, fake_http_server):
    """客服群 turn 必须先收到适配器确认，再进入 Hermes 工具/模型循环。"""
    base, calls = fake_http_server
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API=base,
        ONEBOT11_SELF_ID="1",
        ONEBOT11_PROCESSING_REACTION_ENABLED="false",
    )
    adapter.show_interim_group = True
    await adapter.connect()
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="请查一下项目怎么启动",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
        message_key="group:1001",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "123",
            "小明",
            anchor_kind="hard",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    observed: list[str] = []

    async def capture_handle(_adapter, _event) -> None:
        assert calls, "Hermes handoff 前必须已经完成即时回执"
        assert "收到" not in _event.text
        observed.append("hermes")

    monkeypatch.setattr(BasePlatformAdapter, "handle_message", capture_handle)
    try:
        await adapter._start_queue_turn(lease)
        assert observed == ["hermes"]
        assert calls
        assert calls[0]["path"] == "/send_group_msg"
        first_message = calls[0]["params"]["message"]
        assert first_message[0]["type"] == "reply"
        assert "收到" in first_message[1]["data"]["text"]
    finally:
        await adapter.disconnect()


async def test_活动lease尚未建立正式binding时允许受限出站(monkeypatch, fake_http_server):
    """worker thread 的首次控制/中间出站不能因 binding 建立时序而被误拒绝。"""
    base, calls = fake_http_server
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API=base,
        ONEBOT11_SELF_ID="1",
        ONEBOT11_PROCESSING_REACTION_ENABLED="false",
    )
    await adapter.connect()
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="123",
        user_name="小明",
        text="需要先反馈",
        metadata={
            "onebot11_authority": {
                "role": "user",
                "allowed_tools": sorted(adapter.role_tools["user"]),
                "self_id": "1",
            }
        },
        message_key="group:1002",
    )
    adapter._queue.enqueue(
        message,
        adapter_module.TriggerRequest.create(
            "888",
            message.message_key,
            "mention",
            "123",
            "小明",
            anchor_kind="hard",
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
        ),
    )
    lease = adapter._queue.claim("888")
    assert lease is not None
    adapter._dispatcher._active["888"] = ActiveTurn(
        lease=lease,
        started_at=lease.claimed_at,
    )
    metadata = {
        "onebot11_managed_context": True,
        "onebot11_lease_id": lease.lease_id,
        "onebot11_target": {"chat_type": "group", "chat_id": "888"},
    }
    event = SimpleNamespace(metadata=metadata)
    event_token = adapter_module._CURRENT_EVENT.set(event)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        result = await adapter.send("888", "我先给你查一下", metadata=metadata)
        assert result.success
        assert calls and calls[0]["path"] == "/send_group_msg"
        assert adapter._bindings.snapshot() == {}
        monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
        blocked = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="",
            turn_id="",
            args={"command": "pwd"},
            platform="onebot11",
        )
        assert blocked == {
            "action": "block",
            "message": "OneBot11 current turn binding unavailable",
        }
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(context_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
        await adapter.disconnect()


async def test_回复以问句或请求收尾时标记bot_asked(monkeypatch, fake_http_server):
    """bot 回复问句/请求信息时，完成 turn 应进入 deep engaged 预算。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    try:
        assert adapter._reply_asks_user("请把报错日志发我") is True
        assert adapter._reply_asks_user("你能复现一下吗？") is True
        assert adapter._reply_asks_user("这是日志内容") is False
        assert adapter._reply_asks_user("好的，马上处理") is False
        assert adapter._reply_asks_user("") is False
    finally:
        await adapter.disconnect()


async def test_消息回复bot最后一条消息时识别为回复bot(monkeypatch, fake_http_server):
    """reply 目标等于 bot 最后发送的消息 ID 时视为引用 bot。"""
    base, calls = fake_http_server
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API=base, ONEBOT11_SELF_ID="1")
    await adapter.connect()
    try:
        adapter._last_bot_message_ids["888"] = "10086"
        assert adapter._message_replies_to_bot("888", "10086") is True
        assert adapter._message_replies_to_bot("888", "10087") is False
        assert adapter._message_replies_to_bot("888", "") is False
        assert adapter._message_replies_to_bot("999", "10086") is False
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
            authority_role="user",
            authority_tools=adapter.role_tools["user"],
            authority_self_id="1",
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


async def test_generic工具只有显式session_turn时仍执行OneBot角色门禁(monkeypatch):
    """Hermes 未传 ContextVar 时，精确 binding 仍不能放行普通用户终端工具。"""
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
        adapter_epoch=adapter._adapter_epoch,
    )
    adapter._bindings.bind(
        adapter_module.TurnBinding("generic-session", "generic-turn", caller)
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="generic-session",
            turn_id="generic-turn",
            args={"command": "whoami"},
        )
        assert result == {
            "action": "block",
            "message": "权限错误: 角色 user 无权调用 terminal",
        }
    finally:
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        await adapter.disconnect()


async def test_generic工具trusted_user显式授权时允许(monkeypatch):
    """trusted_user 只允许配置明确授予的 generic Hermes 工具。"""
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
        allowed_tools=frozenset({"terminal"}),
        self_id="1",
        adapter_epoch=adapter._adapter_epoch,
    )
    adapter._bindings.bind(
        adapter_module.TurnBinding("trusted-session", "trusted-turn", caller)
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    try:
        assert adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="trusted-session",
            turn_id="trusted-turn",
            args={"command": "pwd"},
        ) is None
    finally:
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        await adapter.disconnect()


async def test_main_agent只读模式放行委派但阻断terminal(monkeypatch):
    """只读主 agent 可以委派；直接 terminal 必须在 hook 层阻断。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._replace_policy(main_agent_read_only=True)
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="super_admin",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
        adapter_epoch=adapter._adapter_epoch,
    )
    binding = adapter_module.TurnBinding("readonly-session", "readonly-turn", caller)
    adapter._bindings.bind(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    binding_token = adapter_module._CURRENT_BINDING.set(binding)
    event_token = adapter_module._CURRENT_EVENT.set(None)
    try:
        assert adapter_module._pre_tool_call_hook(
            tool_name="search_files",
            session_id=binding.session_id,
            turn_id=binding.turn_id,
            args={"pattern": "delegate_task"},
        ) is None
        assert adapter_module._pre_tool_call_hook(
            tool_name="delegate_task",
            session_id=binding.session_id,
            turn_id=binding.turn_id,
            args={"goal": "检查项目"},
        ) is None
        blocked = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id=binding.session_id,
            turn_id=binding.turn_id,
            args={"command": "rg TODO ."},
        )
        assert blocked == {
            "action": "block",
            "message": "权限错误: 当前 OneBot 主 agent 处于只读模式，不能调用 terminal",
        }
    finally:
        adapter_module._CURRENT_EVENT.reset(event_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        await adapter.disconnect()


async def test_delegated_child只允许项目工具且拒绝QQ(monkeypatch):
    """Hermes delegated-child context 可用 shell，但不能借父身份调用 QQ。"""
    from agent.delegation_context import delegated_child_context

    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._replace_policy(main_agent_read_only=True)
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="super_admin",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
        adapter_epoch=adapter._adapter_epoch,
    )
    binding = adapter_module.TurnBinding("parent-session", "parent-turn", caller)
    adapter._bindings.bind(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    binding_token = adapter_module._CURRENT_BINDING.set(binding)
    event_token = adapter_module._CURRENT_EVENT.set(None)
    onebot_context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        with delegated_child_context("child-session"):
            assert adapter_module._pre_tool_call_hook(
                tool_name="terminal",
                session_id="child-session",
                turn_id="child-turn",
                args={"command": "rg -n TODO ."},
            ) is None
            blocked = adapter_module._pre_tool_call_hook(
                tool_name="qq_get_message",
                session_id="child-session",
                turn_id="child-turn",
                args={"message_id": "1"},
            )
            assert blocked == {
                "action": "block",
                "message": "权限错误: OneBot11 子代理不能直接调用 QQ 工具，请由主 agent 处理",
            }
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        await adapter.disconnect()


async def test_无父binding的子代理仍然拒绝OneBot工具(monkeypatch):
    """delegated-child 标记不能单独伪造 OneBot 父 turn 身份。"""
    from agent.delegation_context import delegated_child_context

    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._replace_policy(main_agent_read_only=True)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    event_token = adapter_module._CURRENT_EVENT.set(None)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    onebot_context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        with delegated_child_context("orphan-child"):
            blocked = adapter_module._pre_tool_call_hook(
                tool_name="terminal",
                session_id="orphan-child",
                turn_id="orphan-turn",
                args={"command": "pwd"},
            )
        assert blocked == {
            "action": "block",
            "message": "OneBot11 current turn binding unavailable",
        }
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
        await adapter.disconnect()


async def test_普通平台子代理不受OneBot全局hook影响(monkeypatch):
    """没有 OneBot lineage 时，通用 Hermes 子代理仍由 Hermes 自己授权。"""
    from agent.delegation_context import delegated_child_context

    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    event_token = adapter_module._CURRENT_EVENT.set(None)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    onebot_context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(False)
    try:
        with delegated_child_context("other-platform-child"):
            assert adapter_module._pre_tool_call_hook(
                tool_name="terminal",
                session_id="other-platform-child",
                turn_id="other-turn",
                args={"command": "pwd"},
                platform="discord",
            ) is None
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
        await adapter.disconnect()


async def test_只读模式read_file敏感文件被拦截(monkeypatch):
    """主 agent 只读可以读代码，但不能把 .env/auth.json 读进上下文。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    adapter._replace_policy(main_agent_read_only=True)
    caller = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="super_admin",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
        adapter_epoch=adapter._adapter_epoch,
    )
    binding = adapter_module.TurnBinding("readonly-session", "readonly-turn", caller)
    adapter._bindings.bind(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    binding_token = adapter_module._CURRENT_BINDING.set(binding)
    event_token = adapter_module._CURRENT_EVENT.set(None)
    onebot_context_token = adapter_module._CURRENT_ONEBOT_CONTEXT.set(True)
    try:
        assert adapter_module._pre_tool_call_hook(
            tool_name="read_file",
            session_id=binding.session_id,
            turn_id=binding.turn_id,
            args={"path": "/opt/data/repo/neuro-book/README.md"},
        ) is None
        blocked = adapter_module._pre_tool_call_hook(
            tool_name="read_file",
            session_id=binding.session_id,
            turn_id=binding.turn_id,
            args={"path": "/opt/data/.env"},
        )
        assert blocked == {
            "action": "block",
            "message": "权限错误: OneBot11 拒绝读取安全敏感凭据文件（.env / auth.json / auth.lock）",
        }
    finally:
        adapter_module._CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        await adapter.disconnect()


async def test_同一候选queued_reaction失败后不重复调用(monkeypatch):
    """同一条候选消息 reaction 失败后不再反复调用 OneBot API。"""
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
    calls: list[tuple[str, str, bool]] = []

    async def failing_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))
        raise adapter_module.OneBotApiError("set_msg_emoji_like", "failed", 0)

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", failing_reaction)
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="这个问题怎么处理？",
        message_key="group:1001",
    )
    adapter._queue.enqueue(message)
    try:
        adapter._schedule_queued_reaction("888", message)
        await asyncio.sleep(0.05)
        assert calls == [("1001", "128064", True)]
        adapter._schedule_queued_reaction("888", message)
        adapter._schedule_queued_reaction("888", message)
        await asyncio.sleep(0.05)
        assert len(calls) == 1
        next_message = QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="1002",
            user_id="123",
            user_name="小明",
            text="换个问题",
            message_key="group:1002",
        )
        adapter._queue.enqueue(next_message)
        adapter._schedule_queued_reaction("888", next_message)
        await asyncio.sleep(0.05)
        assert calls[-1] == ("1002", "128064", True)
        assert len(calls) == 2
        adapter._schedule_clear_queued_reaction("888", reset_attempted=True)
        adapter._schedule_queued_reaction("888", message)
        await asyncio.sleep(0.05)
        assert len(calls) == 3
    finally:
        await adapter.disconnect()


async def test_hook拒绝事件metadata与显式turn不一致(monkeypatch):
    """一个 OneBot 事件不能借用同一适配器中的另一个 turn binding。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    caller_a = adapter_module.CallerContext(
        user_id="123",
        chat_type="group",
        chat_id="888",
        role="user",
        allowed_tools=adapter_module.READ_ONLY_TOOLS,
        self_id="1",
        adapter_epoch=adapter._adapter_epoch,
    )
    caller_b = adapter_module.CallerContext(
        user_id="456",
        chat_type="group",
        chat_id="888",
        role="trusted_user",
        allowed_tools=frozenset({"terminal"}),
        self_id="1",
        adapter_epoch=adapter._adapter_epoch,
    )
    binding_a = adapter_module.TurnBinding("event-session", "event-turn", caller_a)
    binding_b = adapter_module.TurnBinding("explicit-session", "explicit-turn", caller_b)
    adapter._bindings.bind(binding_a)
    adapter._bindings.bind(binding_b)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    event = SimpleNamespace(
        source=SimpleNamespace(platform="onebot11"),
        metadata={
            "onebot11_binding_key": {
                "session_id": binding_a.session_id,
                "turn_id": binding_a.turn_id,
            }
        },
    )
    event_token = adapter_module._CURRENT_EVENT.set(event)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id=binding_b.session_id,
            turn_id=binding_b.turn_id,
            args={"command": "pwd"},
        )
        assert result == {
            "action": "block",
            "message": "OneBot11 event metadata 与显式 turn binding 冲突",
        }
    finally:
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter_module._CURRENT_EVENT.reset(event_token)
        await adapter.disconnect()


async def test_pre_llm显式坐标缺少ContextVar仍绑定OneBot_turn(monkeypatch):
    """worker thread 没有 ContextVar 时，精确 session/turn 仍必须建立 binding。"""
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
        adapter_epoch=adapter._adapter_epoch,
    )
    binding = adapter_module.TurnBinding("pre-llm-session", "pre-llm-turn", caller)
    adapter._bindings.bind(binding)
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    caller_token = adapter_module._CURRENT_CALLER.set(None)
    binding_token = adapter_module._CURRENT_BINDING.set(None)
    event_token = adapter_module._CURRENT_EVENT.set(None)
    try:
        result = adapter_module._pre_llm_call_hook(
            session_id="pre-llm-session",
            turn_id="pre-llm-turn",
        )
        assert result is not None
        assert "角色：user" in result["context"]
        assert adapter_module._CURRENT_BINDING.get() == binding
    finally:
        adapter_module._CURRENT_EVENT.reset(event_token)
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        adapter._bindings.discard("pre-llm-session", "pre-llm-turn")
        await adapter.disconnect()


async def test_plugin工具缺少一个turn坐标时fail_closed(monkeypatch):
    """handler 不能用 ContextVar 补齐显式缺失的 session/turn 坐标。"""
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
        adapter_epoch=adapter._adapter_epoch,
    )
    binding = adapter_module.TurnBinding("handler-session", "handler-turn", caller)
    adapter._bindings.bind(binding)
    caller_token = adapter_module._CURRENT_CALLER.set(caller)
    binding_token = adapter_module._CURRENT_BINDING.set(binding)
    try:
        handler = adapter._make_tool_handler("qq_get_message")
        result = await handler(
            {"message_id": "1"},
            session_id="handler-session",
        )
        assert json.loads(result) == {
            "status": "permission_error",
            "error": "当前 turn 身份绑定不存在",
        }
    finally:
        adapter_module._CURRENT_BINDING.reset(binding_token)
        adapter_module._CURRENT_CALLER.reset(caller_token)
        await adapter.disconnect()


async def test_generic工具明确其他平台但撞OneBotbinding时拒绝(monkeypatch):
    """其他平台不能借用同一组 session/turn ID 跨入 OneBot binding。"""
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
        adapter_epoch=adapter._adapter_epoch,
    )
    adapter._bindings.bind(
        adapter_module.TurnBinding("collision-session", "collision-turn", caller)
    )
    monkeypatch.setattr(adapter_module, "_get_live_adapter", lambda: adapter)
    try:
        result = adapter_module._pre_tool_call_hook(
            tool_name="terminal",
            session_id="collision-session",
            turn_id="collision-turn",
            platform="discord",
            args={},
        )
        assert result == {
            "action": "block",
            "message": "OneBot11 platform 与 binding 冲突",
        }
    finally:
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


async def test_resolve_retry缺少trigger时保持hold(monkeypatch):
    """旧 anchor 无法证明时，管理员 retry 不得猜测 authority 重新启动 turn。"""
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
    assert adapter._queue.status("888")["pending_trigger_requests"] == 0
    assert responses and "无法证明" in responses[0]
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


async def test_确认后的参数错误是known_failed而不是unknown(monkeypatch):
    """参数转换在 OneBot 请求前失败时，不应要求人工处理未知出站。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    confirmation = SimpleNamespace(
        token="test-token",
        tool_name="qq_set_group_ban",
        params={"user_id": "not-a-number", "duration": 60},
        user_id="123",
        chat_type="group",
        chat_id="888",
    )
    result = await adapter._execute_confirmed(confirmation)
    assert result["status"] == "error"
    record = adapter._queue.operation_records("888")[0]
    assert record.status == "known_failed"
    assert adapter._queue.unknown_operation_count("888") == 0
    await adapter.disconnect()


async def test_adapter关闭后确认执行fail_closed(monkeypatch):
    """迟到的确认入口不能在 adapter 关闭后访问 OneBot。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_SUPER_ADMINS="123",
    )
    confirmation = SimpleNamespace(
        token="test-token",
        tool_name="qq_set_group_ban",
        params={"user_id": "456", "duration": 60},
        user_id="123",
        chat_type="group",
        chat_id="888",
    )
    await adapter.disconnect()
    result = await adapter._execute_confirmed(confirmation)
    assert result == {"status": "permission_error", "error": "OneBot11 adapter 已关闭"}


def test_check_requirements(monkeypatch):
    """依赖检查不读取部署配置；配置合同由 validate_config 负责。"""
    assert check_requirements()


def test_check_requirements_roles文件存在时要求PyYAML(monkeypatch, tmp_path):
    """独立 roles 文件存在但缺 PyYAML 时依赖检查必须失败。"""
    import sys

    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    (hermes_home / "onebot11").mkdir()
    (hermes_home / "onebot11" / "roles.yaml").write_text(
        "super_admins: ['1']\nroles: {}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    # 模拟 PyYAML 未安装：把 yaml 从 sys.modules 移除，import 会抛 ImportError。
    monkeypatch.setitem(sys.modules, "yaml", None)
    assert check_requirements() is False


def test_check_requirements_无roles文件时不强制PyYAML(monkeypatch, tmp_path):
    """roles 文件缺失时，PyYAML 不可用不应阻止基础依赖通过。"""
    import sys

    hermes_home = tmp_path / "hermes-empty-home"
    hermes_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setitem(sys.modules, "yaml", None)
    assert check_requirements() is True


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
            self.hooks: dict[str, object] = {}
            self.skills: list[tuple[str, object, str]] = []

        def register_platform(self, **kwargs):
            self.platform_kwargs = kwargs

        def register_tool(self, **kwargs):
            self.tools.append(kwargs)

        def register_hook(self, name, callback):
            self.hooks[name] = callback

        def register_skill(self, name, path, description):
            self.skills.append((name, path, description))

    ctx = FakeCtx()
    register(ctx)
    assert len(ctx.skills) == 1
    name, skill_path, description = ctx.skills[0]
    assert name == "repository-research"
    assert skill_path.exists()
    text = skill_path.read_text(encoding="utf-8")
    assert "name: repository-research" in text
    assert "ONEBOT11_" not in text
    assert "qq_" not in text
    assert "NeuroBook" not in text
    assert description

    assert ctx.platform_kwargs is not None
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
    assert {
        "pre_gateway_dispatch",
        "pre_llm_call",
        "pre_tool_call",
    } <= set(ctx.hooks)


def test_register缺少安全hook时拒绝启用():
    """没有 Hermes hook 能力时不能静默注册一个可能 fail-open 的平台。"""
    class MissingHookCtx:
        def register_platform(self, **_kwargs):
            raise AssertionError("安全 hook 检查应先于平台注册")

        def register_tool(self, **_kwargs):
            raise AssertionError("安全 hook 检查应先于工具注册")

    with pytest.raises(RuntimeError, match="register_hook"):
        register(MissingHookCtx())


def test_register缺少任一关键hook时拒绝启用(monkeypatch):
    """三个关键 hook 任一不可用都必须在平台注册前 fail-closed。"""
    import hermes_cli.plugins as plugins

    class FakeCtx:
        """提供注册接口，确保失败原因来自 capability gate。"""

        def register_platform(self, **_kwargs):
            """测试不应进入平台注册阶段。"""

        def register_tool(self, **_kwargs):
            """测试不应进入工具注册阶段。"""

        def register_hook(self, _name, _callback):
            """测试不应进入 hook 注册阶段。"""

    for missing in ("pre_gateway_dispatch", "pre_llm_call", "pre_tool_call"):
        monkeypatch.setattr(
            plugins,
            "VALID_HOOKS",
            set(plugins.VALID_HOOKS) - {missing},
        )
        with pytest.raises(RuntimeError, match=missing):
            register(FakeCtx())


async def test_selector连续失败达到上限后放弃自动重试(monkeypatch):
    """llm_failure_count 达到上限后不再启动 judge task，清理 👀 并审计。"""
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
                    "max_failures": 3,
                },
            },
        )
    )
    calls: list[tuple[str, str, bool]] = []

    async def fake_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        calls.append((message_id, emoji_id, enabled))

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", fake_reaction)
    audit_records: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        adapter._audit,
        "record",
        lambda event, data: audit_records.append((event, data)),
    )
    try:
        # 先登记一条候选消息，让 give_up 有可清理的 queued reaction。
        message = QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="1007",
            user_id="123",
            user_name="小明",
            text="今天星期几",
            message_key="group:1007",
        )
        caller = adapter_module.CallerContext(
            user_id="123",
            chat_type="group",
            chat_id="888",
            role="user",
            allowed_tools=adapter_module.READ_ONLY_TOOLS,
            self_id="1",
        )
        adapter._trigger_state_for("888").last_message_at = time.monotonic() - 1
        await adapter._enqueue_group_message(
            message,
            mentioned_self=False,
            caller=caller,
            user_name="小明",
        )
        await asyncio.sleep(0.05)
        # 连续 3 次失败达到上限（不经过 judge task，直接写持久状态）。
        for _ in range(3):
            await asyncio.to_thread(
                adapter._queue.mark_llm_failure,
                "888",
                observed_seq=1,
                error="timeout",
                next_attempt_at=time.time() + 60,
            )
        state = adapter._trigger_states["888"]
        state.mode = "judging"
        state._judgement_generation = 1
        action = adapter_module.TriggerAction(
            "judge",
            reason="debounce_due",
            candidate_type="question",
            revision=1,
            generation=1,
        )
        await adapter._start_llm_judgement("888", action)
        # 不再创建 judge task，也不再消耗模型。
        assert "888" not in adapter._llm_trigger_tasks
        assert any(
            event == "llm_trigger" and data.get("failure") == "give_up"
            for event, data in audit_records
        )
        # 👀 不残留：即使候选添加被抢先执行，最后也必须是移除调用。
        await asyncio.sleep(0.1)
        assert calls == [] or calls[-1] == ("1007", "128064", False)
        # 消息保留 pending，不被 give_up 删除。
        assert adapter._queue.status("888")["pending"] == 1
    finally:
        await adapter.disconnect()


async def test_selector_queued_reaction_移除失败重试一次(monkeypatch):
    """queued reaction 移除失败后短延迟重试一次，仍失败则放弃。"""
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
    attempts: list[bool] = []

    async def flaky_reaction(message_id: str, emoji_id: str, *, enabled: bool) -> None:
        attempts.append(enabled)
        if len(attempts) == 1:
            raise adapter_module.OneBotApiError(
                action="set_msg_emoji_like",
                status="failed",
                retcode=-1,
            )

    monkeypatch.setattr(adapter._api, "set_message_emoji_like", flaky_reaction)
    try:
        adapter._queued_reaction_message_ids["888"] = ("group:1008", "1008")
        await adapter._unset_queued_reaction_entry("888", ("group:1008", "1008"))
        # 第一次失败 + 2 秒后重试成功，共两次移除调用。
        assert attempts == [False, False]
        assert "888" not in adapter._queued_reaction_message_ids
    finally:
        await adapter.disconnect()


def test_工具进度输出中文且脱敏(monkeypatch):
    """结构化工具事件只展示安全摘要，不泄露命令、路径或凭据。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    event = ToolCallChunk(
        tool_name="terminal",
        preview="cat /srv/neuro-book/.env API_KEY=sk-live-very-secret-value",
        args={
            "command": "cat /srv/neuro-book/.env API_KEY=sk-live-very-secret-value",
        },
    )
    rendered = adapter.format_tool_event(event, mode="all", preview_max_len=80)
    assert rendered is not None
    assert rendered.startswith("正在执行")
    assert "/srv/neuro-book" not in rendered
    assert "sk-live-very-secret-value" not in rendered
    assert ".env" not in rendered
    asyncio.run(adapter.disconnect())


def test_工具进度连续相同内容去重(monkeypatch):
    """相同工具摘要连续到达时只保留一次展示。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    event = ToolCallChunk(
        tool_name="read_file",
        preview="README.md",
        args={"path": "README.md"},
    )
    assert adapter.format_tool_event(event) is not None
    assert adapter.format_tool_event(event) is None
    asyncio.run(adapter.disconnect())
