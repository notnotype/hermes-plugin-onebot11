"""adapter.py 冒烟测试。

需要 hermes gateway 可导入（本地跑：用 hermes venv + PYTHONPATH）；
CI 环境没有 gateway 时自动跳过。
"""

import asyncio

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
from gateway.platforms.base import BasePlatformAdapter, SendResult  # noqa: E402

from adapter import OneBot11Adapter, check_requirements, register  # noqa: E402
from onebot11.events import InboundEvent  # noqa: E402


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


def test_缺HTTP配置时connect失败(monkeypatch):
    adapter = _make_adapter(monkeypatch)  # 没有 ONEBOT11_HTTP_API
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


async def test_私聊事件不加前缀(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    event = await adapter._build_message_event(
        InboundEvent(text="在吗", chat_id="123", chat_type="dm", user_id="123", user_name="小明", message_id="1")
    )
    assert event.text == "在吗"
    assert event.source.chat_type == "dm"


async def test_入站群聊事件进入handle_message(monkeypatch):
    adapter = _make_adapter(monkeypatch, ONEBOT11_HTTP_API="http://127.0.0.1:3000", ONEBOT11_SELF_ID="1")
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
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
    assert len(recorded) == 1
    assert recorded[0].source.chat_id == "888"
    assert recorded[0].text == "[小明] 在吗"


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


def _fake_runner(adapter: OneBot11Adapter, user_id: str, chat_type: str, chat_id: str) -> None:
    """给 adapter 挂一个假的 gateway runner,让 _resolve_tool_context 能取到会话来源。"""
    from gateway.config import Platform
    from gateway.session import SessionSource

    class _FakeRunner:
        def _get_cached_session_source(self, session_id: str):
            return SessionSource(
                platform=Platform("onebot11"),
                chat_id=chat_id,
                chat_type=chat_type,
                user_id=user_id,
            )

    adapter.gateway_runner = _FakeRunner()


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


async def test_群聊未触发消息只入队不响应(monkeypatch):
    """v2: 未 @ 未命中关键词的群消息只进队列,不进入会话。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(888, at_self=False))
    assert recorded == []
    assert len(adapter._queue.snapshot("888")) == 1


async def test_群聊at机器人放行并清空队列(monkeypatch):
    """@ 了机器人的群消息触发并消费队列。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(888, at_self=True))
    assert len(recorded) == 1
    assert adapter._queue.snapshot("888") == []


async def test_关键词触发进入会话并清空队列(monkeypatch):
    """关键词命中触发;未命中消息积累在队列,触发后被消费。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_KEYWORD_TRIGGERS="机器人",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    # 第一条未触发 → 入队
    await adapter._on_ws_event(_group_raw(888, text="今天天气不错", at_self=False))
    assert recorded == [] and len(adapter._queue.snapshot("888")) == 1
    # 第二条命中关键词 → 触发,队列被消费
    await adapter._on_ws_event(_group_raw(888, text="机器人帮我查一下", at_self=False))
    assert len(recorded) == 1
    assert adapter._queue.snapshot("888") == []


async def test_触发消息带群聊上下文前缀(monkeypatch):
    """触发时,触发前的队列消息拼进上下文。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_KEYWORD_TRIGGERS="机器人",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(888, text="第一条", at_self=False))
    await adapter._on_ws_event(_group_raw(888, text="机器人触发", at_self=False))
    text = recorded[0].text
    assert "群聊上下文" in text and "第一条" in text and "当前消息" in text


async def test_require_mention不影响私聊(monkeypatch):
    """私聊消息不受 require_mention 限制。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
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


async def test_admin工具普通用户调用被拒(monkeypatch):
    """admin-only 工具被普通用户调用时返回权限错误。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ADMINS="10001",
        ONEBOT11_ADMIN_TOOLS="qq_get_message",
    )
    _fake_runner(adapter, user_id="99999", chat_type="group", chat_id="888")
    handler = adapter._make_tool_handler("qq_get_message")
    result = await handler(args={}, task_id="t", session_id="s1", user_task="u")
    assert "仅管理员可用" in result


async def test_admin工具管理员调用放行(monkeypatch, fake_http_server):
    """admin-only 工具被管理员调用时越过角色守卫,正常执行。"""
    base, calls = fake_http_server
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API=base,
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ADMINS="10001",
        ONEBOT11_ADMIN_TOOLS="qq_get_message",
    )
    _fake_runner(adapter, user_id="10001", chat_type="group", chat_id="888")
    handler = adapter._make_tool_handler("qq_get_message")
    result = await handler(args={"message_id": 1}, task_id="t", session_id="s1", user_task="u")
    assert "仅管理员可用" not in result
    assert calls and calls[0]["path"] == "/get_msg"


async def test_普通用户触发消息带角色提示(monkeypatch):
    """存在 admin 工具时,普通用户触发消息注入角色说明。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ADMINS="10001",
        ONEBOT11_ADMIN_TOOLS="qq_ban_member",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(888, at_self=True))  # user_id=123 非管理员
    assert "仅管理员可用工具" in recorded[0].text


async def test_管理员触发消息不带角色提示(monkeypatch):
    """管理员触发时不需要角色提示。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_ADMINS="123",
        ONEBOT11_ADMIN_TOOLS="qq_ban_member",
    )
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(888, at_self=True))  # user_id=123 是管理员
    assert "仅管理员可用工具" not in recorded[0].text


async def test_llm触发开关接线(monkeypatch):
    """ONEBOT11_LLM_TRIGGER=true 时 judge 回调被调用(手动替换为假 judge)。"""
    adapter = _make_adapter(
        monkeypatch,
        ONEBOT11_HTTP_API="http://127.0.0.1:3000",
        ONEBOT11_SELF_ID="1",
        ONEBOT11_LLM_TRIGGER="true",
    )
    calls: list = []

    async def fake_judge(chat_id: str, snapshot: str, current: str) -> bool:
        calls.append((chat_id, current))
        return current.startswith("帮我")

    adapter._trigger.llm_judge = fake_judge
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(888, text="帮我查一下", at_self=False))
    assert len(calls) == 1 and calls[0][0] == "888"
    assert len(recorded) == 1


class _FakeLlm:
    """假 PluginLlm:记录调用,返回固定文本。"""

    def __init__(self, answer: str = "true") -> None:
        self.answer = answer
        self.calls: list = []

    async def acomplete(self, **kwargs) -> object:
        self.calls.append(kwargs)
        from types import SimpleNamespace

        return SimpleNamespace(text=self.answer)


async def test_llm触发真实接线(monkeypatch):
    """ONEBOT11_LLM_TRIGGER=true + llm_facade 存在时,judge 自动接上并触发。"""
    from gateway.config import PlatformConfig

    fake = _FakeLlm(answer="true")
    monkeypatch.setenv("ONEBOT11_WS_PORT", "0")
    monkeypatch.setenv("ONEBOT11_HTTP_API", "http://127.0.0.1:3000")
    monkeypatch.setenv("ONEBOT11_SELF_ID", "1")
    monkeypatch.setenv("ONEBOT11_LLM_TRIGGER", "true")
    adapter = OneBot11Adapter(PlatformConfig(enabled=True, extra={}), llm_facade=fake)
    assert adapter._trigger.llm_judge is not None
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(888, text="帮我查一下", at_self=False))
    assert fake.calls  # LLM 判定被调用
    assert len(recorded) == 1


async def test_llm触发false不接线(monkeypatch):
    """ONEBOT11_LLM_TRIGGER=false 时即使有 facade 也不接 judge。"""
    from gateway.config import PlatformConfig

    fake = _FakeLlm()
    monkeypatch.setenv("ONEBOT11_WS_PORT", "0")
    monkeypatch.setenv("ONEBOT11_HTTP_API", "http://127.0.0.1:3000")
    monkeypatch.setenv("ONEBOT11_SELF_ID", "1")
    adapter = OneBot11Adapter(PlatformConfig(enabled=True, extra={}), llm_facade=fake)
    assert adapter._trigger.llm_judge is None
    assert adapter._ctx_summarizer is None


async def test_群白名单内群放行(monkeypatch):
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
    await adapter._on_ws_event(_group_raw(888))
    assert len(recorded) == 1
    assert recorded[0].source.chat_id == "888"


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
    recorded: list = []

    async def fake_handle(event):
        recorded.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle)
    await adapter._on_ws_event(_group_raw(777))
    assert len(recorded) == 1


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


def test_check_requirements(monkeypatch):
    assert not check_requirements()
    monkeypatch.setenv("ONEBOT11_HTTP_API", "http://127.0.0.1:3000")
    monkeypatch.setenv("ONEBOT11_SELF_ID", "1")
    assert check_requirements()


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
    names = {t["name"] for t in ctx.tools}
    assert names == {"qq_get_message", "qq_get_group_msg_history", "qq_get_friend_msg_history"}
    for t in ctx.tools:
        assert t["toolset"] == "onebot11"
        assert t["is_async"] is True
