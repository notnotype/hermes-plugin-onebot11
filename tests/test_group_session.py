"""群会话粒度:默认一群一会话,可配置 per-user。"""

import pytest

pytest.importorskip("gateway.platforms.base")

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
from gateway.session import build_session_key  # noqa: E402

from adapter import OneBot11Adapter  # noqa: E402


def _make_adapter_shared(monkeypatch, **env) -> OneBot11Adapter:
    """与 test_adapter 同款构造,避免循环 import。"""
    env.setdefault("ONEBOT11_WS_PORT", "0")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return OneBot11Adapter(PlatformConfig(enabled=True, extra={}))


def _key_for(adapter: OneBot11Adapter, user_id: str) -> str:
    """按 adapter 的会话粒度配置生成 session key。"""
    return build_session_key(
        adapter.build_source(
            chat_id="888", chat_name="888", chat_type="group",
            user_id=user_id, user_name="小明",
        ),
        group_sessions_per_user=adapter.config.extra.get("group_sessions_per_user", True),
    )


def test_群会话默认一群一会话(monkeypatch):
    """默认 group_sessions_per_user=false:同群不同用户同 session key。"""
    adapter = _make_adapter_shared(monkeypatch)
    assert _key_for(adapter, "1") == _key_for(adapter, "2")


def test_开启per_user后同群不同用户不同key(monkeypatch):
    adapter = _make_adapter_shared(monkeypatch, ONEBOT11_GROUP_SESSIONS_PER_USER="true")
    assert _key_for(adapter, "1") != _key_for(adapter, "2")
