"""OneBot 11 运行时配置解析合同测试。"""

import pytest

from onebot11.config import parse_runtime_config


def _extra(**overrides: object) -> dict[str, object]:
    """构造最小可启用的 YAML extra。"""
    extra: dict[str, object] = {
        "http_api": "http://127.0.0.1:3000",
        "self_id": "3101482118",
    }
    extra.update(overrides)
    return extra


def test_yaml配置和构造使用同一数值解析器():
    """不合法数值不能等到 adapter 构造时才暴露。"""
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(queue_lease_seconds="not-a-number"))
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(queue_max_messages=float("nan")))
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(http_timeout_seconds=float("inf")))


def test环境变量覆盖YAML且布尔值严格解析():
    """环境覆盖保留优先级，但不能把 false 当成 true。"""
    runtime = parse_runtime_config(
        _extra(
            processing_reaction_enabled=True,
            allowed_groups=["888"],
        ),
        {
            "ONEBOT11_PROCESSING_REACTION_ENABLED": "false",
            "ONEBOT11_ALLOWED_GROUPS": "1072992996",
        },
    )
    assert runtime.processing_reaction_enabled is False
    assert runtime.access_policy.allowed_groups == frozenset({"1072992996"})

    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(),
            {"ONEBOT11_GROUP_SESSIONS_PER_USER": "true"},
        )


def test未知策略和错误YAML列表fail_closed():
    """策略和角色工具的类型错误不能被字符串化吞掉。"""
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(dm_policy="unknown"))
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(roles={"user": {"tools": {"bad": "shape"}}}))


def test超级管理员返回统一解析结果并拒绝mapping():
    """adapter 与 validate_config 必须共享同一份严格管理员配置。"""
    runtime = parse_runtime_config(_extra(super_admins=["2056963663"]))
    assert runtime.super_admins == frozenset({"2056963663"})
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(super_admins={"user": "2056963663"}))


def testws_host拒绝URL格式():
    """监听地址只接受 hostname/IP，不能把 URL 延迟到启动阶段才失败。"""
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(ws_host="http://127.0.0.1", access_token="token"))


def test启用llm_trigger必须显式配置群allowlist():
    """旁路模型不能在未限制群范围时隐式接管所有群。"""
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(
                llm_trigger={
                    "enabled": True,
                    "provider": "provider",
                    "model": "model",
                }
            )
        )


def test角色未知工具直接拒绝():
    """角色配置不能通过静默取交集把拼写错误隐藏掉。"""
    with pytest.raises(ValueError, match="未知工具"):
        parse_runtime_config(
            _extra(roles={"user": {"tools": ["qq_not_a_real_tool"]}})
        )
