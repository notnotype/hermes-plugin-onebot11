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
