"""确定性触发器矩阵测试。"""

import pytest

from onebot11.triggers import TriggerConfig, build_trigger_config, should_trigger


def test_mention和关键词触发():
    """@ 和 Unicode casefold 关键词都能触发群 turn。"""
    config = TriggerConfig(require_mention=True, keywords=("请回答",))
    assert should_trigger(
        chat_type="group", text="在吗", mentioned_self=True, config=config
    ).reason == "mention"
    assert should_trigger(
        chat_type="group", text="请回答这个", mentioned_self=False, config=config
    ).reason == "keyword"


def test_always和兼容模式触发():
    """always 或 require_mention=false 时普通群消息创建 trigger。"""
    assert should_trigger(
        chat_type="group", text="普通消息", mentioned_self=False,
        config=TriggerConfig(always=True),
    ).triggered
    assert should_trigger(
        chat_type="group", text="普通消息", mentioned_self=False,
        config=TriggerConfig(require_mention=False),
    ).reason == "always"


def test_私聊直接触发且冷却生效():
    """私聊不需要 @；冷却只抑制新的 turn，不影响入队。"""
    config = TriggerConfig(cooldown_seconds=10)
    assert should_trigger(
        chat_type="dm", text="hi", mentioned_self=False, config=config, now=20
    ).triggered
    decision = should_trigger(
        chat_type="dm", text="again", mentioned_self=False,
        config=config, last_trigger_at=15, now=20,
    )
    assert not decision.triggered
    assert decision.reason == "cooldown"


def test_触发配置严格解析():
    """拒绝 bool('false') 和非 list/string 的关键词配置。"""
    config = build_trigger_config({"require_mention": "false", "trigger_keywords": ["你好"]})
    assert not config.require_mention
    assert config.keywords == ("你好",)
    with pytest.raises(ValueError):
        build_trigger_config({"require_mention": "not-a-bool"})
    with pytest.raises(ValueError):
        build_trigger_config({"trigger_keywords": {"nested": "no"}})
