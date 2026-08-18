"""OneBot 11 运行时配置解析合同测试。"""

import os
from pathlib import Path

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


def test_roles文件覆盖配置角色(tmp_path, monkeypatch):
    """独立 roles 文件存在时作为 super_admins/roles 的事实来源。"""
    roles_path = tmp_path / "roles.yaml"
    roles_path.write_text(
        "\n".join(
            (
                "super_admins: ['2056963663']",
                "roles:",
                "  trusted_user:",
                "    users: ['1259901822', '1336488699']",
                "    tools: [qq_get_message, image_generate]",
                "  user:",
                "    tools: [qq_get_message]",
                "  super_admin:",
                "    tools: [qq_get_message, qq_delete_message]",
            )
        ),
        encoding="utf-8",
    )
    env = {"ONEBOT11_ROLES_FILE": str(roles_path)}
    runtime = parse_runtime_config(
        _extra(
            super_admins=["old-admin"],
            roles={
                "trusted_user": {"users": ["111"], "tools": ["qq_get_message"]},
                "user": {"tools": []},
                "super_admin": {"tools": []},
            },
        ),
        env,
    )
    assert runtime.super_admins == frozenset({"2056963663"})
    assert runtime.trusted_users == frozenset({"1259901822", "1336488699"})
    assert "image_generate" in runtime.role_tools["trusted_user"]
    assert runtime.role_tools["user"] == frozenset({"qq_get_message"})
    assert "qq_delete_message" in runtime.role_tools["super_admin"]


def test_roles文件缺失时回退配置角色(tmp_path, monkeypatch):
    """没有独立 roles 文件时保持 config.yaml 的角色配置。"""
    env = {"ONEBOT11_ROLES_FILE": str(tmp_path / "missing.yaml")}
    runtime = parse_runtime_config(
        _extra(
            super_admins=["2056963663"],
            roles={"user": {"tools": ["qq_get_message"]}},
        ),
        env,
    )
    assert runtime.super_admins == frozenset({"2056963663"})
    assert runtime.role_tools["user"] == frozenset({"qq_get_message"})


def test_roles文件非法结构和未知键fail_closed(tmp_path):
    """roles 文件不是 YAML mapping 或包含未知键时拒绝启动。"""
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("{unclosed", encoding="utf-8")
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(), {"ONEBOT11_ROLES_FILE": str(bad_yaml)})

    not_mapping = tmp_path / "list.yaml"
    not_mapping.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(),
            {"ONEBOT11_ROLES_FILE": str(not_mapping)},
        )

    unknown_key = tmp_path / "unknown.yaml"
    unknown_key.write_text("roles: {}\nsecret: 1\n", encoding="utf-8")
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(),
            {"ONEBOT11_ROLES_FILE": str(unknown_key)},
        )


def test超级管理员返回统一解析结果并拒绝mapping():
    """adapter 与 validate_config 必须共享同一份严格管理员配置。"""
    runtime = parse_runtime_config(_extra(super_admins=["2056963663"]))
    assert runtime.super_admins == frozenset({"2056963663"})
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(super_admins={"user": "2056963663"}))


def testtrusted_user进入统一运行时配置且只能只读():
    """adapter 与 validate_config 必须得到相同 trusted_user 身份和工具合同。"""
    runtime = parse_runtime_config(
        _extra(
            roles={
                "trusted_user": {
                    "users": ["2056963663"],
                    "tools": ["qq_get_message"],
                }
            }
        )
    )
    assert runtime.trusted_users == frozenset({"2056963663"})
    assert runtime.role_tools["trusted_user"] == frozenset({"qq_get_message"})
    with pytest.raises(ValueError, match="只能包含只读工具"):
        parse_runtime_config(
            _extra(
                roles={
                    "trusted_user": {
                        "users": ["2056963663"],
                        "tools": ["qq_set_group_ban"],
                    }
                }
            )
        )


def testws_host拒绝URL格式():
    """监听地址只接受 hostname/IP，不能把 URL 延迟到启动阶段才失败。"""
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(ws_host="http://127.0.0.1", access_token="token"))


def test媒体host使用字符串列表而ID列表保持纯数字():
    """媒体 host 不是 QQ 号，必须走独立字符串解析器。"""
    runtime = parse_runtime_config(
        _extra(media_allowed_hosts=["cdn.example.com", "127.0.0.1"])
    )
    assert runtime.media_allowed_hosts == frozenset({"cdn.example.com", "127.0.0.1"})
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(allowed_groups=[1.5]))
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(media_allowed_hosts={"host": "cdn.example.com"}))


def test媒体source_roots必须是绝对路径():
    """get_image 返回的本地文件只能来自显式根目录。"""
    roots = (
        ["C:/OneBot/media", "D:/cache"]
        if os.name == "nt"
        else ["/tmp/onebot-media", "/var/cache"]
    )
    runtime = parse_runtime_config(
        _extra(media_source_roots=roots)
    )
    assert runtime.media_source_roots == tuple(
        str(Path(root).resolve(strict=False)) for root in roots
    )
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(media_source_roots=["relative/media"]))


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


def test角色工具名允许显式委派但禁止动态工具发现():
    """角色可以显式授予委派能力，但不能配置动态工具发现桥。"""
    runtime = parse_runtime_config(
        _extra(
            roles={
                "trusted_user": {"tools": ["terminal", "browser", "delegate_task"]},
                "user": {"tools": ["delegate_task"]},
            }
        )
    )
    assert runtime.role_tools["trusted_user"] == frozenset(
        {"terminal", "browser", "delegate_task"}
    )
    assert runtime.role_tools["user"] == frozenset({"delegate_task"})
    with pytest.raises(ValueError, match="禁止"):
        parse_runtime_config(
            _extra(roles={"user": {"tools": ["tool_search"]}})
        )


def test主agent只读模式可由独立roles文件配置(tmp_path):
    """只读开关和角色配置一起从独立 roles 文件读取。"""
    roles_path = tmp_path / "roles.yaml"
    roles_path.write_text(
        "main_agent_read_only: true\nroles: {}\n",
        encoding="utf-8",
    )
    runtime = parse_runtime_config(
        _extra(),
        {"ONEBOT11_ROLES_FILE": str(roles_path)},
    )
    assert runtime.main_agent_read_only is True


def test主agent只读开关严格解析():
    """不能用字符串 truthiness 把错误配置当成只读模式。"""
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(main_agent_read_only="sometimes"))



def test群聊中间正文默认隐藏且私聊默认展示():
    """未配置显示策略时，群聊隐藏中间正文而私聊保留展示。"""
    runtime = parse_runtime_config(_extra())
    assert runtime.show_interim_group is False
    assert runtime.show_interim_dm is True


def test群聊中间正文显式开启():
    """生产需要时仍可显式开启群聊中间正文。"""
    runtime = parse_runtime_config(_extra(show_interim_group=True))
    assert runtime.show_interim_group is True
def test显式空超级管理员工具与错误角色类型区分():
    """空工具集合是合法配置；列表角色和未知工具必须 fail-closed。"""
    runtime = parse_runtime_config(
        _extra(roles={"super_admin": {"tools": []}})
    )
    assert runtime.role_tools["super_admin"] == frozenset()
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(roles={"super_admin": []}))
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(roles=[]))


def test_llm_trigger结构和旁路路由必须严格():
    """LLM trigger 不能用空列表或非字符串 provider/model 绕过校验。"""
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(llm_trigger=[]))
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(
                llm_trigger={
                    "enabled": True,
                    "provider": 123,
                    "model": "model",
                    "groups": ["888"],
                }
            )
        )
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(
                llm_trigger={
                    "enabled": True,
                    "groups": ["888"],
                }
            )
        )
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(
                llm_trigger={
                    "enabled": True,
                    "provider": "custom",
                    "model": "small",
                    "base_url": "https://example.invalid/v1",
                    "api_key": "must-not-be-here",
                    "api_key_env": "ONEBOT11_TRIGGER_KEY",
                    "groups": ["888"],
                }
            )
        )


def test_custom_llm_trigger必须使用安全endpoint和环境变量():
    """自定义 provider 不能把密钥或任意协议带进运行时配置。"""
    runtime = parse_runtime_config(
        _extra(
            llm_trigger={
                "enabled": True,
                "provider": "custom",
                "model": "small",
                "base_url": "https://example.invalid/v1",
                "api_key_env": "ONEBOT11_TRIGGER_KEY",
                "groups": ["888"],
            }
        )
    )
    assert runtime.trigger_config.llm_base_url == "https://example.invalid/v1"
    assert runtime.trigger_config.llm_api_key_env == "ONEBOT11_TRIGGER_KEY"
    uppercase_runtime = parse_runtime_config(
        _extra(
            llm_trigger={
                "enabled": True,
                "provider": "CUSTOM",
                "model": "small",
                "base_url": "https://example.invalid/v1",
                "api_key_env": "ONEBOT11_TRIGGER_KEY",
                "groups": ["888"],
            }
        )
    )
    assert uppercase_runtime.trigger_config.llm_provider == "custom"
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(
                llm_trigger={
                    "enabled": True,
                    "provider": "custom",
                    "model": "small",
                    "api_key_env": "ONEBOT11_TRIGGER_KEY",
                    "groups": ["888"],
                }
            )
        )
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(
                llm_trigger={
                    "enabled": True,
                    "provider": "custom",
                    "model": "small",
                    "base_url": "file:///tmp/secret",
                    "api_key_env": "ONEBOT11_TRIGGER_KEY",
                    "groups": ["888"],
                }
            )
        )
    with pytest.raises(ValueError):
        parse_runtime_config(
            _extra(
                llm_trigger={
                    "enabled": True,
                    "provider": "custom",
                    "model": "small",
                    "base_url": "https://example.invalid/v1",
                    "api_key_env": "ONEBOT11-KEY",
                    "groups": ["888"],
                }
            )
        )


def test_home_channel必须显式声明目标类型():
    """cron home target 不能根据 QQ 号形状猜测群或私聊。"""
    with pytest.raises(ValueError):
        parse_runtime_config(_extra(home_channel="1072992996"))
    runtime = parse_runtime_config(
        _extra(home_channel="1072992996", home_channel_type="group")
    )
    assert runtime.home_channel == "1072992996"
    assert runtime.home_channel_type == "group"


def test纯文本开关属于可热更新策略():
    """纯文本显示策略由 RuntimeConfig 严格解析。"""
    runtime = parse_runtime_config(_extra(plain_text_enabled=False))
    assert runtime.plain_text_enabled is False
