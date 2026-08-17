"""跨平台运行 OneBot11 插件的 Hermes 组合验收。"""

from __future__ import annotations

import argparse
import asyncio
import base64
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def _site_packages(hermes_source: Path, explicit: Path | None) -> Path:
    """解析 Hermes 虚拟环境的 site-packages 路径。"""
    candidates = [explicit] if explicit is not None else []
    candidates.extend(
        [
            hermes_source / "venv" / "Lib" / "site-packages",
            hermes_source / "venv" / "lib",
        ]
    )
    for candidate in candidates:
        if candidate is None:
            continue
        if candidate.name == "lib":
            matches = sorted(candidate.glob("python*/site-packages"))
            if matches:
                return matches[0].resolve()
        if candidate.is_dir():
            return candidate.resolve()
    raise FileNotFoundError(f"找不到 Hermes site-packages: {hermes_source}")


def _environment(
    *,
    plugin_root: Path,
    hermes_source: Path,
    site_packages: Path,
    hermes_home: Path,
) -> dict[str, str]:
    """构造隔离的 Hermes/Python 环境，不修改调用者进程环境。"""
    sources = [str(plugin_root)]
    sources.extend([str(hermes_source), str(site_packages)])
    env = os.environ.copy()
    env["HERMES_HOME"] = str(hermes_home)
    old_python_path = env.get("PYTHONPATH", "")
    if old_python_path:
        sources.append(old_python_path)
    env["PYTHONPATH"] = os.pathsep.join(sources)
    return env


def _run_tests(
    *,
    plugin_root: Path,
    env: dict[str, str],
) -> None:
    """在 Hermes 组合环境中运行插件测试。"""
    command = [sys.executable, "-m", "pytest", "-q"]
    result = subprocess.run(command, cwd=plugin_root, env=env, check=False)
    if result.returncode:
        raise SystemExit(result.returncode)


def _register_platform_if_needed() -> None:
    """确保 smoke 可以在没有真实 gateway runner 的临时进程中解析平台。"""
    from gateway.platform_registry import PlatformEntry, platform_registry

    try:
        platform_registry.register(
            PlatformEntry(
                name="onebot11",
                label="OneBot 11 (QQ)",
                adapter_factory=lambda _config: None,
                check_fn=lambda: True,
                source="plugin",
            )
        )
    except Exception:
        # 已经注册时保留真实 runner/测试环境的已有 entry。
        pass


class _RegistrationContext:
    """收集插件 register() 的真实注册合同。"""

    def __init__(self) -> None:
        """初始化注册结果容器。"""
        self.platforms: list[dict] = []
        self.tools: list[dict] = []
        self.hooks: list[tuple[str, object]] = []
        self.skills: list[tuple[str, Path, str]] = []

    def register_platform(self, **kwargs: object) -> None:
        """记录平台注册参数。"""
        self.platforms.append(kwargs)

    def register_tool(self, **kwargs: object) -> None:
        """记录工具注册参数。"""
        self.tools.append(kwargs)

    def register_hook(self, name: str, callback: object) -> None:
        """记录 hook 注册参数。"""
        self.hooks.append((name, callback))

    def register_skill(self, name: str, path: Path, description: str) -> None:
        """记录插件 Skill 注册参数。"""
        self.skills.append((name, path, description))


async def _smoke(
    plugin_root: Path,
    hermes_home: Path,
) -> None:
    """执行平台、工具、hooks、shared session、命令、reconnect 和图片 smoke。"""
    del plugin_root
    _register_platform_if_needed()
    from gateway.config import PlatformConfig

    import adapter

    # 使用不存在的 provider 做离线 helper smoke：验证 Node、npm 依赖、
    # stdin/stdout 合同已经接通，但不向任何真实模型发送请求。
    try:
        await adapter.PiAiTriggerClient(
            provider="onebot11-test-provider",
            model="onebot11-test-model",
        ).complete("只做离线 helper smoke", timeout_seconds=5)
    except adapter.PiAiTriggerError as error:
        if error.kind != "provider_missing":
            raise AssertionError(f"pi-ai helper 失败分类错误: {error.kind}") from error
    else:
        raise AssertionError("不存在的 pi-ai provider 不应返回成功")

    context = _RegistrationContext()
    adapter.register(context)
    if len(context.platforms) != 1:
        raise AssertionError(f"平台注册数量错误: {len(context.platforms)}")
    if len(context.tools) != 9:
        raise AssertionError(f"工具注册数量错误: {len(context.tools)}")
    if len(context.hooks) != 5:
        raise AssertionError(f"hook 注册数量错误: {len(context.hooks)}")
    if "on_session_reset" not in {name for name, _callback in context.hooks}:
        raise AssertionError("缺少 on_session_reset hook")
    if len(context.skills) != 1:
        raise AssertionError(f"插件 Skill 注册数量错误: {len(context.skills)}")
    skill_name, skill_path, skill_description = context.skills[0]
    if skill_name != "repository-research" or not skill_path.is_file():
        raise AssertionError("repository-research Skill 未注册或路径不存在")
    skill_text = skill_path.read_text(encoding="utf-8")
    if "name: repository-research" not in skill_text or not skill_description:
        raise AssertionError("repository-research Skill frontmatter 或描述无效")
    if "manifest.evidence.mediaFiles" not in skill_text or "manifest.evidence.files" not in skill_text:
        raise AssertionError("repository-research Skill 未声明唯一媒体路径来源")
    if "vision_analyze" not in skill_text or "--annotate" not in skill_text:
        raise AssertionError("repository-research Skill 未声明视觉分析与受控标注流程")
    for required_contract in (
        "任务分流与版本门槛",
        "暂停调研和委派",
        "同批次中与当前产品问题无关的闲聊",
        "客服归档合同",
        "实际写入 HERMES_HOME/evidence",
        "不得声称已归档",
    ):
        if required_contract not in skill_text:
            raise AssertionError(f"repository-research Skill 缺少客服合同: {required_contract}")
    queue_db = hermes_home / "onebot11" / "integration.sqlite3"
    config = PlatformConfig(
        enabled=True,
        extra={
            "http_api": "http://127.0.0.1:3000",
            "self_id": "1",
            "ws_port": 0,
            "queue_db_path": str(queue_db),
        },
    )
    instance = adapter.OneBot11Adapter(config)
    if instance.config.extra.get("group_sessions_per_user") is not False:
        raise AssertionError("OneBot11 没有强制 shared group session")
    if not instance.splits_long_messages:
        raise AssertionError("OneBot11 cron 输出没有交给 adapter 分块")
    try:
        if not await instance.connect():
            raise AssertionError("adapter connect smoke 失败")
        instance._trigger_state_for("888").mode = "engaged"
        await instance.disconnect()
        if not await instance.connect(is_reconnect=True):
            raise AssertionError("adapter reconnect smoke 失败")
        if instance._queue.closed or instance._dispatcher._closed:
            raise AssertionError("reconnect 后 queue/dispatcher 仍是 closed")
        if instance._trigger_states:
            raise AssertionError("reconnect 后不应恢复内存 active/debounce/judging 状态")

        # Hermes 的 pre_llm_call 可能在 worker thread 执行，而最终出站
        # 回到 async event loop；验证插件能从 synthetic event metadata
        # 恢复精确 binding，而不是依赖 worker ContextVar。
        from onebot11.events import InboundEvent

        instance._chat_types["888"] = "group"
        binding_event = await instance._build_message_event(
            InboundEvent(
                text="worker binding smoke",
                chat_id="888",
                chat_type="group",
                user_id="123",
                user_name="smoke",
                message_id="binding-smoke",
            )
        )
        binding_caller = instance._caller_for_event(binding_event.source)
        binding_event.metadata.update(
            {
                "onebot11_managed_context": True,
                "onebot11_caller_context": adapter._serializable_caller(
                    binding_caller
                ),
            }
        )
        original_live_adapter = adapter._get_live_adapter
        original_send_message = instance._api.send_message

        async def fake_binding_send(
            _target_id: str,
            _content: str,
            *,
            chat_type: str,
            reply_to: str | None = None,
        ) -> str:
            del reply_to
            if chat_type != "group":
                raise AssertionError("worker binding smoke 目标类型错误")
            return "worker-binding-smoke-1"

        adapter._get_live_adapter = lambda: instance
        instance._api.send_message = fake_binding_send
        event_token = adapter._CURRENT_EVENT.set(binding_event)
        caller_token = adapter._CURRENT_CALLER.set(binding_caller)
        binding_token = adapter._CURRENT_BINDING.set(None)
        try:
            await asyncio.to_thread(
                adapter._pre_llm_call_hook,
                session_id="worker-session",
                turn_id="worker-turn",
                platform="onebot11",
            )
            result = await instance.send(
                "888",
                "worker binding smoke",
                metadata={"notify": True},
            )
            if not result.success:
                raise AssertionError(f"worker binding 出站 smoke 失败: {result!r}")
        finally:
            adapter._CURRENT_BINDING.reset(binding_token)
            adapter._CURRENT_CALLER.reset(caller_token)
            adapter._CURRENT_EVENT.reset(event_token)
            adapter._get_live_adapter = original_live_adapter
            instance._api.send_message = original_send_message

        # 斜杠命令在群消息入队前桥接到 Hermes 公共命令入口；这里只验证
        # synthetic event 合同，不调用 Hermes 私有 reset 实现。
        command_event = InboundEvent(
            text="/new integration",
            chat_id="888",
            chat_type="group",
            user_id="123",
            user_name="smoke",
            message_id="command-smoke",
        )
        parsed_command = adapter.parse_conversation_command(command_event.text)
        if parsed_command is None or parsed_command.name != "new":
            raise AssertionError("群级 /new 命令解析 smoke 失败")
        synthetic = instance._build_conversation_command_event(
            command_event,
            parsed_command,
            reset_marker_id="integration-reset-marker",
        )
        if synthetic.text != "/new integration":
            raise AssertionError("群级 /new 没有桥接为 Hermes 公共命令")
        if (
            synthetic.metadata.get("onebot11_reset_marker_id")
            != "integration-reset-marker"
        ):
            raise AssertionError("reset marker 未绑定到 synthetic command")
        instance._targets["888"] = adapter.ChatTarget("group", "888")
        instance._chat_types["888"] = "group"
        original_command_send = instance._api.send_message

        async def fake_command_reply(
            target_id: str,
            content: str,
            *,
            chat_type: str,
            reply_to: str | None = None,
        ) -> str:
            if (
                target_id != "888"
                or content != "reset completed"
                or chat_type != "group"
                or reply_to != "command-smoke"
            ):
                raise AssertionError("会话命令回执目标或内容错误")
            return "command-reply-1"

        instance._api.send_message = fake_command_reply
        event_token = adapter._CURRENT_EVENT.set(synthetic)
        try:
            command_reply = await instance._send_with_retry(
                "888",
                "reset completed",
                reply_to="command-smoke",
            )
        finally:
            adapter._CURRENT_EVENT.reset(event_token)
            instance._api.send_message = original_command_send
        if not command_reply.success:
            raise AssertionError(f"会话命令回执被错误拦截: {command_reply!r}")

        # 持久 trigger 不依赖入站历史；暂停群避免 smoke 启动真实 Agent，
        # 只验证断开/重连后消息和 durable request 都还在。
        from onebot11.queue import QueueMessage, TriggerRequest

        pending = QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="integration-pending",
            user_id="123",
            user_name="smoke",
            text="pending recovery",
            message_key="group:integration-pending",
        )
        instance._queue.enqueue(
            pending,
            TriggerRequest.create(
                "888",
                "group:integration-pending",
                "mention",
                "123",
                "smoke",
                authority_self_id="1",
            ),
        )
        instance._queue.set_paused("888", True)
        await instance.disconnect()
        if not await instance.connect(is_reconnect=True):
            raise AssertionError("pending trigger reconnect smoke 失败")
        pending_status = instance._queue.status("888")
        if (
            pending_status.get("pending") != 1
            or pending_status.get("pending_trigger_requests") != 1
        ):
            raise AssertionError("reconnect 后 pending message/trigger 未恢复")
        instance._queue.clear("888")

        # 图片出站只做本地 segment smoke，不访问真实 OneBot；确认宿主机
        # 路径被转换为可跨 Docker 边界传输的 base64://。
        image_path = Path(instance._media_root) / "integration-image.png"
        image_path.write_bytes(b"\x89PNG\r\n\x1a\nintegration")
        captured_segments: list[list[dict]] = []

        async def fake_send_segments(
            target_id: str,
            segments: list[dict],
            *,
            chat_type: str,
        ) -> str:
            if target_id != "888" or chat_type != "group":
                raise AssertionError("图片 smoke 目标类型错误")
            captured_segments.append(segments)
            return "image-smoke-1"

        original_send_segments = instance._api.send_message_segments
        instance._api.send_message_segments = fake_send_segments
        try:
            image_result = await instance.send_image_file(
                "888",
                str(image_path),
                caption="image smoke",
            )
        finally:
            instance._api.send_message_segments = original_send_segments
        if not image_result.success or not captured_segments:
            raise AssertionError(f"图片出站 smoke 失败: {image_result!r}")
        image_segment = next(
            segment
            for segment in captured_segments[0]
            if segment.get("type") == "image"
        )
        encoded_image = str(image_segment["data"]["file"])
        if not encoded_image.startswith("base64://") or base64.b64decode(
            encoded_image.removeprefix("base64://")
        ) != b"\x89PNG\r\n\x1a\nintegration":
            raise AssertionError("图片 smoke 没有生成正确 base64 segment")

        # standalone cron 不依赖当前 session 或入站历史；用本地 monkeypatch
        # 验证目标类型和允许群合同，不访问真实 OneBot endpoint。
        captured: list[tuple[str, str, str]] = []
        original_send_message = adapter.OneBotHttpApi.send_message

        async def fake_send_message(
            api: object,
            target_id: str,
            content: str,
            *,
            chat_type: str,
            reply_to: str | None = None,
        ) -> str:
            del api, reply_to
            captured.append((target_id, content, chat_type))
            return "cron-smoke-1"

        adapter.OneBotHttpApi.send_message = fake_send_message
        try:
            cron_config = PlatformConfig(
                enabled=True,
                extra={
                    "http_api": "http://127.0.0.1:3000",
                    "self_id": "1",
                    "home_channel": "1072992996",
                    "home_channel_type": "group",
                    "allowed_groups": ["1072992996"],
                },
            )
            cron_result = await adapter._standalone_send(
                cron_config,
                "1072992996",
                "cron smoke",
            )
        finally:
            adapter.OneBotHttpApi.send_message = original_send_message
        if cron_result.get("success") is not True or captured != [
            ("1072992996", "cron smoke", "group")
        ]:
            raise AssertionError(f"home cron smoke 失败: {cron_result!r} {captured!r}")
    finally:
        await instance.disconnect()

    print(
        "Hermes integration smoke passed: "
        f"tools={len(context.tools)} hooks={len(context.hooks)} "
        "plugin_skill=True pi_ai_trigger=True reconnect=True slash_commands=True"
    )


def main() -> int:
    """解析参数、建立临时 Hermes home 并完成组合验收。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plugin-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--hermes-source", type=Path, required=True)
    parser.add_argument("--hermes-site-packages", type=Path)
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()

    plugin_root = args.plugin_root.resolve()
    hermes_source = args.hermes_source.resolve()
    site_packages = _site_packages(
        hermes_source,
        args.hermes_site_packages.resolve() if args.hermes_site_packages else None,
    )
    with tempfile.TemporaryDirectory(prefix="hermes-onebot11-") as raw_home:
        hermes_home = Path(raw_home).resolve()
        env = _environment(
            plugin_root=plugin_root,
            hermes_source=hermes_source,
            site_packages=site_packages,
            hermes_home=hermes_home,
        )
        if not args.skip_tests:
            _run_tests(
                plugin_root=plugin_root,
                env=env,
            )
        old_python_path = os.environ.get("PYTHONPATH")
        old_hermes_home = os.environ.get("HERMES_HOME")
        try:
            os.environ.update(
                {
                    "PYTHONPATH": env["PYTHONPATH"],
                    "HERMES_HOME": env["HERMES_HOME"],
                }
            )
            for path in reversed(env["PYTHONPATH"].split(os.pathsep)):
                if path and path not in sys.path:
                    sys.path.insert(0, path)
            asyncio.run(
                _smoke(
                    plugin_root,
                    hermes_home,
                )
            )
        finally:
            if old_python_path is None:
                os.environ.pop("PYTHONPATH", None)
            else:
                os.environ["PYTHONPATH"] = old_python_path
            if old_hermes_home is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = old_hermes_home
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
