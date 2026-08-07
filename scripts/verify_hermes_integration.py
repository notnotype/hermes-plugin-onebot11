"""跨平台运行 OneBot11 插件的 Hermes 组合验收。"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import inspect
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
    auxiliary_source: Path | None,
    hermes_home: Path,
) -> dict[str, str]:
    """构造隔离的 Hermes/Python 环境，不修改调用者进程环境。"""
    sources = [str(plugin_root)]
    if auxiliary_source is not None:
        sources.append(str(auxiliary_source))
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
    auxiliary_source: Path | None,
) -> None:
    """运行插件全套测试和可选的 Hermes strict auxiliary 测试。"""
    if auxiliary_source is None:
        command = [sys.executable, "-m", "pytest", "-q"]
    else:
        injection = """
import importlib.util
import sys
from pathlib import Path
import agent
source = Path(sys.argv[1]) / "agent" / "auxiliary_client.py"
spec = importlib.util.spec_from_file_location("agent.auxiliary_client", source)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)
sys.modules["agent.auxiliary_client"] = module
agent.auxiliary_client = module
import pytest
raise SystemExit(pytest.main(["-q"]))
"""
        command = [sys.executable, "-c", injection, str(auxiliary_source)]
    result = subprocess.run(command, cwd=plugin_root, env=env, check=False)
    if result.returncode:
        raise SystemExit(result.returncode)
    if auxiliary_source is None:
        return
    test_file = auxiliary_source / "tests" / "agent" / "test_auxiliary_no_fallback.py"
    if not test_file.is_file():
        raise FileNotFoundError(f"找不到 strict auxiliary 测试: {test_file}")
    injection = """
import importlib.util
import sys
from pathlib import Path
import agent
source = Path(sys.argv[1]) / "agent" / "auxiliary_client.py"
spec = importlib.util.spec_from_file_location("agent.auxiliary_client", source)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)
sys.modules["agent.auxiliary_client"] = module
agent.auxiliary_client = module
import pytest
raise SystemExit(pytest.main(["-q", sys.argv[2]]))
"""
    result = subprocess.run(
        [sys.executable, "-c", injection, str(auxiliary_source), str(test_file)],
        cwd=plugin_root,
        env=env,
        check=False,
    )
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
        self.auxiliary: list[dict] = []

    def register_platform(self, **kwargs: object) -> None:
        """记录平台注册参数。"""
        self.platforms.append(kwargs)

    def register_tool(self, **kwargs: object) -> None:
        """记录工具注册参数。"""
        self.tools.append(kwargs)

    def register_hook(self, name: str, callback: object) -> None:
        """记录 hook 注册参数。"""
        self.hooks.append((name, callback))

    def register_auxiliary_task(self, **kwargs: object) -> None:
        """记录 auxiliary 注册参数。"""
        self.auxiliary.append(kwargs)


def _strict_auxiliary_supported() -> bool:
    """检查当前 Hermes auxiliary API 是否有严格旁路参数。"""
    from agent.auxiliary_client import async_call_llm

    parameters = inspect.signature(async_call_llm).parameters
    return {"fallback_policy", "max_attempts"}.issubset(parameters)


def _inject_auxiliary_source(auxiliary_source: Path | None) -> None:
    """把独立 Hermes worktree 的 auxiliary_client 注入当前进程。"""
    if auxiliary_source is None:
        return
    source = auxiliary_source / "agent" / "auxiliary_client.py"
    if not source.is_file():
        raise FileNotFoundError(f"找不到 auxiliary_client.py: {source}")
    import agent

    spec = importlib.util.spec_from_file_location("agent.auxiliary_client", source)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载 auxiliary_client.py: {source}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules["agent.auxiliary_client"] = module
    agent.auxiliary_client = module


async def _smoke(
    plugin_root: Path,
    hermes_home: Path,
    auxiliary_source: Path | None,
    require_strict: bool,
) -> None:
    """执行平台、工具、hooks、shared session 和 reconnect smoke。"""
    del plugin_root
    _inject_auxiliary_source(auxiliary_source)
    _register_platform_if_needed()
    from gateway.config import PlatformConfig

    import adapter

    context = _RegistrationContext()
    adapter.register(context)
    if len(context.platforms) != 1:
        raise AssertionError(f"平台注册数量错误: {len(context.platforms)}")
    if len(context.tools) != 9:
        raise AssertionError(f"工具注册数量错误: {len(context.tools)}")
    if len(context.hooks) != 4:
        raise AssertionError(f"hook 注册数量错误: {len(context.hooks)}")
    if not any(item.get("key") == "onebot11_trigger" for item in context.auxiliary):
        raise AssertionError("onebot11_trigger auxiliary 未注册")

    strict = _strict_auxiliary_supported()
    if require_strict and not strict:
        raise AssertionError("当前 Hermes auxiliary API 不支持 fallback_policy/max_attempts")

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
    try:
        if not await instance.connect():
            raise AssertionError("adapter connect smoke 失败")
        await instance.disconnect()
        if not await instance.connect(is_reconnect=True):
            raise AssertionError("adapter reconnect smoke 失败")
        if instance._queue.closed or instance._dispatcher._closed:
            raise AssertionError("reconnect 后 queue/dispatcher 仍是 closed")
    finally:
        await instance.disconnect()

    print(
        "Hermes integration smoke passed: "
        f"tools={len(context.tools)} hooks={len(context.hooks)} "
        f"strict_auxiliary={strict} reconnect=True"
    )


def main() -> int:
    """解析参数、建立临时 Hermes home 并完成组合验收。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plugin-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--hermes-source", type=Path, required=True)
    parser.add_argument("--hermes-site-packages", type=Path)
    parser.add_argument("--hermes-auxiliary-source", type=Path)
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--require-strict", action="store_true")
    args = parser.parse_args()

    plugin_root = args.plugin_root.resolve()
    hermes_source = args.hermes_source.resolve()
    auxiliary_source = (
        args.hermes_auxiliary_source.resolve()
        if args.hermes_auxiliary_source
        else None
    )
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
            auxiliary_source=auxiliary_source,
            hermes_home=hermes_home,
        )
        if not args.skip_tests:
            _run_tests(
                plugin_root=plugin_root,
                env=env,
                auxiliary_source=auxiliary_source,
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
                    auxiliary_source,
                    args.require_strict or auxiliary_source is not None,
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
