"""插件自有 pi-ai 客户端的协议测试。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from onebot11.pi_ai import PiAiTriggerClient, PiAiTriggerError, _parse_helper_result


class _FakeProcess:
    """模拟一次性 Node helper 进程。"""

    def __init__(self, output: bytes, *, returncode: int = 0) -> None:
        """保存固定 stdout 和退出码。"""
        self.output = output
        self.returncode = returncode
        self.input: bytes | None = None

    async def communicate(self, input_data: bytes | None = None) -> tuple[bytes, bytes]:
        """记录 stdin 并返回预设结果。"""
        self.input = input_data
        return self.output, b""


def test_helper_result只接受单个JSON对象() -> None:
    """多余文本、数组和畸形 JSON 都不能伪装成模型结果。"""
    assert _parse_helper_result(b'{"ok":true,"text":"x"}') == {"ok": True, "text": "x"}
    assert _parse_helper_result(b'prefix\n{"ok":true}') is None
    assert _parse_helper_result(b"[]") is None
    assert _parse_helper_result(b"{") is None


@pytest.mark.asyncio
async def test_pi_ai客户端不把密钥值写入stdin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Python 只把环境变量名传给 helper，绝不把密钥值放入 JSON。"""
    script = tmp_path / "helper.mjs"
    script.write_text("// test helper\n", encoding="utf-8")
    process = _FakeProcess(b'{"ok":true,"text":"{\\"decision\\":\\"ignore\\",\\"wait_seconds\\":0}"}')
    monkeypatch.setattr("onebot11.pi_ai.shutil.which", lambda _name: "node")

    async def create_process(*args: object, **kwargs: object) -> _FakeProcess:
        del args, kwargs
        return process

    monkeypatch.setattr("onebot11.pi_ai.asyncio.create_subprocess_exec", create_process)
    monkeypatch.setenv("ONEBOT11_TEST_SECRET", "secret-value")
    client = PiAiTriggerClient(
        provider="opencode-go",
        model="deepseek-v4-flash",
        api_key_env="ONEBOT11_TEST_SECRET",
        script_path=script,
    )

    result = await client.complete("判断这个问题", timeout_seconds=1)

    assert result.startswith('{"decision"')
    assert process.input is not None
    payload = json.loads(process.input.decode("utf-8"))
    assert payload["api_key_env"] == "ONEBOT11_TEST_SECRET"
    assert "secret-value" not in process.input.decode("utf-8")


@pytest.mark.asyncio
async def test_pi_ai客户端分类Node缺失和helper失败(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Node 不存在和 helper 非零退出都按稳定分类暴露给 adapter。"""
    script = tmp_path / "helper.mjs"
    script.write_text("// test helper\n", encoding="utf-8")
    client = PiAiTriggerClient(
        provider="custom",
        model="small",
        base_url="https://example.invalid/v1",
        api_key_env="ONEBOT11_TEST_SECRET",
        script_path=script,
    )
    monkeypatch.setattr("onebot11.pi_ai.shutil.which", lambda _name: None)
    with pytest.raises(PiAiTriggerError, match="Node.js"):
        await client.complete("判断", timeout_seconds=1)

    monkeypatch.setattr("onebot11.pi_ai.shutil.which", lambda _name: "node")
    process = _FakeProcess(
        '{"ok":false,"error_kind":"provider_missing","error":"缺少配置"}'.encode(),
        returncode=1,
    )

    async def create_process(*args: object, **kwargs: object) -> _FakeProcess:
        del args, kwargs
        return process

    monkeypatch.setattr("onebot11.pi_ai.asyncio.create_subprocess_exec", create_process)
    with pytest.raises(PiAiTriggerError) as error:
        await client.complete("判断", timeout_seconds=1)
    assert error.value.kind == "provider_missing"


@pytest.mark.asyncio
async def test_pi_ai客户端超时会结束helper(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """helper 卡住时客户端必须杀掉子进程并返回 timeout。"""
    script = tmp_path / "helper.mjs"
    script.write_text("// test helper\n", encoding="utf-8")
    monkeypatch.setattr("onebot11.pi_ai.shutil.which", lambda _name: "node")

    class HangingProcess(_FakeProcess):
        """模拟不会结束的 Node 进程。"""

        def __init__(self) -> None:
            """初始化杀进程观测字段。"""
            super().__init__(b"")
            self.killed = False

        async def communicate(self, input_data: bytes | None = None) -> tuple[bytes, bytes]:
            """第一次调用挂起，kill 后返回。"""
            self.input = input_data
            if self.killed:
                return b"", b""
            await asyncio.sleep(60)
            return b"", b""

        def kill(self) -> None:
            """记录客户端的超时清理动作。"""
            self.killed = True

    process = HangingProcess()

    async def create_process(*args: object, **kwargs: object) -> HangingProcess:
        del args, kwargs
        return process

    monkeypatch.setattr("onebot11.pi_ai.asyncio.create_subprocess_exec", create_process)
    client = PiAiTriggerClient(
        provider="opencode-go",
        model="deepseek-v4-flash",
        script_path=script,
    )
    with pytest.raises(PiAiTriggerError) as error:
        await client.complete("判断", timeout_seconds=0.01)
    assert error.value.kind == "timeout"
    assert process.killed
