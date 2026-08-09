"""插件自有 pi-ai 客户端的协议测试。"""

from __future__ import annotations

import asyncio
import json
import shutil
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


@pytest.mark.asyncio
async def test_pi_ai真实custom_endpoint使用system_prompt和环境变量key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """真实 helper 必须能调用本地 OpenAI-compatible endpoint。"""
    plugin_root = Path(__file__).resolve().parents[1]
    if (
        shutil.which("node") is None
        or not (plugin_root / "node_modules" / "@earendil-works" / "pi-ai").is_dir()
    ):
        pytest.skip("未安装 Node.js 或 pi-ai npm 依赖")

    expected = '{"decision":"ignore","wait_seconds":0}'
    captured: dict[str, object] = {}

    async def handle(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """读取一次 OpenAI-compatible 请求并返回最小 SSE 响应。"""
        request_line = await reader.readline()
        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            if line in {b"", b"\r\n"}:
                break
            name, separator, value = line.decode("latin-1").partition(":")
            if separator:
                headers[name.casefold()] = value.strip()
        content_length = int(headers.get("content-length", "0"))
        body = await reader.readexactly(content_length) if content_length else b""
        captured["path"] = request_line.decode("latin-1").split(" ", 2)[1]
        captured["authorization"] = headers.get("authorization", "")
        captured["body"] = json.loads(body.decode("utf-8"))
        events = (
            f"data: {json.dumps({'id': 'test', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test-model', 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': expected}, 'finish_reason': None}]}, ensure_ascii=False)}\n\n"
            f"data: {json.dumps({'id': 'test', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test-model', 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]})}\n\n"
            "data: [DONE]\n\n"
        ).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: close\r\n"
            + f"Content-Length: {len(events)}\r\n\r\n".encode("ascii")
            + events
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = int(server.sockets[0].getsockname()[1])
    monkeypatch.setenv("ONEBOT11_TEST_TRIGGER_KEY", "local-test-secret")
    client = PiAiTriggerClient(
        provider="custom",
        model="test-model",
        base_url=f"http://127.0.0.1:{port}/v1",
        api_key_env="ONEBOT11_TEST_TRIGGER_KEY",
    )
    try:
        result = await client.complete("本地 custom provider smoke", timeout_seconds=5)
    finally:
        server.close()
        await server.wait_closed()

    assert result == expected
    assert captured["path"] == "/v1/chat/completions"
    assert captured["authorization"] == "Bearer local-test-secret"
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["messages"][0]["role"] == "system"
    assert body["messages"][1]["content"] == "本地 custom provider smoke"
