"""OneBot11 自有 pi-ai 旁路客户端。

本模块不导入 Hermes，只负责启动短生命周期 Node helper，并把模型调用
结果转换成 Python 可分类的异常。模型判断失败时由 adapter 决定按 ignore
处理；这里不实现语义重试。
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class PiAiTriggerError(RuntimeError):
    """pi-ai 旁路调用失败，并携带稳定的审计分类。"""

    def __init__(self, kind: str, message: str) -> None:
        """保存不含密钥的失败分类和短错误信息。"""
        self.kind = str(kind)
        super().__init__(str(message)[:256])


@dataclass(frozen=True)
class PiAiTriggerClient:
    """通过一次性 Node 进程调用 pi-ai。"""

    provider: str
    model: str
    base_url: str = ""
    api_key_env: str = ""
    script_path: Path | None = None
    node_binary: str = "node"

    def _script(self) -> Path:
        """解析仓库内随插件发布的 Node helper 路径。"""
        if self.script_path is not None:
            return self.script_path
        return Path(__file__).resolve().parents[1] / "scripts" / "onebot11-pi-trigger.mjs"

    async def complete(self, prompt: str, *, timeout_seconds: float) -> str:
        """调用 pi-ai 并返回模型文本；任何异常都转换成可审计失败。"""
        script = self._script()
        if not script.is_file():
            raise PiAiTriggerError("helper_missing", "pi-ai Node helper 不存在")
        executable = shutil.which(self.node_binary)
        if executable is None:
            raise PiAiTriggerError("node_missing", "找不到 Node.js 可执行文件")
        if not isinstance(prompt, str) or not prompt:
            raise PiAiTriggerError("invalid_input", "pi-ai prompt 不能为空")
        if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise PiAiTriggerError("invalid_input", "pi-ai timeout 无效")

        payload = {
            "provider": self.provider,
            "model": self.model,
            "base_url": self.base_url,
            "api_key_env": self.api_key_env,
            "prompt": prompt,
            "timeout_ms": int(float(timeout_seconds) * 1000),
        }
        env = os.environ.copy()
        try:
            process = await asyncio.create_subprocess_exec(
                executable,
                str(script),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        except FileNotFoundError as exc:
            raise PiAiTriggerError("node_missing", "找不到 Node.js 可执行文件") from exc
        except OSError as exc:
            raise PiAiTriggerError("helper_error", f"启动 pi-ai helper 失败: {exc}") from exc

        raw_output: bytes
        try:
            raw_output, _raw_error = await asyncio.wait_for(
                process.communicate(
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                ),
                timeout=float(timeout_seconds) + 1.0,
            )
        except TimeoutError as exc:
            process.kill()
            await process.communicate()
            raise PiAiTriggerError("timeout", "pi-ai helper 超时") from exc

        result = _parse_helper_result(raw_output)
        if process.returncode != 0:
            if result is not None and result.get("error_kind"):
                raise PiAiTriggerError(
                    str(result["error_kind"]),
                    str(result.get("error") or "pi-ai helper 返回失败"),
                )
            raise PiAiTriggerError(
                "helper_error",
                f"pi-ai helper 退出码 {process.returncode}",
            )
        if result is None:
            raise PiAiTriggerError("invalid_output", "pi-ai helper 没有返回 JSON")
        if result.get("ok") is not True:
            raise PiAiTriggerError(
                str(result.get("error_kind") or "model_error"),
                str(result.get("error") or "pi-ai helper 返回失败"),
            )
        text = result.get("text")
        if not isinstance(text, str) or not text.strip():
            raise PiAiTriggerError("invalid_output", "pi-ai helper 没有返回文本")
        return text.strip()


def _parse_helper_result(raw_output: bytes) -> dict[str, Any] | None:
    """解析 helper 的单行 JSON，拒绝多余输出和非对象结果。"""
    try:
        text = raw_output.decode("utf-8").strip()
        value = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None
