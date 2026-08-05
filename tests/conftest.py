"""测试隔离配置，避免持久化 adapter 测试写入真实 Hermes home。"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """为每个测试提供独立的 Hermes 状态根目录。"""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
