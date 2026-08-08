"""审计旁路失败时的降级合同测试。"""

from pathlib import Path

from onebot11.audit import AuditLog


def test_审计目录不可写时不阻塞业务(tmp_path: Path):
    """审计路径初始化失败时，消息处理仍可继续。"""
    blocked_parent = tmp_path / "blocked"
    blocked_parent.write_text("not a directory", encoding="utf-8")

    audit = AuditLog(blocked_parent / "audit.jsonl")
    audit.record("test", {"value": "still-running"})
