"""OneBot 11 的 SQLite 持久消息队列。

队列状态只依赖 SQLite，不导入 Hermes。输入语义是至少一次：消息先持久化，
turn 用 lease 认领；明确失败释放，出站结果未知则进入 ``uncertain``，不自动重放。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 8
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = (2.0, 4.0, 8.0)
MAX_BACKOFF_SECONDS = 60.0


class QueueError(RuntimeError):
    """队列操作错误。"""


class QueueFull(QueueError):
    """队列超过边界，调用方应保留事件未确认状态。"""


class QueueBusy(QueueError):
    """活动 lease 或 uncertain 状态阻止了管理操作。"""


@dataclass(frozen=True)
class QueueMessage:
    """待处理的规范化 OneBot 消息。"""

    chat_id: str
    chat_type: str
    message_id: str
    user_id: str
    user_name: str
    text: str
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    message_key: str | None = None
    seq: int | None = None
    byte_size: int | None = None
    raw_text: str = ""


@dataclass(frozen=True)
class TriggerRequest:
    """持久化的触发请求。"""

    request_id: str
    chat_id: str
    message_key: str
    reason: str
    caller_user_id: str
    caller_user_name: str
    created_at: float

    @classmethod
    def create(
        cls,
        chat_id: str,
        message_key: str,
        reason: str,
        caller_user_id: str,
        caller_user_name: str,
    ) -> TriggerRequest:
        """创建一个新的持久触发请求。"""
        return cls(
            request_id=uuid.uuid4().hex,
            chat_id=str(chat_id),
            message_key=str(message_key),
            reason=str(reason),
            caller_user_id=str(caller_user_id),
            caller_user_name=str(caller_user_name),
            created_at=time.time(),
        )


@dataclass(frozen=True)
class EnqueueResult:
    """入队结果，重复事件不会重复消费。"""

    inserted: bool
    duplicate: bool
    message_key: str
    seq: int | None
    trigger_request_id: str | None


@dataclass(frozen=True)
class QueueLease:
    """一批被某个 Hermes turn 认领的消息。"""

    chat_id: str
    lease_id: str
    messages: tuple[QueueMessage, ...]
    trigger: TriggerRequest
    summary: str
    claimed_at: float
    lease_until: float
    phase: str = "agent_running"
    outbound_started: bool = False
    attempts: int = 0
    failure_count: int = 0
    revision: int = 0


@dataclass(frozen=True)
class OperationRecord:
    """非幂等管理动作的持久化状态。"""

    operation_id: str
    fingerprint: str
    tool_name: str
    chat_type: str
    chat_id: str
    caller_user_id: str
    params: Mapping[str, Any]
    status: str
    reason: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class OperationStart:
    """管理动作开始结果；未知动作不会被重复发出。"""

    started: bool
    blocked: bool
    operation: OperationRecord


class QueueStore:
    """基于 SQLite WAL 的持久队列存储。"""

    def __init__(
        self,
        db_path: str | Path,
        *,
        max_messages: int = 1000,
        max_queue_bytes: int = 2_000_000,
        max_message_bytes: int = 32_000,
        max_original_bytes: int = 8_000,
        max_summary_bytes: int = 16_000,
        recent_originals: int = 3,
        dedupe_ttl_seconds: float = 7 * 24 * 3600,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_seconds: tuple[float, ...] = DEFAULT_BACKOFF_SECONDS,
    ) -> None:
        """打开数据库并执行受版本保护的 schema 初始化。"""
        self.path = str(db_path)
        self.max_messages = max(1, int(max_messages))
        self.max_queue_bytes = max(1, int(max_queue_bytes))
        self.max_message_bytes = max(256, int(max_message_bytes))
        self.max_original_bytes = max(0, int(max_original_bytes))
        self.max_summary_bytes = max(256, int(max_summary_bytes))
        self.recent_originals = max(0, int(recent_originals))
        self.dedupe_ttl_seconds = max(60.0, float(dedupe_ttl_seconds))
        self.max_attempts = max(1, int(max_attempts))
        parsed_backoff = tuple(max(0.0, float(item)) for item in backoff_seconds)
        self.backoff_seconds = parsed_backoff or DEFAULT_BACKOFF_SECONDS
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._owner_id = uuid.uuid4().hex
        self._closed = False
        self._conn = self._open_connection()
        self._migrate()

    def _open_connection(self) -> sqlite3.Connection:
        """打开一个新的 SQLite 连接并启用可靠队列所需的 pragma。"""
        connection = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _migrate(self) -> None:
        """创建当前 schema 或从已知旧版本迁移。"""
        with self._lock:
            version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise QueueError(
                    f"OneBot11 queue schema {version} 高于支持版本 {SCHEMA_VERSION}"
                )
            self._create_tables()
            self._migrate_columns(version)
            if version < SCHEMA_VERSION:
                self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self._conn.commit()

    def _migrate_columns(self, version: int) -> None:
        """为已存在的队列文件补充增量列；未知结构直接拒绝启动。"""
        message_columns = {
            str(row[1]) for row in self._conn.execute(
                "PRAGMA table_info(onebot_queue_message)"
            ).fetchall()
        }
        trigger_columns = {
            str(row[1]) for row in self._conn.execute(
                "PRAGMA table_info(onebot_queue_trigger)"
            ).fetchall()
        }
        chat_columns = {
            str(row[1])
            for row in self._conn.execute("PRAGMA table_info(onebot_queue_chat)").fetchall()
        }
        required_message = {
            "chat_id", "message_key", "chat_type", "user_id", "user_name", "text",
            "raw_text", "metadata_json", "seq", "byte_size", "state", "lease_id",
            "lease_until", "attempts", "created_at", "updated_at",
        }
        required_trigger = {
            "request_id", "chat_id", "message_key", "reason", "caller_user_id",
            "caller_user_name", "status", "lease_id", "created_at", "updated_at",
        }
        required_chat = {"chat_id", "next_seq", "summary", "paused", "updated_at"}
        if (
            not required_message.issubset(message_columns)
            or not required_trigger.issubset(trigger_columns)
            or not required_chat.issubset(chat_columns)
        ):
            raise QueueError("OneBot11 queue schema 缺少必需列，无法安全迁移")
        additions = (
            ("onebot_queue_chat", chat_columns, "revision", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_message", message_columns, "message_id", "TEXT NOT NULL DEFAULT ''"),
            ("onebot_queue_message", message_columns, "lease_owner", "TEXT"),
            ("onebot_queue_message", message_columns, "uncertain_reason", "TEXT"),
            ("onebot_queue_message", message_columns, "lease_phase", "TEXT NOT NULL DEFAULT 'pending'"),
            ("onebot_queue_message", message_columns, "outbound_started", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_message", message_columns, "failure_count", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_message", message_columns, "next_attempt_at", "REAL"),
            ("onebot_queue_message", message_columns, "failure_reason", "TEXT"),
            ("onebot_queue_trigger", trigger_columns, "lease_owner", "TEXT"),
            ("onebot_queue_trigger", trigger_columns, "uncertain_reason", "TEXT"),
        )
        structure_changed = False
        for table, columns, name, definition in additions:
            if name not in columns:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
                structure_changed = True
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS onebot_queue_dedupe (
                chat_id TEXT NOT NULL,
                message_key TEXT NOT NULL,
                seq INTEGER,
                created_at REAL NOT NULL,
                PRIMARY KEY(chat_id, message_key)
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_onebot_queue_dedupe_created "
                "ON onebot_queue_dedupe(created_at)"
        )
        message_sql = str(
            self._conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='onebot_queue_message'"
            ).fetchone()[0]
        ).casefold()
        trigger_sql = str(
            self._conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='onebot_queue_trigger'"
            ).fetchone()[0]
        ).casefold()
        rebuilt_legacy = False
        if "failed" not in message_sql:
            self._rebuild_message_table()
            rebuilt_legacy = True
        if "failed" not in trigger_sql:
            self._rebuild_trigger_table()
            rebuilt_legacy = True
        if version < 6 or structure_changed or rebuilt_legacy:
            self._mark_legacy_leases_uncertain()
        self._conn.execute(
            """
            UPDATE onebot_queue_message
            SET lease_phase='pending'
            WHERE state='pending' AND lease_id IS NULL AND outbound_started=0
            """
        )
        self._create_indexes()
        self._dedupe_pending_triggers()
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_onebot_queue_trigger_pending_chat "
            "ON onebot_queue_trigger(chat_id) WHERE status='pending'"
        )
        self._recover_started_operations()
        self._conn.commit()

    def _recover_started_operations(self) -> None:
        """进程启动后把没有结算的管理动作标记为未知。"""
        self._conn.execute(
            """
            UPDATE onebot_operation
            SET status='unknown',
                reason=COALESCE(reason, '进程恢复时管理动作结果未知'),
                updated_at=?
            WHERE status='started'
            """,
            (self._now(),),
        )

    def _dedupe_pending_triggers(self) -> None:
        """迁移旧文件时每群只保留最早的 pending trigger。"""
        rows = self._conn.execute(
            """
            SELECT rowid, chat_id
            FROM onebot_queue_trigger
            WHERE status='pending'
            ORDER BY chat_id, created_at, rowid
            """
        ).fetchall()
        kept: set[str] = set()
        remove: list[int] = []
        for row in rows:
            chat_id = str(row[1])
            if chat_id in kept:
                remove.append(int(row[0]))
            else:
                kept.add(chat_id)
        if remove:
            self._conn.executemany(
                "DELETE FROM onebot_queue_trigger WHERE rowid=?",
                [(row_id,) for row_id in remove],
            )

    def _rebuild_message_table(self) -> None:
        """重建旧消息表，使 state CHECK 能安全加入 failed。"""
        legacy = "onebot_queue_message_legacy"
        if self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (legacy,)
        ).fetchone():
            raise QueueError("OneBot11 queue 存在未完成的消息表迁移")
        self._conn.execute("DROP INDEX IF EXISTS idx_onebot_queue_message_state")
        self._conn.execute("DROP INDEX IF EXISTS idx_onebot_queue_message_lease")
        self._conn.execute("ALTER TABLE onebot_queue_message RENAME TO " + legacy)
        self._conn.execute(
            """
            CREATE TABLE onebot_queue_message (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                message_key TEXT NOT NULL,
                chat_type TEXT NOT NULL,
                user_id TEXT NOT NULL,
                user_name TEXT NOT NULL,
                text TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                message_id TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL,
                seq INTEGER NOT NULL,
                byte_size INTEGER NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','leased','uncertain','failed')),
                lease_id TEXT,
                lease_until REAL,
                lease_owner TEXT,
                lease_phase TEXT NOT NULL DEFAULT 'pending',
                outbound_started INTEGER NOT NULL DEFAULT 0,
                uncertain_reason TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                failure_reason TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(chat_id, message_key)
            )
            """
        )
        self._conn.execute(
            """
            INSERT INTO onebot_queue_message(
                row_id,chat_id,message_key,chat_type,user_id,user_name,text,raw_text,message_id,
                metadata_json,seq,byte_size,state,lease_id,lease_until,lease_owner,
                uncertain_reason,attempts,created_at,updated_at
            )
            SELECT row_id,chat_id,message_key,chat_type,user_id,user_name,text,raw_text,
                COALESCE(message_id,''),metadata_json,seq,byte_size,state,lease_id,lease_until,
                lease_owner,uncertain_reason,attempts,created_at,updated_at
            FROM onebot_queue_message_legacy
            """
        )
        self._conn.execute("DROP TABLE " + legacy)

    def _rebuild_trigger_table(self) -> None:
        """重建旧触发表，使失败状态可持久化。"""
        legacy = "onebot_queue_trigger_legacy"
        if self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (legacy,)
        ).fetchone():
            raise QueueError("OneBot11 queue 存在未完成的触发表迁移")
        self._conn.execute("DROP INDEX IF EXISTS idx_onebot_queue_trigger_status")
        self._conn.execute("ALTER TABLE onebot_queue_trigger RENAME TO " + legacy)
        self._conn.execute(
            """
            CREATE TABLE onebot_queue_trigger (
                request_id TEXT PRIMARY KEY,
                chat_id TEXT NOT NULL,
                message_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                caller_user_id TEXT NOT NULL,
                caller_user_name TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending','claimed','uncertain','failed')),
                lease_id TEXT,
                lease_owner TEXT,
                uncertain_reason TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(chat_id, message_key)
            )
            """
        )
        self._conn.execute(
            """
            INSERT INTO onebot_queue_trigger(
                request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                status,lease_id,lease_owner,uncertain_reason,created_at,updated_at
            )
            SELECT request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                status,lease_id,lease_owner,uncertain_reason,created_at,updated_at
            FROM onebot_queue_trigger_legacy
            """
        )
        self._conn.execute("DROP TABLE " + legacy)

    def _mark_legacy_leases_uncertain(self) -> None:
        """旧版本无法判断请求阶段时，禁止自动重放活动 lease。"""
        reason = "旧 schema 无法判断出站阶段，需管理员确认"
        old_leases = self._conn.execute(
            "SELECT DISTINCT lease_id FROM onebot_queue_message WHERE state='leased' AND lease_id IS NOT NULL"
        ).fetchall()
        for row in old_leases:
            lease_id = str(row[0])
            self._conn.execute(
                """
                UPDATE onebot_queue_trigger
                SET status='uncertain', lease_id=NULL, lease_owner=NULL,
                    uncertain_reason=?, updated_at=?
                WHERE lease_id=? AND status='claimed'
                """,
                (reason, self._now(), lease_id),
            )
            self._conn.execute(
                """
                UPDATE onebot_queue_message
                SET state='uncertain', lease_id=NULL, lease_until=NULL, lease_owner=NULL,
                    lease_phase='uncertain', outbound_started=1, uncertain_reason=?, updated_at=?
                WHERE lease_id=? AND state='leased'
                """,
                (reason, self._now(), lease_id),
            )

    def _create_indexes(self) -> None:
        """创建迁移后仍需存在的索引。"""
        statements = (
            "CREATE INDEX IF NOT EXISTS idx_onebot_queue_message_state ON onebot_queue_message(chat_id, state, seq)",
            "CREATE INDEX IF NOT EXISTS idx_onebot_queue_message_lease ON onebot_queue_message(chat_id, lease_id)",
            "CREATE INDEX IF NOT EXISTS idx_onebot_queue_trigger_status ON onebot_queue_trigger(chat_id, status, created_at)",
        )
        for statement in statements:
            self._conn.execute(statement)

    def _create_tables(self) -> None:
        """创建幂等表和索引，避免 executescript 隐式提交迁移事务。"""
        statements = (
            """
            CREATE TABLE IF NOT EXISTS onebot_queue_chat (
                chat_id TEXT PRIMARY KEY,
                next_seq INTEGER NOT NULL DEFAULT 1,
                summary TEXT NOT NULL DEFAULT '',
                paused INTEGER NOT NULL DEFAULT 0,
                revision INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS onebot_queue_message (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                message_key TEXT NOT NULL,
                chat_type TEXT NOT NULL,
                user_id TEXT NOT NULL,
                user_name TEXT NOT NULL,
                text TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                message_id TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL,
                seq INTEGER NOT NULL,
                byte_size INTEGER NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','leased','uncertain','failed')),
                lease_id TEXT,
                lease_until REAL,
                lease_owner TEXT,
                lease_phase TEXT NOT NULL DEFAULT 'pending',
                outbound_started INTEGER NOT NULL DEFAULT 0,
                uncertain_reason TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                failure_reason TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(chat_id, message_key)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS onebot_queue_trigger (
                request_id TEXT PRIMARY KEY,
                chat_id TEXT NOT NULL,
                message_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                caller_user_id TEXT NOT NULL,
                caller_user_name TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending','claimed','uncertain','failed')),
                lease_id TEXT,
                lease_owner TEXT,
                uncertain_reason TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(chat_id, message_key)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS onebot_operation (
                operation_id TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                chat_type TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                caller_user_id TEXT NOT NULL,
                params_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK(
                    status IN (
                        'started','succeeded','known_failed','unknown',
                        'retry_armed','discarded'
                    )
                ),
                reason TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """,
        )
        for statement in statements:
            self._conn.execute(statement)
        self._create_indexes()
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_onebot_operation_fingerprint "
            "ON onebot_operation(fingerprint, updated_at)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_onebot_operation_chat "
            "ON onebot_operation(chat_type, chat_id, updated_at)"
        )
        self._conn.commit()

    def _now(self) -> float:
        """返回可替换的当前时间，测试可通过 monkeypatch 控制。"""
        return time.time()

    def _transaction(self) -> None:
        """开始写事务；调用方必须在锁内负责 commit/rollback。"""
        self._conn.execute("BEGIN IMMEDIATE")

    def _message_key(self, message: QueueMessage) -> str:
        """生成稳定去重键，优先使用 OneBot message_id。"""
        if message.message_key:
            return str(message.message_key)
        if message.message_id:
            return f"{message.chat_type}:{message.message_id}"
        payload = json.dumps(
            {
                "chat_id": message.chat_id,
                "chat_type": message.chat_type,
                "user_id": message.user_id,
                "user_name": message.user_name,
                "text": message.text,
                "metadata": dict(message.metadata),
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        return "hash:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _truncate_utf8(self, value: str, limit: int) -> str:
        """按 UTF-8 字节截断，不产生半个 Unicode 字符。"""
        if limit <= 0:
            return ""
        raw = str(value).encode("utf-8", errors="replace")
        return raw[:limit].decode("utf-8", errors="ignore")

    def _truncate_utf8_tail(self, value: str, limit: int) -> str:
        """按 UTF-8 字节保留字符串末尾，摘要裁剪时优先保留最新内容。"""
        if limit <= 0:
            return ""
        raw = str(value).encode("utf-8", errors="replace")
        return raw[-limit:].decode("utf-8", errors="ignore")

    def _normalize(self, message: QueueMessage) -> tuple[str, str, str, str, int]:
        """限制正文、原文和元数据，并返回规范化存储字段。"""
        text = self._truncate_utf8(message.text, self.max_message_bytes)
        original = message.raw_text or message.text
        raw_text = self._truncate_utf8(original, self.max_original_bytes)
        message_id = self._truncate_utf8(message.message_id, 256)
        user_name = self._truncate_utf8(message.user_name, 512)
        metadata: dict[str, Any] = {}
        for key, value in dict(message.metadata).items():
            if key in {"raw", "onebot11_raw", "onebot11_caller_context"}:
                continue
            metadata[str(key)] = self._normalize_metadata_value(value)
        if len(text.encode("utf-8")) < len(str(message.text).encode("utf-8")):
            metadata["truncated"] = True
            metadata["original_bytes"] = len(str(message.text).encode("utf-8"))
        metadata_json = self._bounded_metadata_json(metadata, max(256, self.max_message_bytes // 2))
        parts = [message.chat_id, message.chat_type, message_id, text, raw_text, user_name, metadata_json]
        byte_size = sum(len(part.encode("utf-8")) for part in parts)
        return text, raw_text, message_id, metadata_json, byte_size

    def _bounded_metadata_json(self, metadata: Mapping[str, Any], limit: int) -> str:
        """在不破坏 JSON 的前提下限制元数据大小。"""
        encoded = json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) <= limit:
            return encoded
        bounded: dict[str, Any] = {"truncated": True}
        for key in sorted(metadata):
            candidate = dict(bounded)
            candidate[str(key)] = metadata[key]
            serialized = json.dumps(candidate, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if len(serialized.encode("utf-8")) > limit:
                continue
            bounded = candidate
        return json.dumps(bounded, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _normalize_metadata_value(self, value: Any, depth: int = 0) -> Any:
        """保留有限的媒体标记和 reply 元数据，不把原始 payload 带入队列。"""
        if depth >= 3:
            return str(value)[:256]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return self._truncate_utf8(str(value), 512) if isinstance(value, str) else value
        if isinstance(value, Mapping):
            return {
                str(key)[:128]: self._normalize_metadata_value(item, depth + 1)
                for key, item in list(value.items())[:32]
            }
        if isinstance(value, (list, tuple, set, frozenset)):
            return [self._normalize_metadata_value(item, depth + 1) for item in list(value)[:16]]
        return str(value)[:512]

    def enqueue(
        self,
        message: QueueMessage,
        trigger_request: TriggerRequest | None = None,
    ) -> EnqueueResult:
        """在同一事务中去重、分配序号并可选持久化触发请求。"""
        if message.chat_type not in {"group", "dm"}:
            raise QueueError(f"未知 chat_type: {message.chat_type!r}")
        key = self._message_key(message)
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                self._purge_dedupe(now)
                self._conn.execute(
                    "INSERT OR IGNORE INTO onebot_queue_chat(chat_id, updated_at) VALUES (?, ?)",
                    (message.chat_id, now),
                )
                existing = self._conn.execute(
                    "SELECT seq FROM onebot_queue_message WHERE chat_id=? AND message_key=?",
                    (message.chat_id, key),
                ).fetchone()
                if existing is not None:
                    request_id = self._ensure_trigger(trigger_request, message.chat_id, key, now)
                    self._conn.commit()
                    return EnqueueResult(False, True, key, int(existing[0]), request_id)
                dedupe = self._conn.execute(
                    "SELECT seq FROM onebot_queue_dedupe WHERE chat_id=? AND message_key=?",
                    (message.chat_id, key),
                ).fetchone()
                if dedupe is not None:
                    self._conn.commit()
                    return EnqueueResult(False, True, key, int(dedupe[0]) if dedupe[0] is not None else None, None)
                text, raw_text, message_id, metadata_json, byte_size = self._normalize(message)
                totals = self._conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(byte_size), 0) FROM onebot_queue_message"
                ).fetchone()
                if int(totals[0]) >= self.max_messages or int(totals[1]) + byte_size > self.max_queue_bytes:
                    raise QueueFull("OneBot11 消息队列已满")
                next_seq = int(
                    self._conn.execute(
                        "SELECT next_seq FROM onebot_queue_chat WHERE chat_id=?", (message.chat_id,)
                    ).fetchone()[0]
                )
                self._conn.execute(
                    "UPDATE onebot_queue_chat SET next_seq=?, revision=revision+1, updated_at=? WHERE chat_id=?",
                    (next_seq + 1, now, message.chat_id),
                )
                self._conn.execute(
                    """
                    INSERT INTO onebot_queue_message(
                        chat_id,message_key,chat_type,user_id,user_name,text,raw_text,message_id,
                        metadata_json,seq,byte_size,state,lease_phase,created_at,updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?, 'pending', 'pending', ?, ?)
                    """,
                    (
                        message.chat_id,
                        key,
                        message.chat_type,
                        message.user_id,
                        self._truncate_utf8(message.user_name, 512),
                        text,
                        raw_text,
                        message_id,
                        metadata_json,
                        next_seq,
                        byte_size,
                        message.created_at,
                        now,
                    ),
                )
                request_id = self._ensure_trigger(trigger_request, message.chat_id, key, now)
                self._conn.commit()
                return EnqueueResult(True, False, key, next_seq, request_id)
            except Exception:
                self._conn.rollback()
                raise

    def _ensure_trigger(
        self,
        trigger: TriggerRequest | None,
        chat_id: str,
        message_key: str,
        now: float,
    ) -> str | None:
        """插入触发请求；同一消息只保留最早的一次请求。"""
        if trigger is None:
            return None
        existing_pending = self._conn.execute(
            """
            SELECT request_id FROM onebot_queue_trigger
            WHERE chat_id=? AND status='pending'
            ORDER BY created_at, rowid LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
        if existing_pending is not None:
            return str(existing_pending[0])
        self._conn.execute(
            """
            INSERT OR IGNORE INTO onebot_queue_trigger(
                request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                status,created_at,updated_at
            ) VALUES (?,?,?,?,?,?, 'pending', ?, ?)
            """,
            (
                trigger.request_id,
                chat_id,
                message_key,
                trigger.reason,
                trigger.caller_user_id,
                trigger.caller_user_name,
                trigger.created_at,
                now,
            ),
        )
        row = self._conn.execute(
            "SELECT request_id FROM onebot_queue_trigger WHERE chat_id=? AND message_key=?",
            (chat_id, message_key),
        ).fetchone()
        return str(row[0]) if row else None

    def _transition_trigger_rows(
        self,
        rows: list[sqlite3.Row],
        status: str,
        now: float,
        *,
        reason: str | None = None,
    ) -> None:
        """在同一事务中结算 trigger，并合并同群重复 pending 请求。"""
        if status not in {"pending", "uncertain", "failed"}:
            raise ValueError(f"非法 trigger 状态: {status}")
        for row in rows:
            request_id = str(row["request_id"])
            chat_id = str(row["chat_id"])
            if status == "pending":
                existing = self._conn.execute(
                    """
                    SELECT request_id FROM onebot_queue_trigger
                    WHERE chat_id=? AND status='pending' AND request_id<>?
                    ORDER BY created_at, rowid LIMIT 1
                    """,
                    (chat_id, request_id),
                ).fetchone()
                if existing is not None:
                    self._conn.execute(
                        """
                        DELETE FROM onebot_queue_trigger
                        WHERE request_id=? AND status IN ('claimed','uncertain','failed')
                        """,
                        (request_id,),
                    )
                    continue
            self._conn.execute(
                """
                UPDATE onebot_queue_trigger
                SET status=?, lease_id=NULL, lease_owner=NULL,
                    uncertain_reason=?, updated_at=?
                WHERE request_id=?
                """,
                (
                    status,
                    str(reason or "")[:512] or None
                    if status in {"uncertain", "failed"}
                    else None,
                    now,
                    request_id,
                ),
            )

    def _ensure_retriable_trigger(self, chat_id: str, now: float) -> str | None:
        """为曾经被认领但没有 durable trigger 的 pending 消息补触发请求。"""
        existing = self._conn.execute(
            """
            SELECT request_id FROM onebot_queue_trigger
            WHERE chat_id=? AND status='pending'
            ORDER BY created_at, rowid LIMIT 1
            """,
            (str(chat_id),),
        ).fetchone()
        if existing is not None:
            return str(existing[0])
        row = self._conn.execute(
            """
            SELECT * FROM onebot_queue_message
            WHERE chat_id=? AND state='pending' AND attempts>0
            ORDER BY seq LIMIT 1
            """,
            (str(chat_id),),
        ).fetchone()
        if row is None:
            return None
        trigger = TriggerRequest.create(
            str(chat_id),
            str(row["message_key"]),
            "queue_recovery",
            str(row["user_id"]),
            str(row["user_name"]),
        )
        return self._ensure_trigger(trigger, str(chat_id), str(row["message_key"]), now)

    def _purge_dedupe(self, now: float) -> None:
        """清理过期的持久去重记录，避免 tombstone 无限增长。"""
        cutoff = now - self.dedupe_ttl_seconds
        self._conn.execute("DELETE FROM onebot_queue_dedupe WHERE created_at<?", (cutoff,))

    def peek(self, chat_id: str) -> tuple[QueueMessage, ...]:
        """只读查看当前群 pending 消息，不创建 lease。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM onebot_queue_message
                WHERE chat_id=? AND state='pending'
                  AND (next_attempt_at IS NULL OR next_attempt_at<=?)
                ORDER BY seq
                """,
                (str(chat_id), self._now()),
            ).fetchall()
            return tuple(self._row_to_message(row) for row in rows)

    def chat_type(self, chat_id: str) -> str | None:
        """读取队列中该目标的类型；未知目标返回 None。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT chat_type FROM onebot_queue_message WHERE chat_id=? LIMIT 1",
                (str(chat_id),),
            ).fetchone()
            return str(row[0]) if row else None

    def create_trigger(
        self,
        chat_id: str,
        reason: str,
        caller_user_id: str,
        caller_user_name: str,
        message_key: str | None = None,
    ) -> str | None:
        """为指定或最早待处理消息创建 durable trigger，不创建 lease。"""
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                row = None
                if message_key:
                    row = self._conn.execute(
                        """
                        SELECT * FROM onebot_queue_message
                        WHERE chat_id=? AND message_key=? AND state='pending'
                        """,
                        (str(chat_id), str(message_key)),
                    ).fetchone()
                if row is None:
                    row = self._conn.execute(
                        """
                        SELECT * FROM onebot_queue_message
                        WHERE chat_id=? AND state='pending'
                        ORDER BY seq LIMIT 1
                        """,
                        (str(chat_id),),
                    ).fetchone()
                if row is None:
                    self._conn.commit()
                    return None
                self._conn.execute(
                    "UPDATE onebot_queue_message SET next_attempt_at=NULL, updated_at=? WHERE row_id=?",
                    (now, int(row["row_id"])),
                )
                trigger = TriggerRequest.create(
                    str(chat_id),
                    str(row["message_key"]),
                    str(reason),
                    str(caller_user_id),
                    str(caller_user_name),
                )
                request_id = self._ensure_trigger(
                    trigger, str(chat_id), str(row["message_key"]), now
                )
                self._conn.commit()
                return request_id
            except Exception:
                self._conn.rollback()
                raise

    def pending_chat_ids(self) -> tuple[str, ...]:
        """读取有待处理消息的群号，供启动恢复和旁路 trigger 使用。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT DISTINCT chat_id
                FROM onebot_queue_message
                WHERE state='pending'
                  AND (next_attempt_at IS NULL OR next_attempt_at<=?)
                ORDER BY chat_id
                """,
                (self._now(),),
            ).fetchall()
            return tuple(str(row[0]) for row in rows)

    def claim(self, chat_id: str, lease_seconds: float = 60.0) -> QueueLease | None:
        """认领一个有触发请求的 chat，并只恢复该 chat 的过期 lease。"""
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                chat_id = str(chat_id)
                chat = self._conn.execute(
                    "SELECT paused FROM onebot_queue_chat WHERE chat_id=?", (chat_id,)
                ).fetchone()
                if chat is not None and bool(chat[0]):
                    self._conn.commit()
                    return None
                uncertain_reason = "lease 过期时出站阶段未知，需管理员确认"
                pending_expired_trigger_rows = self._conn.execute(
                    """
                    SELECT DISTINCT trigger.*
                    FROM onebot_queue_trigger AS trigger
                    JOIN onebot_queue_message AS message
                      ON message.lease_id=trigger.lease_id
                     AND message.chat_id=trigger.chat_id
                    WHERE trigger.chat_id=? AND trigger.status='claimed'
                      AND message.state='leased'
                      AND message.lease_until IS NOT NULL
                      AND message.lease_until<=?
                      AND message.outbound_started=0
                      AND message.lease_phase='agent_running'
                    """,
                    (chat_id, now),
                ).fetchall()
                self._conn.execute(
                    """
                    UPDATE onebot_queue_trigger
                    SET status='uncertain', lease_id=NULL, lease_owner=NULL,
                        uncertain_reason=?, updated_at=?
                    WHERE chat_id=? AND status='claimed'
                      AND EXISTS (
                          SELECT 1 FROM onebot_queue_message AS message
                          WHERE message.chat_id=onebot_queue_trigger.chat_id
                            AND message.lease_id=onebot_queue_trigger.lease_id
                            AND message.state='leased'
                            AND (message.lease_until IS NULL OR message.lease_until<=?)
                            AND (message.outbound_started=1 OR message.lease_phase='outbound_started')
                      )
                    """,
                    (uncertain_reason, now, chat_id, now),
                )
                self._conn.execute(
                    """
                    UPDATE onebot_queue_message
                    SET state=CASE
                            WHEN lease_until IS NULL OR outbound_started=1 OR lease_phase='outbound_started'
                                THEN 'uncertain' ELSE 'pending' END,
                        lease_id=NULL, lease_until=NULL, lease_owner=NULL,
                        lease_phase=CASE
                            WHEN outbound_started=1 OR lease_phase='outbound_started' OR lease_until IS NULL
                                THEN 'uncertain' ELSE 'agent_running' END,
                        uncertain_reason=CASE
                            WHEN outbound_started=1 OR lease_phase='outbound_started' OR lease_until IS NULL
                                THEN ? ELSE NULL END,
                        updated_at=?
                    WHERE chat_id=? AND state='leased'
                      AND (lease_until IS NULL OR lease_until<=?)
                    """,
                    (uncertain_reason, now, chat_id, now),
                )
                self._transition_trigger_rows(
                    list(pending_expired_trigger_rows),
                    "pending",
                    now,
                )
                active = self._conn.execute(
                    """
                    SELECT COUNT(*) FROM onebot_queue_message
                    WHERE chat_id=? AND state='leased' AND lease_until IS NOT NULL AND lease_until>?
                    """,
                    (chat_id, now),
                ).fetchone()
                uncertain = self._conn.execute(
                    "SELECT COUNT(*) FROM onebot_queue_message WHERE chat_id=? AND state IN ('uncertain','failed')",
                    (chat_id,),
                ).fetchone()
                if (active and int(active[0]) > 0) or (uncertain and int(uncertain[0]) > 0):
                    self._conn.commit()
                    return None
                stale_trigger_rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_trigger AS trigger
                    WHERE trigger.chat_id=? AND trigger.status='claimed'
                      AND NOT EXISTS (
                          SELECT 1 FROM onebot_queue_message
                          WHERE onebot_queue_message.chat_id=trigger.chat_id
                            AND onebot_queue_message.lease_id=trigger.lease_id
                            AND onebot_queue_message.state='leased'
                      )
                    """,
                    (chat_id,),
                ).fetchall()
                self._transition_trigger_rows(list(stale_trigger_rows), "pending", now)
                self._conn.execute(
                    """
                    DELETE FROM onebot_queue_trigger
                    WHERE chat_id=? AND status='pending' AND NOT EXISTS (
                        SELECT 1 FROM onebot_queue_message AS message
                        WHERE message.chat_id=onebot_queue_trigger.chat_id
                          AND message.message_key=onebot_queue_trigger.message_key
                          AND message.state='pending'
                    )
                    """,
                    (chat_id,),
                )
                trigger_row = self._conn.execute(
                    """
                    SELECT trigger.* FROM onebot_queue_trigger AS trigger
                    WHERE trigger.chat_id=? AND trigger.status='pending'
                      AND EXISTS (
                          SELECT 1 FROM onebot_queue_message AS message
                          WHERE message.chat_id=trigger.chat_id
                            AND message.message_key=trigger.message_key
                            AND message.state='pending'
                            AND (message.next_attempt_at IS NULL OR message.next_attempt_at<=?)
                      )
                    ORDER BY trigger.created_at LIMIT 1
                    """,
                    (chat_id, now),
                ).fetchone()
                if trigger_row is None:
                    self._conn.commit()
                    return None
                message_rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_message
                    WHERE chat_id=? AND state='pending'
                      AND (next_attempt_at IS NULL OR next_attempt_at<=?)
                    ORDER BY seq
                    """,
                    (chat_id, now),
                ).fetchall()
                if not message_rows:
                    self._conn.commit()
                    return None
                lease_id = uuid.uuid4().hex
                until = now + max(1.0, float(lease_seconds))
                self._conn.executemany(
                    """
                    UPDATE onebot_queue_message
                    SET state='leased', lease_id=?, lease_until=?, lease_owner=?,
                        lease_phase='agent_running', outbound_started=0,
                        uncertain_reason=NULL, failure_reason=NULL,
                        next_attempt_at=NULL, attempts=attempts+1, updated_at=?
                    WHERE row_id=?
                    """,
                    [(lease_id, until, self._owner_id, now, int(row["row_id"])) for row in message_rows],
                )
                self._conn.execute(
                    """
                    UPDATE onebot_queue_trigger SET status='claimed', lease_id=?, lease_owner=?,
                        uncertain_reason=NULL, updated_at=?
                    WHERE chat_id=? AND status='pending' AND EXISTS (
                        SELECT 1 FROM onebot_queue_message AS message
                        WHERE message.chat_id=onebot_queue_trigger.chat_id
                          AND message.message_key=onebot_queue_trigger.message_key
                          AND message.lease_id=? AND message.state='leased'
                    )
                    """,
                    (lease_id, self._owner_id, now, chat_id, lease_id),
                )
                chat_row = self._conn.execute(
                    "SELECT summary, revision FROM onebot_queue_chat WHERE chat_id=?", (chat_id,)
                ).fetchone()
                self._conn.commit()
                return QueueLease(
                    chat_id=chat_id,
                    lease_id=lease_id,
                    messages=tuple(self._row_to_message(row) for row in message_rows),
                    trigger=self._row_to_trigger(trigger_row),
                    summary=str(chat_row[0] if chat_row else ""),
                    claimed_at=now,
                    lease_until=until,
                    phase="agent_running",
                    outbound_started=False,
                    attempts=max(int(row["attempts"]) for row in message_rows) + 1,
                    failure_count=max(int(row["failure_count"]) for row in message_rows),
                    revision=int(chat_row["revision"]) if chat_row is not None else 0,
                )
            except Exception:
                self._conn.rollback()
                raise

    def renew(self, lease: QueueLease | str, lease_seconds: float = 60.0) -> bool:
        """延长活动 lease；过期 lease 不会被复活。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._lock:
            now = self._now()
            until = now + max(1.0, float(lease_seconds))
            cursor = self._conn.execute(
                """
                UPDATE onebot_queue_message SET lease_until=?, updated_at=?
                WHERE lease_id=? AND lease_owner=? AND state='leased' AND lease_until>?
                """,
                (until, now, lease_id, self._owner_id, now),
            )
            self._conn.execute(
                "UPDATE onebot_queue_trigger SET updated_at=? WHERE lease_id=? AND lease_owner=? AND status='claimed'",
                (now, lease_id, self._owner_id),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def is_lease_current(self, lease: QueueLease | str) -> bool:
        """原子读取 lease 是否仍由当前 owner 持有且未过期。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1 FROM onebot_queue_message
                WHERE lease_id=? AND lease_owner=? AND state='leased'
                  AND lease_until IS NOT NULL AND lease_until>?
                LIMIT 1
                """,
                (lease_id, self._owner_id, self._now()),
            ).fetchone()
            return row is not None

    def mark_agent_started(self, lease: QueueLease | str) -> bool:
        """确认 Agent 可以继续运行；失效 lease 不会重新获得执行权。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._lock:
            now = self._now()
            cursor = self._conn.execute(
                """
                UPDATE onebot_queue_message
                SET lease_phase='agent_running', updated_at=?
                WHERE lease_id=? AND lease_owner=? AND state='leased'
                  AND lease_until IS NOT NULL AND lease_until>?
                  AND outbound_started=0
                """,
                (now, lease_id, self._owner_id, now),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def mark_outbound_started(self, lease: QueueLease | str) -> bool:
        """在访问非幂等 OneBot API 前持久化出站阶段并执行 fencing。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._lock:
            now = self._now()
            cursor = self._conn.execute(
                """
                UPDATE onebot_queue_message
                SET lease_phase='outbound_started', outbound_started=1, updated_at=?
                WHERE lease_id=? AND lease_owner=? AND state='leased'
                  AND lease_until IS NOT NULL AND lease_until>?
                """,
                (now, lease_id, self._owner_id, now),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def ack(self, lease: QueueLease | str) -> bool:
        """确认 lease，删除消息并更新确定性滚动摘要。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_message
                    WHERE lease_id=? AND lease_owner=? AND state='leased'
                      AND lease_until IS NOT NULL AND lease_until>?
                    ORDER BY seq
                    """,
                    (lease_id, self._owner_id, now),
                ).fetchall()
                if not rows:
                    self._conn.commit()
                    return False
                chat_id = str(rows[0]["chat_id"])
                chat = self._conn.execute(
                    "SELECT summary FROM onebot_queue_chat WHERE chat_id=?", (chat_id,)
                ).fetchone()
                summary = str(chat[0] if chat else "")
                summary = self._append_summary(summary, rows)
                self._conn.executemany(
                    "INSERT OR REPLACE INTO onebot_queue_dedupe(chat_id,message_key,seq,created_at) VALUES (?,?,?,?)",
                    [(str(row["chat_id"]), str(row["message_key"]), int(row["seq"]), now) for row in rows],
                )
                self._conn.execute(
                    "DELETE FROM onebot_queue_message WHERE lease_id=? AND lease_owner=?",
                    (lease_id, self._owner_id),
                )
                self._conn.execute(
                    "DELETE FROM onebot_queue_trigger WHERE lease_id=? AND lease_owner=?",
                    (lease_id, self._owner_id),
                )
                self._ensure_retriable_trigger(chat_id, now)
                self._conn.execute(
                    "UPDATE onebot_queue_chat SET summary=?, updated_at=? WHERE chat_id=?",
                    (summary, now, chat_id),
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def release(
        self,
        lease: QueueLease | str,
        *,
        reason: str | None = None,
        allow_after_outbound: bool = False,
    ) -> bool:
        """明确失败时释放 lease；出站 marker 后立即转为 uncertain。"""
        return self._change_lease_state(
            lease,
            "pending",
            "pending",
            reason=reason,
            allow_after_outbound=allow_after_outbound,
        )

    def mark_uncertain(self, lease: QueueLease | str, reason: str) -> bool:
        """出站结果未知时停止自动重试，转入人工处理状态。"""
        return self._change_lease_state(lease, "uncertain", "uncertain", reason=str(reason)[:512])

    def start_operation(
        self,
        *,
        fingerprint: str,
        tool_name: str,
        chat_type: str,
        chat_id: str,
        caller_user_id: str,
        params: Mapping[str, Any],
    ) -> OperationStart:
        """持久化管理动作开始标记，并阻断仍处于未知状态的重复动作。"""
        if chat_type not in {"group", "dm"}:
            raise ValueError(f"未知 operation chat_type: {chat_type!r}")
        params_json = json.dumps(
            dict(params),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        with self._lock:
            try:
                self._transaction()
                existing = self._conn.execute(
                    """
                    SELECT * FROM onebot_operation
                    WHERE fingerprint=? AND status IN ('started','unknown')
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (str(fingerprint),),
                ).fetchone()
                if existing is not None:
                    self._conn.commit()
                    return OperationStart(
                        started=False,
                        blocked=True,
                        operation=self._row_to_operation(existing),
                    )
                now = self._now()
                operation_id = uuid.uuid4().hex[:16]
                self._conn.execute(
                    """
                    INSERT INTO onebot_operation(
                        operation_id,fingerprint,tool_name,chat_type,chat_id,
                        caller_user_id,params_json,status,created_at,updated_at
                    ) VALUES (?,?,?,?,?,?,?,'started',?,?)
                    """,
                    (
                        operation_id,
                        str(fingerprint),
                        str(tool_name),
                        str(chat_type),
                        str(chat_id),
                        str(caller_user_id),
                        params_json,
                        now,
                        now,
                    ),
                )
                row = self._conn.execute(
                    "SELECT * FROM onebot_operation WHERE operation_id=?",
                    (operation_id,),
                ).fetchone()
                self._conn.commit()
                if row is None:
                    raise QueueError("管理动作台账写入后无法读取")
                return OperationStart(
                    started=True,
                    blocked=False,
                    operation=self._row_to_operation(row),
                )
            except Exception:
                self._conn.rollback()
                raise

    def finish_operation(
        self,
        operation_id: str,
        status: str,
        *,
        reason: str | None = None,
    ) -> bool:
        """结算已开始的管理动作；未知结果不会被转换为成功。"""
        if status not in {"succeeded", "known_failed", "unknown"}:
            raise ValueError("非法管理动作结算状态")
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE onebot_operation
                SET status=?, reason=?, updated_at=?
                WHERE operation_id=? AND status='started'
                """,
                (status, str(reason or "")[:512] or None, self._now(), str(operation_id)),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def resolve_operation(
        self,
        operation_id: str,
        action: str,
        *,
        chat_type: str,
        chat_id: str,
        caller_user_id: str,
    ) -> OperationRecord | None:
        """由同一群同一管理员把 unknown 台账置为 retry_armed 或 discarded。"""
        if action not in {"retry", "discard"}:
            raise ValueError("管理动作只能 resolve retry 或 discard")
        with self._lock:
            try:
                self._transaction()
                row = self._conn.execute(
                    "SELECT * FROM onebot_operation WHERE operation_id=?",
                    (str(operation_id),),
                ).fetchone()
                if row is None:
                    self._conn.commit()
                    return None
                if (
                    str(row["chat_type"]) != str(chat_type)
                    or str(row["chat_id"]) != str(chat_id)
                    or str(row["caller_user_id"]) != str(caller_user_id)
                ):
                    self._conn.commit()
                    return None
                target_status = "retry_armed" if action == "retry" else "discarded"
                if str(row["status"]) == "unknown":
                    self._conn.execute(
                        """
                        UPDATE onebot_operation
                        SET status=?, reason=?, updated_at=?
                        WHERE operation_id=? AND status='unknown'
                        """,
                        (
                            target_status,
                            f"管理员明确 {action}，等待后续预览确认"
                            if action == "retry"
                            else "管理员明确 discard",
                            self._now(),
                            str(operation_id),
                        ),
                    )
                updated = self._conn.execute(
                    "SELECT * FROM onebot_operation WHERE operation_id=?",
                    (str(operation_id),),
                ).fetchone()
                self._conn.commit()
                return self._row_to_operation(updated) if updated is not None else None
            except Exception:
                self._conn.rollback()
                raise

    def operation_records(self, chat_id: str, limit: int = 20) -> tuple[OperationRecord, ...]:
        """读取当前目标最近的管理动作台账，不返回 token。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM onebot_operation
                WHERE chat_id=?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (str(chat_id), max(1, min(100, int(limit)))),
            ).fetchall()
            return tuple(self._row_to_operation(row) for row in rows)

    def unknown_operation_count(self, chat_id: str | None = None) -> int:
        """统计仍需人工处理的未知管理动作。"""
        with self._lock:
            if chat_id is None:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM onebot_operation WHERE status='unknown'"
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM onebot_operation WHERE chat_id=? AND status='unknown'",
                    (str(chat_id),),
                ).fetchone()
            return int(row[0] if row else 0)

    def _change_lease_state(
        self,
        lease: QueueLease | str,
        message_state: str,
        trigger_state: str,
        *,
        reason: str | None = None,
        allow_after_outbound: bool = False,
    ) -> bool:
        """原子改变 lease 对应消息和触发请求状态。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_message
                    WHERE lease_id=? AND lease_owner=? AND state='leased'
                    """,
                    (lease_id, self._owner_id),
                ).fetchall()
                if not rows or any(
                    row["lease_until"] is None or float(row["lease_until"]) <= now
                    for row in rows
                ):
                    self._conn.commit()
                    return False
                if message_state == "pending" and any(bool(row["outbound_started"]) for row in rows):
                    # marker 一旦落盘，release 也必须立即转 uncertain；
                    # allow_after_outbound 仅保留接口兼容，不能绕过 fencing。
                    message_state = "uncertain"
                    trigger_state = "uncertain"
                    reason = reason or "出站已开始，明确失败也需要管理员确认"
                trigger_rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_trigger
                    WHERE lease_id=? AND lease_owner=? AND status='claimed'
                    """,
                    (lease_id, self._owner_id),
                ).fetchall()
                final_message_state = message_state
                final_trigger_state = trigger_state
                next_attempt_at: float | None = None
                failure_count = max(int(row["failure_count"]) for row in rows)
                failure_reason = str(reason or "")[:512] or None
                if message_state == "pending":
                    failure_count += 1
                    if failure_count >= self.max_attempts:
                        final_message_state = "failed"
                        final_trigger_state = "failed"
                    else:
                        backoff_index = min(failure_count - 1, len(self.backoff_seconds) - 1)
                        delay = min(MAX_BACKOFF_SECONDS, self.backoff_seconds[backoff_index])
                        next_attempt_at = now + delay
                cursor = self._conn.execute(
                    """
                    UPDATE onebot_queue_message SET state=?, lease_id=NULL, lease_until=NULL,
                        lease_owner=NULL, lease_phase=?, outbound_started=outbound_started,
                        uncertain_reason=?, failure_count=?, next_attempt_at=?, failure_reason=?, updated_at=?
                    WHERE lease_id=? AND lease_owner=? AND state='leased'
                    """,
                    (
                        final_message_state,
                        "uncertain" if final_message_state == "uncertain" else final_message_state,
                        failure_reason if final_message_state in {"uncertain", "failed"} else None,
                        failure_count,
                        next_attempt_at,
                        failure_reason if final_message_state == "failed" else None,
                        now,
                        lease_id,
                        self._owner_id,
                    ),
                )
                self._transition_trigger_rows(
                    list(trigger_rows),
                    final_trigger_state,
                    now,
                    reason=failure_reason,
                )
                if final_trigger_state == "pending":
                    self._ensure_retriable_trigger(str(rows[0]["chat_id"]), now)
                self._conn.commit()
                return cursor.rowcount > 0
            except Exception:
                self._conn.rollback()
                raise

    def resolve_uncertain(self, chat_id: str, action: str) -> int:
        """管理员明确 retry 或 discard uncertain/failed 消息。"""
        if action not in {"retry", "discard"}:
            raise ValueError("队列只能 resolve retry 或 discard")
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                rows = self._conn.execute(
                    "SELECT row_id FROM onebot_queue_message WHERE chat_id=? AND state IN ('uncertain','failed')",
                    (str(chat_id),),
                ).fetchall()
                trigger_rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_trigger
                    WHERE chat_id=? AND status IN ('uncertain','failed')
                    """,
                    (str(chat_id),),
                ).fetchall()
                if action == "retry":
                    self._conn.execute(
                        """
                        UPDATE onebot_queue_message
                        SET state='pending', lease_id=NULL, lease_until=NULL, lease_owner=NULL,
                            lease_phase='pending', outbound_started=0, uncertain_reason=NULL,
                            failure_reason=NULL, failure_count=0, attempts=0,
                            next_attempt_at=?, updated_at=?
                        WHERE chat_id=? AND state IN ('uncertain','failed')
                        """,
                        (now, now, str(chat_id)),
                    )
                    self._transition_trigger_rows(list(trigger_rows), "pending", now)
                else:
                    self._conn.execute(
                        """
                        INSERT OR REPLACE INTO onebot_queue_dedupe(
                            chat_id, message_key, seq, created_at
                        )
                        SELECT chat_id, message_key, seq, ?
                        FROM onebot_queue_message
                        WHERE chat_id=? AND state IN ('uncertain','failed')
                        """,
                        (now, str(chat_id)),
                    )
                    self._conn.execute(
                        "DELETE FROM onebot_queue_message WHERE chat_id=? AND state IN ('uncertain','failed')", (str(chat_id),)
                    )
                    self._conn.execute(
                        "DELETE FROM onebot_queue_trigger WHERE chat_id=? AND status IN ('uncertain','failed')", (str(chat_id),)
                    )
                self._conn.commit()
                return len(rows)
            except Exception:
                self._conn.rollback()
                raise

    def status(self, chat_id: str) -> dict[str, Any]:
        """读取队列数量、阶段、退避原因、摘要和暂停状态。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT state, COUNT(*) AS count, COALESCE(SUM(byte_size),0) AS bytes FROM onebot_queue_message WHERE chat_id=? GROUP BY state",
                (str(chat_id),),
            ).fetchall()
            counts = {str(row["state"]): int(row["count"]) for row in rows}
            bytes_total = int(sum(int(row["bytes"]) for row in rows))
            chat = self._conn.execute(
                "SELECT summary, paused, next_seq, revision FROM onebot_queue_chat WHERE chat_id=?", (str(chat_id),)
            ).fetchone()
            trigger = self._conn.execute(
                "SELECT COUNT(*) FROM onebot_queue_trigger WHERE chat_id=? AND status IN ('pending','uncertain','failed')",
                (str(chat_id),),
            ).fetchone()
            pending_triggers = self._conn.execute(
                "SELECT COUNT(*) FROM onebot_queue_trigger WHERE chat_id=? AND status='pending'",
                (str(chat_id),),
            ).fetchone()
            blocked_triggers = self._conn.execute(
                "SELECT COUNT(*) FROM onebot_queue_trigger WHERE chat_id=? AND status IN ('uncertain','failed')",
                (str(chat_id),),
            ).fetchone()
            active_lease = self._conn.execute(
                "SELECT MIN(lease_until) FROM onebot_queue_message WHERE chat_id=? AND state='leased'",
                (str(chat_id),),
            ).fetchone()
            reasons = self._conn.execute(
                """
                SELECT DISTINCT reason FROM (
                    SELECT uncertain_reason AS reason FROM onebot_queue_message
                    WHERE chat_id=? AND uncertain_reason IS NOT NULL
                    UNION ALL
                    SELECT failure_reason AS reason FROM onebot_queue_message
                    WHERE chat_id=? AND failure_reason IS NOT NULL
                ) WHERE reason IS NOT NULL
                """,
                (str(chat_id), str(chat_id)),
            ).fetchall()
            next_retry = self._conn.execute(
                """
                SELECT MIN(next_attempt_at) FROM onebot_queue_message
                WHERE chat_id=? AND state='pending' AND next_attempt_at IS NOT NULL
                """,
                (str(chat_id),),
            ).fetchall()
            phase = self._conn.execute(
                """
                SELECT lease_phase, outbound_started
                FROM onebot_queue_message
                WHERE chat_id=? AND state IN ('leased','uncertain','failed')
                ORDER BY CASE state WHEN 'leased' THEN 0 ELSE 1 END, updated_at DESC
                LIMIT 1
                """,
                (str(chat_id),),
            ).fetchone()
            failure_stats = self._conn.execute(
                """
                SELECT COALESCE(MAX(failure_count), 0), COALESCE(MAX(attempts), 0)
                FROM onebot_queue_message WHERE chat_id=?
                """,
                (str(chat_id),),
            ).fetchone()
            chat_type = self._conn.execute(
                "SELECT chat_type FROM onebot_queue_message WHERE chat_id=? LIMIT 1",
                (str(chat_id),),
            ).fetchone()
            return {
                "chat_id": str(chat_id),
                "chat_type": str(chat_type[0]) if chat_type else None,
                "pending": counts.get("pending", 0),
                "leased": counts.get("leased", 0),
                "uncertain": counts.get("uncertain", 0),
                "failed": counts.get("failed", 0),
                "bytes": bytes_total,
                "trigger_requests": int(trigger[0] if trigger else 0),
                "pending_trigger_requests": int(pending_triggers[0] if pending_triggers else 0),
                "blocked_trigger_requests": int(blocked_triggers[0] if blocked_triggers else 0),
                "lease_until": float(active_lease[0]) if active_lease and active_lease[0] is not None else None,
                "lease_phase": str(phase[0]) if phase else None,
                "outbound_started": bool(phase[1]) if phase else False,
                "failure_count": int(failure_stats[0]) if failure_stats else 0,
                "attempts": int(failure_stats[1]) if failure_stats else 0,
                "next_retry_at": float(next_retry[0][0]) if next_retry and next_retry[0][0] is not None else None,
                "failure_reasons": [str(row[0]) for row in reasons],
                "uncertain_reasons": [str(row[0]) for row in reasons],
                "paused": bool(chat["paused"]) if chat else False,
                "next_seq": int(chat["next_seq"]) if chat else 1,
                "latest_seq": max(0, int(chat["next_seq"]) - 1) if chat else 0,
                "revision": int(chat["revision"]) if chat else 0,
                "summary": str(chat["summary"]) if chat else "",
            }

    def revision(self, chat_id: str) -> int:
        """读取群消息 revision，供旁路判断检测判断期间的新消息。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT revision FROM onebot_queue_chat WHERE chat_id=?", (str(chat_id),)
            ).fetchone()
            return int(row[0]) if row else 0

    def status_for_lease(self, lease_id: str) -> dict[str, Any]:
        """读取 lease 的持久阶段，供 adapter 在 completion 时判定结果。"""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT lease_phase, outbound_started, state, lease_until, chat_id, chat_type
                FROM onebot_queue_message WHERE lease_id=? LIMIT 1
                """,
                (str(lease_id),),
            ).fetchone()
            if row is None:
                return {}
            return {
                "lease_phase": str(row["lease_phase"] or ""),
                "outbound_started": bool(row["outbound_started"]),
                "state": str(row["state"]),
                "lease_until": float(row["lease_until"]) if row["lease_until"] is not None else None,
                "chat_id": str(row["chat_id"]),
                "chat_type": str(row["chat_type"]),
            }

    def clear(self, chat_id: str) -> int:
        """清理 pending 消息和摘要；活动或 uncertain 状态必须先显式处理。"""
        with self._lock:
            try:
                self._transaction()
                active = self._conn.execute(
                    "SELECT COUNT(*) FROM onebot_queue_message WHERE chat_id=? AND state IN ('leased','uncertain','failed')",
                    (str(chat_id),),
                ).fetchone()
                if active and int(active[0]) > 0:
                    raise QueueBusy("当前群存在 leased/uncertain/failed，不能直接 clear")
                count = self._conn.execute(
                    "SELECT COUNT(*) FROM onebot_queue_message WHERE chat_id=? AND state='pending'", (str(chat_id),)
                ).fetchone()[0]
                now = self._now()
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO onebot_queue_dedupe(
                        chat_id, message_key, seq, created_at
                    )
                    SELECT chat_id, message_key, seq, ?
                    FROM onebot_queue_message
                    WHERE chat_id=? AND state='pending'
                    """,
                    (now, str(chat_id)),
                )
                self._conn.execute("DELETE FROM onebot_queue_message WHERE chat_id=? AND state='pending'", (str(chat_id),))
                self._conn.execute(
                    "DELETE FROM onebot_queue_trigger WHERE chat_id=? AND status IN ('pending','claimed')",
                    (str(chat_id),),
                )
                self._conn.execute(
                    "UPDATE onebot_queue_chat SET summary='', revision=revision+1, updated_at=? WHERE chat_id=?",
                    (now, str(chat_id)),
                )
                self._conn.commit()
                return int(count)
            except Exception:
                self._conn.rollback()
                raise

    def set_paused(self, chat_id: str, paused: bool) -> None:
        """持久化群级自动 dispatch 暂停状态。"""
        with self._lock:
            self._conn.execute(
                "INSERT INTO onebot_queue_chat(chat_id, paused, updated_at) VALUES (?, ?, ?) ON CONFLICT(chat_id) DO UPDATE SET paused=excluded.paused, updated_at=excluded.updated_at",
                (str(chat_id), int(bool(paused)), self._now()),
            )
            self._conn.commit()

    def recover_trigger_requests(self) -> tuple[TriggerRequest, ...]:
        """启动恢复过期 lease，并返回仍需 dispatch 的持久触发请求。"""
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                uncertain_reason = "lease 过期时出站阶段未知，需管理员确认"
                uncertain_trigger_rows = self._conn.execute(
                    """
                    SELECT DISTINCT trigger.*
                    FROM onebot_queue_trigger AS trigger
                    JOIN onebot_queue_message AS message
                      ON message.lease_id=trigger.lease_id
                     AND message.chat_id=trigger.chat_id
                    WHERE trigger.status='claimed'
                      AND message.state='leased'
                      AND (message.lease_until IS NULL OR message.lease_until<=?)
                      AND (
                          message.outbound_started=1
                          OR message.lease_phase='outbound_started'
                          OR message.lease_until IS NULL
                      )
                    """,
                    (now,),
                ).fetchall()
                pending_trigger_rows = self._conn.execute(
                    """
                    SELECT DISTINCT trigger.*
                    FROM onebot_queue_trigger AS trigger
                    JOIN onebot_queue_message AS message
                      ON message.lease_id=trigger.lease_id
                     AND message.chat_id=trigger.chat_id
                    WHERE trigger.status='claimed'
                      AND message.state='leased'
                      AND message.lease_until IS NOT NULL
                      AND message.lease_until<=?
                      AND message.outbound_started=0
                      AND message.lease_phase='agent_running'
                    """,
                    (now,),
                ).fetchall()
                self._conn.execute(
                    """
                    UPDATE onebot_queue_message
                    SET state=CASE
                            WHEN lease_until IS NULL OR outbound_started=1 OR lease_phase='outbound_started'
                                THEN 'uncertain' ELSE 'pending' END,
                        lease_id=NULL, lease_until=NULL, lease_owner=NULL,
                        lease_phase=CASE
                            WHEN lease_until IS NULL OR outbound_started=1 OR lease_phase='outbound_started'
                                THEN 'uncertain' ELSE 'agent_running' END,
                        uncertain_reason=CASE
                            WHEN lease_until IS NULL OR outbound_started=1 OR lease_phase='outbound_started'
                                THEN 'lease 过期时出站阶段未知，需管理员确认' ELSE NULL END,
                        updated_at=?
                    WHERE state='leased' AND (lease_until IS NULL OR lease_until<=?)
                    """,
                    (now, now),
                )
                self._transition_trigger_rows(
                    list(uncertain_trigger_rows),
                    "uncertain",
                    now,
                    reason=uncertain_reason,
                )
                self._transition_trigger_rows(
                    list(pending_trigger_rows),
                    "pending",
                    now,
                )
                stale_trigger_rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_trigger AS trigger
                    WHERE trigger.status='claimed' AND NOT EXISTS (
                        SELECT 1 FROM onebot_queue_message AS message
                        WHERE message.chat_id=trigger.chat_id
                          AND message.lease_id=trigger.lease_id
                          AND message.state='leased'
                    )
                    """
                ).fetchall()
                self._transition_trigger_rows(list(stale_trigger_rows), "pending", now)
                self._conn.execute(
                    """
                    DELETE FROM onebot_queue_trigger
                    WHERE status='pending' AND NOT EXISTS (
                        SELECT 1 FROM onebot_queue_message
                        WHERE onebot_queue_message.chat_id=onebot_queue_trigger.chat_id
                          AND onebot_queue_message.message_key=onebot_queue_trigger.message_key
                    )
                    """
                )
                self._conn.commit()
                rows = self._conn.execute(
                    """
                    SELECT trigger.* FROM onebot_queue_trigger AS trigger
                    WHERE trigger.status='pending' AND EXISTS (
                        SELECT 1 FROM onebot_queue_message AS message
                        WHERE message.chat_id=trigger.chat_id
                          AND message.message_key=trigger.message_key
                          AND message.state='pending'
                          AND (message.next_attempt_at IS NULL OR message.next_attempt_at<=?)
                    ) ORDER BY trigger.created_at
                    """,
                    (now,),
                ).fetchall()
                return tuple(self._row_to_trigger(row) for row in rows)
            except Exception:
                self._conn.rollback()
                raise

    def _append_summary(self, current: str, rows: list[sqlite3.Row]) -> str:
        """将已确认消息追加为确定性摘要，并保留最近内容。"""
        parts = [current] if current else []
        original_start = max(0, len(rows) - self.recent_originals)
        for index, row in enumerate(rows):
            text = str(row["text"])
            raw_text = str(row["raw_text"] or "")
            if index >= original_start and raw_text and raw_text != text:
                text = f"{text} [原文: {raw_text}]"
            parts.append(f"#{row['seq']} {row['user_name']}: {text}")
        result = "\n".join(parts)
        if len(result.encode("utf-8")) <= self.max_summary_bytes:
            return result
        marker = "[更早的群消息摘要已裁剪]"
        recent: list[str] = []
        used = len(marker.encode("utf-8")) + 1
        for line in reversed(result.splitlines()):
            line_bytes = len(line.encode("utf-8")) + (1 if recent else 0)
            if used + line_bytes > self.max_summary_bytes:
                break
            recent.append(line)
            used += line_bytes
        if not recent:
            return marker + "\n" + self._truncate_utf8_tail(result, max(0, self.max_summary_bytes - used))
        return marker + "\n" + "\n".join(reversed(recent))

    def _row_to_message(self, row: sqlite3.Row) -> QueueMessage:
        """把 SQLite 行转换为不可变队列消息。"""
        try:
            metadata = json.loads(str(row["metadata_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        return QueueMessage(
            chat_id=str(row["chat_id"]),
            chat_type=str(row["chat_type"]),
            message_id=str(row["message_id"] or row["message_key"]),
            user_id=str(row["user_id"]),
            user_name=str(row["user_name"]),
            text=str(row["text"]),
            raw_text=str(row["raw_text"] or ""),
            metadata=metadata if isinstance(metadata, dict) else {},
            created_at=float(row["created_at"]),
            message_key=str(row["message_key"]),
            seq=int(row["seq"]),
            byte_size=int(row["byte_size"]),
        )

    def _row_to_trigger(self, row: sqlite3.Row) -> TriggerRequest:
        """把 SQLite 行转换为触发请求。"""
        return TriggerRequest(
            request_id=str(row["request_id"]),
            chat_id=str(row["chat_id"]),
            message_key=str(row["message_key"]),
            reason=str(row["reason"]),
            caller_user_id=str(row["caller_user_id"]),
            caller_user_name=str(row["caller_user_name"]),
            created_at=float(row["created_at"]),
        )

    def _row_to_operation(self, row: sqlite3.Row) -> OperationRecord:
        """把 SQLite 行转换为不可变管理动作记录。"""
        try:
            params = json.loads(str(row["params_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            params = {}
        return OperationRecord(
            operation_id=str(row["operation_id"]),
            fingerprint=str(row["fingerprint"]),
            tool_name=str(row["tool_name"]),
            chat_type=str(row["chat_type"]),
            chat_id=str(row["chat_id"]),
            caller_user_id=str(row["caller_user_id"]),
            params=params if isinstance(params, Mapping) else {},
            status=str(row["status"]),
            reason=str(row["reason"]) if row["reason"] is not None else None,
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def operation_summary(self, operation: OperationRecord) -> dict[str, Any]:
        """生成不含完整参数的管理动作状态摘要。"""
        safe_params: dict[str, str] = {}
        visible_keys = {"message_id", "user_id", "duration", "reject_add_request", "enable"}
        for key in sorted(operation.params):
            safe_params[str(key)] = (
                str(operation.params[key])[:64]
                if str(key) in visible_keys
                else "<redacted>"
            )
        return {
            "operation_id": operation.operation_id,
            "fingerprint": operation.fingerprint[:12],
            "tool": operation.tool_name,
            "target": {"chat_type": operation.chat_type, "chat_id": operation.chat_id},
            "caller_user_id": operation.caller_user_id,
            "status": operation.status,
            "reason": operation.reason,
            "created_at": operation.created_at,
            "updated_at": operation.updated_at,
            "params_summary": safe_params,
        }

    def reopen(self) -> None:
        """重新打开同一路径数据库并更换 owner，隔离旧 task。"""
        with self._lock:
            if not self._closed:
                return
            self._conn = self._open_connection()
            self._owner_id = uuid.uuid4().hex
            self._closed = False
            self._migrate()

    def abandon_owner_leases(self) -> dict[str, int]:
        """断开时结算当前 owner 的 lease，避免旧 turn 在重连后复活。"""
        with self._lock:
            try:
                self._transaction()
                now = self._now()
                lease_rows = self._conn.execute(
                    """
                    SELECT lease_id,
                           MAX(CASE WHEN outbound_started=1
                                    OR lease_phase='outbound_started'
                                    OR lease_until IS NULL THEN 1 ELSE 0 END) AS uncertain
                    FROM onebot_queue_message
                    WHERE state='leased' AND lease_owner=?
                    GROUP BY lease_id
                    """,
                    (self._owner_id,),
                ).fetchall()
                pending = 0
                uncertain = 0
                reason = "adapter 断开时出站结果未知，需管理员确认"
                for row in lease_rows:
                    lease_id = str(row["lease_id"])
                    is_uncertain = bool(row["uncertain"])
                    trigger_rows = self._conn.execute(
                        """
                        SELECT * FROM onebot_queue_trigger
                        WHERE lease_id=? AND lease_owner=? AND status='claimed'
                        """,
                        (lease_id, self._owner_id),
                    ).fetchall()
                    if is_uncertain:
                        self._conn.execute(
                            """
                            UPDATE onebot_queue_message
                            SET state='uncertain', lease_id=NULL, lease_until=NULL,
                                lease_owner=NULL, lease_phase='uncertain',
                                outbound_started=1, uncertain_reason=?, updated_at=?
                            WHERE lease_id=? AND lease_owner=? AND state='leased'
                            """,
                            (reason, now, lease_id, self._owner_id),
                        )
                        self._transition_trigger_rows(
                            list(trigger_rows),
                            "uncertain",
                            now,
                            reason=reason,
                        )
                        uncertain += 1
                    else:
                        self._conn.execute(
                            """
                            UPDATE onebot_queue_message
                            SET state='pending', lease_id=NULL, lease_until=NULL,
                                lease_owner=NULL, lease_phase='pending',
                                outbound_started=0, next_attempt_at=NULL,
                                uncertain_reason=NULL, failure_reason=NULL, updated_at=?
                            WHERE lease_id=? AND lease_owner=? AND state='leased'
                            """,
                            (now, lease_id, self._owner_id),
                        )
                        self._transition_trigger_rows(
                            list(trigger_rows),
                            "pending",
                            now,
                        )
                        if trigger_rows:
                            self._ensure_retriable_trigger(
                                str(trigger_rows[0]["chat_id"]),
                                now,
                            )
                        pending += 1
                self._conn.commit()
                return {"pending": pending, "uncertain": uncertain}
            except Exception:
                self._conn.rollback()
                raise

    @property
    def closed(self) -> bool:
        """返回连接是否已关闭。"""
        return self._closed

    def close(self) -> None:
        """关闭 SQLite 连接。"""
        with self._lock:
            if self._closed:
                return
            self._conn.close()
            self._closed = True
