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
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 9
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
    anchor_id: str | None = None


@dataclass(frozen=True)
class TurnAnchor:
    """一条持久 TurnAnchor；它固定 batch 边界和本轮 authority 来源。"""

    request_id: str
    chat_id: str
    message_key: str
    reason: str
    caller_user_id: str
    caller_user_name: str
    created_at: float
    anchor_seq: int | None = None
    anchor_kind: str = "message"
    batch_start_seq: int | None = None
    control_message_id: str | None = None
    failure_count: int = 0
    next_attempt_at: float | None = None
    failure_reason: str | None = None
    status: str = "pending"
    lease_id: str | None = None
    uncertain_reason: str | None = None

    @property
    def anchor_id(self) -> str:
        """返回持久 anchor 标识；与旧 request_id 是同一事实。"""
        return self.request_id

    @classmethod
    def create(
        cls,
        chat_id: str,
        message_key: str,
        reason: str,
        caller_user_id: str,
        caller_user_name: str,
        *,
        anchor_kind: str = "message",
        anchor_seq: int | None = None,
        control_message_id: str | None = None,
    ) -> TurnAnchor:
        """创建一个尚未落盘的 TurnAnchor。"""
        if anchor_kind not in {"message", "operator", "service", "legacy"}:
            raise ValueError(f"未知 anchor_kind: {anchor_kind!r}")
        return cls(
            request_id=uuid.uuid4().hex,
            chat_id=str(chat_id),
            message_key=str(message_key),
            reason=str(reason),
            caller_user_id=str(caller_user_id),
            caller_user_name=str(caller_user_name),
            created_at=time.time(),
            anchor_seq=anchor_seq,
            anchor_kind=anchor_kind,
            control_message_id=(
                str(control_message_id) if control_message_id is not None else None
            ),
        )


# v0.3.x 的公开名称保留为兼容别名；新代码统一把该对象视为 TurnAnchor。
TriggerRequest = TurnAnchor


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
    trigger: TurnAnchor
    summary: str
    claimed_at: float
    lease_until: float
    phase: str = "agent_running"
    outbound_started: bool = False
    attempts: int = 0
    failure_count: int = 0

    @property
    def anchor(self) -> TurnAnchor:
        """返回当前 lease 对应的唯一 TurnAnchor。"""
        return self.trigger


@dataclass(frozen=True)
class ReactionRecord:
    """需要由 OneBot 控制面清理的消息 reaction。"""

    lease_id: str
    chat_id: str
    message_id: str
    state: str
    created_at: float
    updated_at: float
    attempts: int = 0
    next_attempt_at: float | None = None
    last_error: str | None = None
    anchor_id: str | None = None
    reaction_kind: str = "processing"
    emoji_id: str = ""


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
        self._condition = threading.Condition(self._lock)
        self._active_operations = 0
        self._closed = False
        self._owner_id = uuid.uuid4().hex
        self._conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._migrate()
        except Exception:
            self._closed = True
            self._conn.close()
            raise

    def _migrate(self) -> None:
        """创建当前 schema 或从已知旧版本迁移。"""
        with self._lock:
            version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise QueueError(
                    f"OneBot11 queue schema {version} 高于支持版本 {SCHEMA_VERSION}"
                )
            try:
                self._transaction()
                self._create_tables(commit=False)
                self._migrate_columns(version)
                if version < 9:
                    self._migrate_to_v9(version)
                self._create_indexes()
                if version < SCHEMA_VERSION:
                    self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _migrate_columns(self, version: int) -> None:
        """为已存在的队列文件补充增量列；未知结构直接拒绝启动。"""
        chat_columns = {
            str(row[1])
            for row in self._conn.execute("PRAGMA table_info(onebot_queue_chat)").fetchall()
        }
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
        required_chat = {"chat_id", "next_seq", "summary", "paused", "updated_at"}
        required_message = {
            "chat_id", "message_key", "chat_type", "user_id", "user_name", "text",
            "raw_text", "metadata_json", "seq", "byte_size", "state", "lease_id",
            "lease_until", "attempts", "created_at", "updated_at",
        }
        required_trigger = {
            "request_id", "chat_id", "message_key", "reason", "caller_user_id",
            "caller_user_name", "status", "lease_id", "created_at", "updated_at",
        }
        if (
            not required_chat.issubset(chat_columns)
            or not required_message.issubset(message_columns)
            or not required_trigger.issubset(trigger_columns)
        ):
            raise QueueError("OneBot11 queue schema 缺少必需列，无法安全迁移")
        additions = (
            ("onebot_queue_chat", chat_columns, "revision", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_chat", chat_columns, "last_trigger_at", "REAL"),
            ("onebot_queue_chat", chat_columns, "llm_judged_seq", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_chat", chat_columns, "llm_next_attempt_at", "REAL"),
            ("onebot_queue_chat", chat_columns, "llm_failure_count", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_chat", chat_columns, "llm_last_error", "TEXT"),
            ("onebot_queue_message", message_columns, "message_id", "TEXT NOT NULL DEFAULT ''"),
            ("onebot_queue_message", message_columns, "lease_owner", "TEXT"),
            ("onebot_queue_message", message_columns, "uncertain_reason", "TEXT"),
            ("onebot_queue_message", message_columns, "lease_phase", "TEXT NOT NULL DEFAULT 'pending'"),
            ("onebot_queue_message", message_columns, "outbound_started", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_message", message_columns, "failure_count", "INTEGER NOT NULL DEFAULT 0"),
            ("onebot_queue_message", message_columns, "next_attempt_at", "REAL"),
            ("onebot_queue_message", message_columns, "failure_reason", "TEXT"),
            ("onebot_queue_message", message_columns, "anchor_id", "TEXT"),
            ("onebot_queue_trigger", trigger_columns, "lease_owner", "TEXT"),
            ("onebot_queue_trigger", trigger_columns, "uncertain_reason", "TEXT"),
            ("onebot_queue_trigger", trigger_columns, "anchor_seq", "INTEGER"),
            (
                "onebot_queue_trigger",
                trigger_columns,
                "anchor_kind",
                "TEXT NOT NULL DEFAULT 'message'",
            ),
            ("onebot_queue_trigger", trigger_columns, "batch_start_seq", "INTEGER"),
            ("onebot_queue_trigger", trigger_columns, "control_message_id", "TEXT"),
            (
                "onebot_queue_trigger",
                trigger_columns,
                "failure_count",
                "INTEGER NOT NULL DEFAULT 0",
            ),
            ("onebot_queue_trigger", trigger_columns, "next_attempt_at", "REAL"),
            ("onebot_queue_trigger", trigger_columns, "failure_reason", "TEXT"),
        )
        for table, columns, name, definition in additions:
            if name not in columns:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
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
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS onebot_queue_reaction (
                lease_id TEXT PRIMARY KEY,
                chat_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','maybe_set')),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                last_error TEXT
            )
            """
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
        # v5 已经具备 lease phase/outbound marker；v5 -> v6 不应把活动 turn
        # 无条件升级成 unknown，只迁移真正无法判断阶段的旧文件。
        if version < 5 or rebuilt_legacy:
            self._mark_legacy_leases_uncertain()
        self._conn.execute(
            """
            UPDATE onebot_queue_message
            SET lease_phase='pending'
            WHERE state='pending' AND lease_id IS NULL AND outbound_started=0
            """
        )
        self._create_indexes()
        # 事务由 _migrate 统一提交，避免中途提升 schema 后留下半迁移文件。

    def _migrate_to_v9(self, version: int) -> None:
        """把旧 trigger/batch 状态保守迁移为有序 TurnAnchor。"""
        now = self._now()
        active = self._conn.execute(
            """
            SELECT COUNT(*) FROM onebot_queue_message
            WHERE state='leased' AND lease_until IS NOT NULL AND lease_until>?
            """,
            (now,),
        ).fetchone()
        if version >= 8 and active and int(active[0]) > 0:
            raise QueueBusy("检测到仍有效的 v8 lease；请停止旧进程并等待租约到期后再升级")

        # 旧 LLM trigger 的 caller 可能与 message_key 不一致，不能升级成权限锚点。
        self._conn.execute(
            "DELETE FROM onebot_queue_trigger WHERE status='pending' AND reason='llm'"
        )
        self._conn.execute(
            """
            UPDATE onebot_queue_trigger
            SET anchor_seq=(
                    SELECT message.seq FROM onebot_queue_message AS message
                    WHERE message.chat_id=onebot_queue_trigger.chat_id
                      AND message.message_key=onebot_queue_trigger.message_key
                ),
                anchor_kind=CASE
                    WHEN reason IN ('admin_flush','admin_resolve_retry') THEN 'operator'
                    ELSE 'message'
                END,
                control_message_id=CASE
                    WHEN reason IN ('admin_flush','admin_resolve_retry') THEN message_key
                    ELSE control_message_id
                END
            WHERE status='pending'
            """
        )

        blocked_chats = self._conn.execute(
            """
            SELECT DISTINCT chat_id FROM onebot_queue_message
            WHERE state IN ('leased','uncertain','failed')
            """
        ).fetchall()
        for blocked in blocked_chats:
            chat_id = str(blocked[0])
            rows = self._conn.execute(
                """
                SELECT MIN(seq), MAX(seq)
                FROM onebot_queue_message
                WHERE chat_id=? AND state IN ('leased','uncertain','failed')
                """,
                (chat_id,),
            ).fetchone()
            if rows is None or rows[1] is None:
                continue
            anchor_id = f"legacy-{uuid.uuid4().hex}"
            self._conn.execute(
                """
                DELETE FROM onebot_queue_trigger
                WHERE chat_id=? AND status IN ('claimed','uncertain','failed')
                """,
                (chat_id,),
            )
            self._conn.execute(
                """
                INSERT INTO onebot_queue_trigger(
                    request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                    status,anchor_seq,anchor_kind,batch_start_seq,uncertain_reason,
                    failure_reason,created_at,updated_at
                ) VALUES (?,?,?,?,?,?, 'uncertain', ?, 'legacy', ?, ?, ?, ?, ?)
                """,
                (
                    anchor_id,
                    chat_id,
                    f"legacy:{chat_id}:{int(rows[1])}",
                    "legacy_hold",
                    "",
                    "legacy",
                    int(rows[1]),
                    int(rows[0]),
                    "v8 blocked batch 无法安全重建单锚点权限，需管理员处理",
                    "v8 blocked batch migration hold",
                    now,
                    now,
                ),
            )
            self._conn.execute(
                """
                UPDATE onebot_queue_message
                SET anchor_id=?, state='uncertain', lease_id=NULL, lease_until=NULL,
                    lease_owner=NULL, lease_phase='uncertain', outbound_started=1,
                    uncertain_reason='v8 blocked batch 无法安全重建单锚点权限，需管理员处理',
                    updated_at=?
                WHERE chat_id=? AND state IN ('leased','uncertain','failed')
                """,
                (anchor_id, now, chat_id),
            )

        # 找不到消息的旧 pending trigger 也不能借用其他用户消息，转成 legacy hold。
        self._conn.execute(
            """
            UPDATE onebot_queue_trigger
            SET anchor_kind='legacy', status='uncertain',
                uncertain_reason='旧 trigger 找不到锚点消息，需管理员处理'
            WHERE anchor_seq IS NULL
            """
        )
        self._conn.execute(
            """
            UPDATE onebot_queue_message
            SET anchor_id=(
                SELECT trigger.request_id FROM onebot_queue_trigger AS trigger
                WHERE trigger.chat_id=onebot_queue_message.chat_id
                  AND trigger.message_key=onebot_queue_message.message_key
                  AND trigger.anchor_kind!='legacy'
            )
            WHERE anchor_id IS NULL AND EXISTS (
                SELECT 1 FROM onebot_queue_trigger AS trigger
                WHERE trigger.chat_id=onebot_queue_message.chat_id
                  AND trigger.message_key=onebot_queue_message.message_key
                  AND trigger.anchor_kind!='legacy'
            )
            """
        )
        self._rebuild_reaction_table_v9()
        self._conn.execute("DROP INDEX IF EXISTS idx_onebot_queue_trigger_status")
        self._create_indexes()

    def _rebuild_reaction_table_v9(self) -> None:
        """把 lease 主键 reaction 表迁移为每 anchor/阶段一条清理记录。"""
        columns = {
            str(row[1])
            for row in self._conn.execute(
                "PRAGMA table_info(onebot_queue_reaction)"
            ).fetchall()
        }
        if {"anchor_id", "reaction_kind", "emoji_id"}.issubset(columns):
            return
        self._conn.execute("DROP INDEX IF EXISTS idx_onebot_queue_reaction_cleanup")
        self._conn.execute(
            "ALTER TABLE onebot_queue_reaction RENAME TO onebot_queue_reaction_v8"
        )
        self._conn.execute(
            """
            CREATE TABLE onebot_queue_reaction (
                anchor_id TEXT NOT NULL,
                reaction_kind TEXT NOT NULL CHECK(reaction_kind IN ('queued','processing','legacy_processing')),
                lease_id TEXT,
                chat_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                emoji_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL CHECK(state IN ('pending','maybe_set')),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                last_error TEXT,
                PRIMARY KEY(anchor_id, reaction_kind)
            )
            """
        )
        self._conn.execute(
            """
            INSERT INTO onebot_queue_reaction(
                anchor_id,reaction_kind,lease_id,chat_id,message_id,state,
                created_at,updated_at,attempts,next_attempt_at,last_error
            )
            SELECT COALESCE(
                       (SELECT request_id FROM onebot_queue_trigger AS trigger
                        WHERE trigger.lease_id=old.lease_id LIMIT 1),
                       'legacy-reaction-' || old.lease_id
                   ),
                   'legacy_processing',old.lease_id,old.chat_id,old.message_id,old.state,
                   old.created_at,old.updated_at,old.attempts,old.next_attempt_at,old.last_error
            FROM onebot_queue_reaction_v8 AS old
            """
        )
        self._conn.execute("DROP TABLE onebot_queue_reaction_v8")

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
                anchor_id TEXT,
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
                uncertain_reason,attempts,anchor_id,created_at,updated_at
            )
            SELECT row_id,chat_id,message_key,chat_type,user_id,user_name,text,raw_text,
                COALESCE(message_id,''),metadata_json,seq,byte_size,state,lease_id,lease_until,
                lease_owner,uncertain_reason,attempts,NULL,created_at,updated_at
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
                anchor_seq INTEGER,
                anchor_kind TEXT NOT NULL DEFAULT 'message',
                batch_start_seq INTEGER,
                control_message_id TEXT,
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
            INSERT INTO onebot_queue_trigger(
                request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                status,lease_id,lease_owner,uncertain_reason,anchor_seq,anchor_kind,
                batch_start_seq,control_message_id,failure_count,next_attempt_at,
                failure_reason,created_at,updated_at
            )
            SELECT request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                status,lease_id,lease_owner,uncertain_reason,NULL,'message',
                NULL,NULL,0,NULL,NULL,created_at,updated_at
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
            "CREATE INDEX IF NOT EXISTS idx_onebot_queue_message_anchor ON onebot_queue_message(chat_id, anchor_id, seq)",
            "CREATE INDEX IF NOT EXISTS idx_onebot_queue_trigger_status ON onebot_queue_trigger(chat_id, status, anchor_seq)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_onebot_queue_anchor_seq ON onebot_queue_trigger(chat_id, anchor_seq) WHERE anchor_seq IS NOT NULL AND anchor_kind!='legacy'",
            "CREATE INDEX IF NOT EXISTS idx_onebot_queue_reaction_cleanup ON onebot_queue_reaction(state, next_attempt_at, updated_at)",
        )
        for statement in statements:
            self._conn.execute(statement)

    def _create_tables(self, *, commit: bool = True) -> None:
        """创建幂等表和索引，避免 executescript 隐式提交迁移事务。"""
        statements = (
            """
            CREATE TABLE IF NOT EXISTS onebot_queue_chat (
                chat_id TEXT PRIMARY KEY,
                next_seq INTEGER NOT NULL DEFAULT 1,
                summary TEXT NOT NULL DEFAULT '',
                revision INTEGER NOT NULL DEFAULT 0,
                last_trigger_at REAL,
                llm_judged_seq INTEGER NOT NULL DEFAULT 0,
                llm_next_attempt_at REAL,
                llm_failure_count INTEGER NOT NULL DEFAULT 0,
                llm_last_error TEXT,
                paused INTEGER NOT NULL DEFAULT 0,
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
                anchor_id TEXT,
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
                anchor_seq INTEGER,
                anchor_kind TEXT NOT NULL DEFAULT 'message',
                batch_start_seq INTEGER,
                control_message_id TEXT,
                failure_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                failure_reason TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(chat_id, message_key)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS onebot_queue_reaction (
                anchor_id TEXT NOT NULL,
                reaction_kind TEXT NOT NULL CHECK(reaction_kind IN ('queued','processing','legacy_processing')),
                lease_id TEXT,
                chat_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                emoji_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL CHECK(state IN ('pending','maybe_set')),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                last_error TEXT,
                PRIMARY KEY(anchor_id, reaction_kind)
            )
            """,
        )
        for statement in statements:
            self._conn.execute(statement)
        if commit:
            self._create_indexes()
            self._conn.commit()

    def _now(self) -> float:
        """返回可替换的当前时间，测试可通过 monkeypatch 控制。"""
        return time.time()

    @contextmanager
    def _operation(self) -> Iterator[None]:
        """登记一个同步 SQLite 操作，关闭时等待已进入的操作完成。"""
        self._lock.acquire()
        try:
            if self._closed:
                raise QueueError("OneBot11 QueueStore 已关闭")
            self._active_operations += 1
            yield
        finally:
            if self._active_operations > 0:
                self._active_operations -= 1
            if self._active_operations <= 0:
                self._active_operations = 0
                self._condition.notify_all()
            self._lock.release()

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
        *,
        triggered_at: float | None = None,
    ) -> EnqueueResult:
        """在同一事务中去重、分配序号、触发并更新 cooldown 时间。"""
        if message.chat_type not in {"group", "dm"}:
            raise QueueError(f"未知 chat_type: {message.chat_type!r}")
        key = self._message_key(message)
        with self._operation():
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
                        existing_trigger = self._conn.execute(
                            """
                            SELECT 1 FROM onebot_queue_trigger
                            WHERE chat_id=? AND message_key=? AND anchor_kind!='legacy'
                            LIMIT 1
                            """,
                            (message.chat_id, key),
                        ).fetchone()
                        request_id = self._ensure_trigger(
                            trigger_request, message.chat_id, key, now
                        )
                        if (
                            request_id is not None
                            and existing_trigger is None
                            and triggered_at is not None
                        ):
                            self._conn.execute(
                                "UPDATE onebot_queue_chat SET last_trigger_at=?, updated_at=? WHERE chat_id=?",
                                (float(triggered_at), now, message.chat_id),
                            )
                        self._conn.commit()
                        return EnqueueResult(False, True, key, int(existing[0]), request_id)
                    dedupe = self._conn.execute(
                        "SELECT seq FROM onebot_queue_dedupe WHERE chat_id=? AND message_key=?",
                        (message.chat_id, key),
                    ).fetchone()
                    if dedupe is not None:
                        self._conn.commit()
                        return EnqueueResult(
                            False,
                            True,
                            key,
                            int(dedupe[0]) if dedupe[0] is not None else None,
                            None,
                        )
                    text, raw_text, message_id, metadata_json, byte_size = self._normalize(message)
                    totals = self._conn.execute(
                        "SELECT COUNT(*), COALESCE(SUM(byte_size), 0) FROM onebot_queue_message"
                    ).fetchone()
                    if (
                        int(totals[0]) >= self.max_messages
                        or int(totals[1]) + byte_size > self.max_queue_bytes
                    ):
                        raise QueueFull("OneBot11 消息队列已满")
                    next_seq = int(
                        self._conn.execute(
                            "SELECT next_seq FROM onebot_queue_chat WHERE chat_id=?",
                            (message.chat_id,),
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
                    existing_trigger = self._conn.execute(
                        """
                        SELECT 1 FROM onebot_queue_trigger
                        WHERE chat_id=? AND message_key=? AND anchor_kind!='legacy'
                        LIMIT 1
                        """,
                        (message.chat_id, key),
                    ).fetchone()
                    request_id = self._ensure_trigger(
                        trigger_request, message.chat_id, key, now
                    )
                    if (
                        request_id is not None
                        and existing_trigger is None
                        and triggered_at is not None
                    ):
                        self._conn.execute(
                            "UPDATE onebot_queue_chat SET last_trigger_at=?, updated_at=? WHERE chat_id=?",
                            (float(triggered_at), now, message.chat_id),
                        )
                    self._conn.commit()
                    return EnqueueResult(True, False, key, next_seq, request_id)
                except Exception:
                    self._conn.rollback()
                    raise

    def _ensure_trigger(
        self,
        trigger: TurnAnchor | None,
        chat_id: str,
        message_key: str,
        now: float,
    ) -> str | None:
        """插入 message anchor；同一消息只保留一个、精确原因优先。"""
        if trigger is None:
            return None
        message = self._conn.execute(
            "SELECT seq,message_id,user_id,user_name FROM onebot_queue_message WHERE chat_id=? AND message_key=?",
            (chat_id, message_key),
        ).fetchone()
        if message is None:
            return None
        anchor_kind = str(trigger.anchor_kind or "message")
        if anchor_kind != "message":
            raise QueueError("enqueue 只能随消息原子创建 message anchor")
        anchor_seq = int(message["seq"])
        if trigger.anchor_seq is not None and int(trigger.anchor_seq) != anchor_seq:
            raise QueueError("message anchor_seq 必须等于锚点消息的真实 seq")
        self._conn.execute(
            """
            INSERT OR IGNORE INTO onebot_queue_trigger(
                request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                status,anchor_seq,anchor_kind,control_message_id,created_at,updated_at
            ) VALUES (?,?,?,?,?,?, 'pending', ?, ?, ?, ?, ?)
            """,
            (
                trigger.request_id,
                chat_id,
                message_key,
                trigger.reason,
                str(message["user_id"]) if anchor_kind == "message" else trigger.caller_user_id,
                str(message["user_name"]) if anchor_kind == "message" else trigger.caller_user_name,
                anchor_seq,
                anchor_kind,
                trigger.control_message_id,
                trigger.created_at,
                now,
            ),
        )
        row = self._conn.execute(
            "SELECT request_id,reason FROM onebot_queue_trigger WHERE chat_id=? AND anchor_seq=? AND anchor_kind!='legacy'",
            (chat_id, anchor_seq),
        ).fetchone()
        if row is not None:
            request_id = str(row["request_id"])
            if str(row["reason"]) in {"llm", "automatic"} and trigger.reason not in {
                "llm",
                "automatic",
            }:
                self._conn.execute(
                    "UPDATE onebot_queue_trigger SET reason=?,updated_at=? WHERE request_id=?",
                    (trigger.reason, now, request_id),
                )
            self._conn.execute(
                "UPDATE onebot_queue_message SET anchor_id=?,updated_at=? WHERE chat_id=? AND message_key=?",
                (request_id, now, chat_id, message_key),
            )
            return request_id
        return None

    def _purge_dedupe(self, now: float) -> None:
        """清理过期的持久去重记录，避免 tombstone 无限增长。"""
        cutoff = now - self.dedupe_ttl_seconds
        self._conn.execute("DELETE FROM onebot_queue_dedupe WHERE created_at<?", (cutoff,))

    def peek(self, chat_id: str) -> tuple[QueueMessage, ...]:
        """只读查看当前群 pending 消息，不创建 lease。"""
        with self._operation():
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

    def peek_unanchored(self, chat_id: str) -> tuple[QueueMessage, ...]:
        """读取最后一个已知 anchor 边界之后的未归属消息，供自动选择器使用。"""
        with self._operation():
            rows = self._conn.execute(
                """
                SELECT * FROM onebot_queue_message
                WHERE chat_id=? AND state='pending' AND anchor_id IS NULL
                  AND (next_attempt_at IS NULL OR next_attempt_at<=?)
                  AND seq>COALESCE((
                      SELECT MAX(anchor_seq) FROM onebot_queue_trigger
                      WHERE chat_id=? AND anchor_seq IS NOT NULL
                  ), 0)
                ORDER BY seq
                """,
                (str(chat_id), self._now(), str(chat_id)),
            ).fetchall()
            return tuple(self._row_to_message(row) for row in rows)

    def message_at(self, chat_id: str, seq: int) -> QueueMessage | None:
        """按稳定 seq 读取一条仍在队列中的消息。"""
        with self._operation():
            row = self._conn.execute(
                "SELECT * FROM onebot_queue_message WHERE chat_id=? AND seq=?",
                (str(chat_id), int(seq)),
            ).fetchone()
            return self._row_to_message(row) if row is not None else None

    def chat_type(self, chat_id: str) -> str | None:
        """读取队列中该目标的类型；未知目标返回 None。"""
        with self._operation():
            row = self._conn.execute(
                "SELECT chat_type FROM onebot_queue_message WHERE chat_id=? LIMIT 1",
                (str(chat_id),),
            ).fetchone()
            return str(row[0]) if row else None

    def last_trigger_at(self, chat_id: str) -> float | None:
        """读取持久化的最近一次真实触发时间。"""
        with self._operation():
            row = self._conn.execute(
                "SELECT last_trigger_at FROM onebot_queue_chat WHERE chat_id=?",
                (str(chat_id),),
            ).fetchone()
            return float(row[0]) if row and row[0] is not None else None

    def llm_judgment(self, chat_id: str) -> dict[str, Any]:
        """读取群级 LLM trigger 判断游标和退避状态。"""
        with self._operation():
            row = self._conn.execute(
                """
                SELECT llm_judged_seq, llm_next_attempt_at, llm_failure_count, llm_last_error
                FROM onebot_queue_chat WHERE chat_id=?
                """,
                (str(chat_id),),
            ).fetchone()
            return {
                "judged_seq": int(row["llm_judged_seq"]) if row else 0,
                "next_attempt_at": (
                    float(row["llm_next_attempt_at"])
                    if row and row["llm_next_attempt_at"] is not None
                    else None
                ),
                "failure_count": int(row["llm_failure_count"]) if row else 0,
                "last_error": str(row["llm_last_error"]) if row and row["llm_last_error"] else None,
            }

    def mark_llm_judged(self, chat_id: str, observed_seq: int) -> None:
        """确认一批消息已完成判断，清除该群的 LLM 退避状态。"""
        with self._operation():
            now = self._now()
            self._conn.execute(
                """
                INSERT INTO onebot_queue_chat(chat_id, llm_judged_seq, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    llm_judged_seq=MAX(onebot_queue_chat.llm_judged_seq, excluded.llm_judged_seq),
                    llm_next_attempt_at=NULL,
                    llm_failure_count=0,
                    llm_last_error=NULL,
                    updated_at=excluded.updated_at
                """,
                (str(chat_id), max(0, int(observed_seq)), now),
            )
            self._conn.commit()

    def mark_llm_failure(
        self,
        chat_id: str,
        observed_seq: int,
        reason: str,
        *,
        backoff_seconds: tuple[float, ...] = DEFAULT_BACKOFF_SECONDS,
    ) -> None:
        """记录旁路模型失败，按 2/4/8 秒退避且允许新消息提前唤醒。"""
        with self._operation():
            now = self._now()
            row = self._conn.execute(
                "SELECT llm_failure_count FROM onebot_queue_chat WHERE chat_id=?",
                (str(chat_id),),
            ).fetchone()
            failure_count = (int(row[0]) if row else 0) + 1
            delays = tuple(max(0.0, float(item)) for item in backoff_seconds)
            delays = delays or DEFAULT_BACKOFF_SECONDS
            delay = min(MAX_BACKOFF_SECONDS, delays[min(failure_count - 1, len(delays) - 1)])
            self._conn.execute(
                """
                INSERT INTO onebot_queue_chat(
                    chat_id, llm_judged_seq, llm_next_attempt_at,
                    llm_failure_count, llm_last_error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    llm_judged_seq=MAX(onebot_queue_chat.llm_judged_seq, excluded.llm_judged_seq),
                    llm_next_attempt_at=excluded.llm_next_attempt_at,
                    llm_failure_count=excluded.llm_failure_count,
                    llm_last_error=excluded.llm_last_error,
                    updated_at=excluded.updated_at
                """,
                (
                    str(chat_id),
                    max(0, int(observed_seq)),
                    now + delay,
                    failure_count,
                    str(reason)[:512],
                    now,
                ),
            )
            self._conn.commit()

    def create_message_anchor(
        self,
        chat_id: str,
        anchor_seq: int,
        reason: str,
        *,
        triggered_at: float | None = None,
    ) -> str | None:
        """按真实消息 seq 创建 durable message anchor；authority 取自该消息。"""
        with self._operation():
            try:
                self._transaction()
                now = self._now()
                row = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_message
                    WHERE chat_id=? AND seq=? AND state='pending'
                    """,
                    (str(chat_id), int(anchor_seq)),
                ).fetchone()
                if row is None:
                    self._conn.commit()
                    return None
                if row["anchor_id"] is not None:
                    existing = self._conn.execute(
                        "SELECT request_id FROM onebot_queue_trigger WHERE request_id=?",
                        (str(row["anchor_id"]),),
                    ).fetchone()
                    self._conn.commit()
                    return str(existing[0]) if existing is not None else None
                existing_trigger = self._conn.execute(
                    """
                    SELECT 1 FROM onebot_queue_trigger
                    WHERE chat_id=? AND anchor_seq=? AND anchor_kind!='legacy'
                    LIMIT 1
                    """,
                    (str(chat_id), int(row["seq"])),
                ).fetchone()
                self._conn.execute(
                    "UPDATE onebot_queue_message SET next_attempt_at=NULL, updated_at=? WHERE row_id=?",
                    (now, int(row["row_id"])),
                )
                anchor = TurnAnchor.create(
                    str(chat_id),
                    str(row["message_key"]),
                    str(reason),
                    str(row["user_id"]),
                    str(row["user_name"]),
                    anchor_seq=int(row["seq"]),
                )
                request_id = self._ensure_trigger(
                    anchor, str(chat_id), str(row["message_key"]), now
                )
                if (
                    request_id is not None
                    and existing_trigger is None
                    and triggered_at is not None
                ):
                    self._conn.execute(
                        "UPDATE onebot_queue_chat SET last_trigger_at=?, updated_at=? WHERE chat_id=?",
                        (float(triggered_at), now, str(chat_id)),
                    )
                self._conn.commit()
                return request_id
            except Exception:
                self._conn.rollback()
                raise

    def pending_chat_ids(self) -> tuple[str, ...]:
        """读取有待处理消息的群号，供启动恢复和旁路 trigger 使用。"""
        with self._operation():
            rows = self._conn.execute(
                "SELECT DISTINCT chat_id FROM onebot_queue_message WHERE state='pending' ORDER BY chat_id"
            ).fetchall()
            return tuple(str(row[0]) for row in rows)

    def recoverable_chat_ids(self) -> tuple[str, ...]:
        """读取仍可能需要恢复的群号，包含 leased/uncertain/failed 状态。"""
        with self._operation():
            rows = self._conn.execute(
                """
                SELECT DISTINCT chat_id
                FROM onebot_queue_message
                WHERE state IN ('pending','leased','uncertain','failed')
                ORDER BY chat_id
                """
            ).fetchall()
            return tuple(str(row[0]) for row in rows)

    def _chat_scope(
        self,
        column: str,
        allowed_chat_ids: tuple[str, ...] | set[str] | frozenset[str] | None,
    ) -> tuple[str, tuple[str, ...]]:
        """生成受限恢复查询的安全 IN 子句；列名只由内部调用方提供。"""
        if allowed_chat_ids is None:
            return "", ()
        values = tuple(sorted({str(item) for item in allowed_chat_ids if str(item).strip()}))
        if not values:
            return " AND 0=1", ()
        placeholders = ",".join("?" for _ in values)
        return f" AND {column} IN ({placeholders})", values

    def claim(self, chat_id: str, lease_seconds: float = 60.0) -> QueueLease | None:
        """按 anchor_seq 认领最早 TurnAnchor 的固定消息范围。"""
        with self._operation():
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
                self._recover_expired_anchors(chat_id, now)
                self._conn.execute(
                    """
                    DELETE FROM onebot_queue_trigger
                    WHERE chat_id=? AND status='pending' AND NOT EXISTS (
                        SELECT 1 FROM onebot_queue_message AS message
                        WHERE message.chat_id=onebot_queue_trigger.chat_id
                          AND (message.anchor_id=onebot_queue_trigger.request_id
                               OR (message.message_key=onebot_queue_trigger.message_key
                                   AND onebot_queue_trigger.anchor_kind='message'))
                    )
                      AND anchor_kind NOT IN ('operator','legacy','service')
                    """,
                    (chat_id,),
                )
                trigger_row = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_trigger
                    WHERE chat_id=?
                    ORDER BY COALESCE(anchor_seq, 9223372036854775807), created_at, request_id
                    LIMIT 1
                    """,
                    (chat_id,),
                ).fetchone()
                if trigger_row is None:
                    self._conn.commit()
                    return None
                if (
                    str(trigger_row["status"]) != "pending"
                    or str(trigger_row["anchor_kind"]) == "legacy"
                    or trigger_row["anchor_seq"] is None
                    or (
                        trigger_row["next_attempt_at"] is not None
                        and float(trigger_row["next_attempt_at"]) > now
                    )
                ):
                    self._conn.commit()
                    return None
                anchor_id = str(trigger_row["request_id"])
                anchor_seq = int(trigger_row["anchor_seq"])
                if trigger_row["batch_start_seq"] is None:
                    start_row = self._conn.execute(
                        """
                        SELECT MIN(seq) FROM onebot_queue_message
                        WHERE chat_id=? AND state='pending'
                          AND (anchor_id IS NULL OR anchor_id=?) AND seq<=?
                        """,
                        (chat_id, anchor_id, anchor_seq),
                    ).fetchone()
                    self._conn.execute(
                        """
                        UPDATE onebot_queue_message SET anchor_id=?,updated_at=?
                        WHERE chat_id=? AND state='pending' AND anchor_id IS NULL AND seq<=?
                        """,
                        (anchor_id, now, chat_id, anchor_seq),
                    )
                    batch_start = (
                        int(start_row[0])
                        if start_row is not None and start_row[0] is not None
                        else anchor_seq
                    )
                    self._conn.execute(
                        "UPDATE onebot_queue_trigger SET batch_start_seq=? WHERE request_id=?",
                        (batch_start, anchor_id),
                    )
                message_rows = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_message
                    WHERE chat_id=? AND anchor_id=? AND state='pending'
                    ORDER BY seq
                    """,
                    (chat_id, anchor_id),
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
                    WHERE request_id=? AND status='pending'
                    """,
                    (lease_id, self._owner_id, now, anchor_id),
                )
                trigger_row = self._conn.execute(
                    "SELECT * FROM onebot_queue_trigger WHERE request_id=?",
                    (anchor_id,),
                ).fetchone()
                batch_summary = self._build_batch_summary(message_rows)
                self._conn.commit()
                return QueueLease(
                    chat_id=chat_id,
                    lease_id=lease_id,
                    messages=tuple(self._row_to_message(row) for row in message_rows),
                    trigger=self._row_to_trigger(trigger_row),
                    # summary 只属于当前 lease 的早期消息；不读取 chat.summary，
                    # 避免上一轮已经写入 Hermes session 的内容再次进入 prompt。
                    summary=batch_summary,
                    claimed_at=now,
                    lease_until=until,
                    phase="agent_running",
                    outbound_started=False,
                    attempts=max(int(row["attempts"]) for row in message_rows) + 1,
                    failure_count=max(int(row["failure_count"]) for row in message_rows),
                )
            except Exception:
                self._conn.rollback()
                raise

    def create_operator_anchor(
        self,
        chat_id: str,
        reason: str,
        caller_user_id: str,
        caller_user_name: str,
        *,
        control_message_id: str,
        triggered_at: float | None = None,
    ) -> str | None:
        """把命令到达时最晚的未锚定消息固定为 operator anchor 边界。"""
        with self._operation():
            try:
                self._transaction()
                now = self._now()
                row = self._conn.execute(
                    """
                    SELECT MAX(seq) AS anchor_seq
                    FROM onebot_queue_message
                    WHERE chat_id=? AND state='pending' AND anchor_id IS NULL
                      AND seq>COALESCE((
                          SELECT MAX(anchor_seq) FROM onebot_queue_trigger
                          WHERE chat_id=? AND anchor_seq IS NOT NULL
                      ), 0)
                    """,
                    (str(chat_id), str(chat_id)),
                ).fetchone()
                if row is None or row["anchor_seq"] is None:
                    self._conn.commit()
                    return None
                anchor_seq = int(row["anchor_seq"])
                anchor = TurnAnchor.create(
                    str(chat_id),
                    f"operator:{uuid.uuid4().hex}",
                    str(reason),
                    str(caller_user_id),
                    str(caller_user_name),
                    anchor_kind="operator",
                    anchor_seq=anchor_seq,
                    control_message_id=str(control_message_id),
                )
                self._conn.execute(
                    """
                    INSERT INTO onebot_queue_trigger(
                        request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
                        status,anchor_seq,anchor_kind,control_message_id,created_at,updated_at
                    ) VALUES (?,?,?,?,?,?, 'pending', ?, 'operator', ?, ?, ?)
                    """,
                    (
                        anchor.request_id,
                        anchor.chat_id,
                        anchor.message_key,
                        anchor.reason,
                        anchor.caller_user_id,
                        anchor.caller_user_name,
                        anchor_seq,
                        anchor.control_message_id,
                        anchor.created_at,
                        now,
                    ),
                )
                if triggered_at is not None:
                    self._conn.execute(
                        "UPDATE onebot_queue_chat SET last_trigger_at=?,updated_at=? WHERE chat_id=?",
                        (float(triggered_at), now, str(chat_id)),
                    )
                self._conn.commit()
                return anchor.request_id
            except Exception:
                self._conn.rollback()
                raise

    def create_trigger(
        self,
        chat_id: str,
        reason: str,
        caller_user_id: str,
        caller_user_name: str,
        *,
        triggered_at: float | None = None,
    ) -> str | None:
        """兼容 v0.3 API：只为最早未锚定消息创建 message anchor。"""
        del caller_user_id, caller_user_name
        messages = self.peek_unanchored(str(chat_id))
        if not messages or messages[0].seq is None:
            return None
        return self.create_message_anchor(
            str(chat_id),
            int(messages[0].seq),
            str(reason),
            triggered_at=triggered_at,
        )

    def list_anchors(self, chat_id: str) -> tuple[TurnAnchor, ...]:
        """按执行顺序列出当前群全部 durable anchor。"""
        with self._operation():
            rows = self._conn.execute(
                """
                SELECT * FROM onebot_queue_trigger WHERE chat_id=?
                ORDER BY COALESCE(anchor_seq, 9223372036854775807),created_at,request_id
                """,
                (str(chat_id),),
            ).fetchall()
            return tuple(self._row_to_trigger(row) for row in rows)

    def _recover_expired_anchors(self, chat_id: str, now: float) -> None:
        """在当前事务中恢复当前群过期 anchor，保留固定 batch 归属。"""
        rows = self._conn.execute(
            """
            SELECT * FROM onebot_queue_trigger
            WHERE chat_id=? AND status='claimed'
            ORDER BY COALESCE(anchor_seq, 9223372036854775807)
            """,
            (str(chat_id),),
        ).fetchall()
        for anchor in rows:
            lease_id = str(anchor["lease_id"] or "")
            message = self._conn.execute(
                """
                SELECT MIN(lease_until) AS lease_until,
                       MAX(outbound_started) AS outbound_started,
                       MAX(
                           CASE
                               WHEN lease_phase='outbound_started'
                                    OR lease_phase != 'agent_running'
                               THEN 1 ELSE 0
                           END
                       ) AS phase_unknown
                FROM onebot_queue_message
                WHERE anchor_id=? AND lease_id=? AND state='leased'
                """,
                (str(anchor["request_id"]), lease_id),
            ).fetchone()
            if message is None or message["lease_until"] is None:
                uncertain = True
            elif float(message["lease_until"]) > now:
                continue
            else:
                uncertain = bool(message["outbound_started"] or message["phase_unknown"])
            if uncertain:
                reason = "lease 过期时出站阶段未知，需管理员确认"
                self._conn.execute(
                    """
                    UPDATE onebot_queue_message
                    SET state='uncertain',lease_id=NULL,lease_until=NULL,lease_owner=NULL,
                        lease_phase='uncertain',uncertain_reason=?,updated_at=?
                    WHERE anchor_id=? AND state='leased'
                    """,
                    (reason, now, str(anchor["request_id"])),
                )
                self._conn.execute(
                    """
                    UPDATE onebot_queue_trigger
                    SET status='uncertain',lease_id=NULL,lease_owner=NULL,
                        uncertain_reason=?,failure_reason=?,updated_at=?
                    WHERE request_id=?
                    """,
                    (reason, reason, now, str(anchor["request_id"])),
                )
            else:
                failure_count = int(anchor["failure_count"] or 0) + 1
                if failure_count >= self.max_attempts:
                    state = "failed"
                    next_attempt_at = None
                    failure_reason = "turn lease 过期，已达到自动恢复上限"
                else:
                    state = "pending"
                    delay = min(
                        MAX_BACKOFF_SECONDS,
                        self.backoff_seconds[
                            min(failure_count - 1, len(self.backoff_seconds) - 1)
                        ],
                    )
                    next_attempt_at = now + delay
                    failure_reason = "turn lease 过期，等待退避后自动恢复"
                self._conn.execute(
                    """
                    UPDATE onebot_queue_message
                    SET state=?,lease_id=NULL,lease_until=NULL,lease_owner=NULL,
                        lease_phase=?,uncertain_reason=NULL,failure_count=?,
                        next_attempt_at=?,failure_reason=?,updated_at=?
                    WHERE anchor_id=? AND state='leased'
                    """,
                    (
                        state,
                        state,
                        failure_count,
                        next_attempt_at,
                        failure_reason,
                        now,
                        str(anchor["request_id"]),
                    ),
                )
                self._conn.execute(
                    """
                    UPDATE onebot_queue_trigger
                    SET status=?,lease_id=NULL,lease_owner=NULL,
                        uncertain_reason=NULL,failure_count=?,
                        next_attempt_at=?,failure_reason=?,updated_at=?
                    WHERE request_id=?
                    """,
                    (
                        state,
                        failure_count,
                        next_attempt_at,
                        failure_reason,
                        now,
                        str(anchor["request_id"]),
                    ),
                )

    def renew(self, lease: QueueLease | str, lease_seconds: float = 60.0) -> bool:
        """延长活动 lease；过期 lease 不会被复活。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._operation():
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
        with self._operation():
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
        with self._operation():
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
        with self._operation():
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
        """确认 lease 并删除消息；已成功物化的 batch 不再写入跨轮摘要。"""
        lease_id = lease.lease_id if isinstance(lease, QueueLease) else str(lease)
        with self._operation():
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
                if any(
                    str(row["lease_phase"] or "") not in {"agent_running", "outbound_started"}
                    for row in rows
                ):
                    reason = "lease phase 无法证明，成功结果也不能安全 ack"
                    self._conn.execute(
                        """
                        UPDATE onebot_queue_message
                        SET state='uncertain', lease_id=NULL, lease_until=NULL,
                            lease_owner=NULL, lease_phase='uncertain', outbound_started=1,
                            uncertain_reason=?, updated_at=?
                        WHERE lease_id=? AND lease_owner=? AND state='leased'
                        """,
                        (reason, now, lease_id, self._owner_id),
                    )
                    self._conn.execute(
                        """
                        UPDATE onebot_queue_trigger
                        SET status='uncertain', lease_id=NULL, lease_owner=NULL,
                            uncertain_reason=?, failure_reason=?, updated_at=?
                        WHERE lease_id=? AND lease_owner=? AND status='claimed'
                        """,
                        (reason, reason, now, lease_id, self._owner_id),
                    )
                    self._conn.commit()
                    return False
                chat_id = str(rows[0]["chat_id"])
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
                self._conn.execute(
                    "UPDATE onebot_queue_chat SET updated_at=? WHERE chat_id=?",
                    (now, chat_id),
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
        with self._operation():
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
                if message_state == "pending" and any(
                    bool(row["outbound_started"])
                    or str(row["lease_phase"] or "") == "outbound_started"
                    or str(row["lease_phase"] or "") != "agent_running"
                    for row in rows
                ):
                    # marker 一旦落盘，release 也必须立即转 uncertain；
                    # allow_after_outbound 仅保留接口兼容，不能绕过 fencing。
                    message_state = "uncertain"
                    trigger_state = "uncertain"
                    reason = reason or "出站已开始，明确失败也需要管理员确认"
                final_message_state = message_state
                final_trigger_state = trigger_state
                next_attempt_at: float | None = None
                anchor_row = self._conn.execute(
                    "SELECT * FROM onebot_queue_trigger WHERE lease_id=? AND lease_owner=?",
                    (lease_id, self._owner_id),
                ).fetchone()
                if anchor_row is None:
                    self._conn.commit()
                    return False
                failure_count = int(anchor_row["failure_count"] or 0)
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
                        failure_reason if final_message_state == "uncertain" else None,
                        failure_count,
                        next_attempt_at,
                        failure_reason if final_message_state in {"pending", "failed"} else None,
                        now,
                        lease_id,
                        self._owner_id,
                    ),
                )
                self._conn.execute(
                    """
                    UPDATE onebot_queue_trigger SET status=?, lease_id=NULL, lease_owner=NULL,
                        uncertain_reason=?, failure_count=?, next_attempt_at=?,
                        failure_reason=?, updated_at=?
                    WHERE lease_id=? AND lease_owner=? AND status='claimed'
                    """,
                    (
                        final_trigger_state,
                        failure_reason if final_trigger_state in {"uncertain", "failed"} else None,
                        failure_count,
                        next_attempt_at,
                        failure_reason,
                        now,
                        lease_id,
                        self._owner_id,
                    ),
                )
                self._conn.commit()
                return cursor.rowcount > 0
            except Exception:
                self._conn.rollback()
                raise

    def resolve_uncertain(self, chat_id: str, action: str) -> int:
        """管理员只处理当前群最早 blocking anchor。"""
        if action not in {"retry", "discard"}:
            raise ValueError("队列只能 resolve retry 或 discard")
        with self._operation():
            try:
                self._transaction()
                now = self._now()
                anchor = self._conn.execute(
                    """
                    SELECT * FROM onebot_queue_trigger
                    WHERE chat_id=? AND status IN ('uncertain','failed')
                    ORDER BY COALESCE(anchor_seq, 9223372036854775807), created_at
                    LIMIT 1
                    """,
                    (str(chat_id),),
                ).fetchone()
                if anchor is None:
                    orphan_rows = self._conn.execute(
                        """
                        SELECT MIN(seq) AS first_seq,MAX(seq) AS last_seq,COUNT(*) AS count
                        FROM onebot_queue_message
                        WHERE chat_id=? AND state IN ('uncertain','failed')
                          AND (anchor_id IS NULL OR NOT EXISTS (
                              SELECT 1 FROM onebot_queue_trigger AS trigger
                              WHERE trigger.request_id=onebot_queue_message.anchor_id
                          ))
                        """,
                        (str(chat_id),),
                    ).fetchone()
                    if orphan_rows is None or int(orphan_rows["count"] or 0) == 0:
                        self._conn.commit()
                        return 0
                    anchor_id = f"legacy-{uuid.uuid4().hex}"
                    first_seq = int(orphan_rows["first_seq"])
                    last_seq = int(orphan_rows["last_seq"])
                    self._conn.execute(
                        """
                        INSERT INTO onebot_queue_trigger(
                            request_id,chat_id,message_key,reason,caller_user_id,
                            caller_user_name,status,anchor_seq,anchor_kind,batch_start_seq,
                            uncertain_reason,failure_reason,created_at,updated_at
                        ) VALUES (?,?,?,?,?,?, 'uncertain', ?, 'legacy', ?, ?, ?, ?, ?)
                        """,
                        (
                            anchor_id,
                            str(chat_id),
                            f"legacy:{chat_id}:{last_seq}",
                            "orphan_hold",
                            "",
                            "legacy",
                            last_seq,
                            first_seq,
                            "消息缺少可验证 TurnAnchor，禁止猜测 authority",
                            "orphan anchor hold",
                            now,
                            now,
                        ),
                    )
                    self._conn.execute(
                        """
                        UPDATE onebot_queue_message SET anchor_id=?,state='uncertain',
                            lease_id=NULL,lease_until=NULL,lease_owner=NULL,
                            lease_phase='uncertain',outbound_started=1,
                            uncertain_reason='消息缺少可验证 TurnAnchor，禁止猜测 authority',
                            updated_at=?
                        WHERE chat_id=? AND state IN ('uncertain','failed')
                          AND (anchor_id IS NULL OR NOT EXISTS (
                              SELECT 1 FROM onebot_queue_trigger AS trigger
                              WHERE trigger.request_id=onebot_queue_message.anchor_id
                          ))
                        """,
                        (anchor_id, now, str(chat_id)),
                    )
                    anchor = self._conn.execute(
                        "SELECT * FROM onebot_queue_trigger WHERE request_id=?",
                        (anchor_id,),
                    ).fetchone()
                anchor_id = str(anchor["request_id"])
                rows = self._conn.execute(
                    "SELECT row_id FROM onebot_queue_message WHERE anchor_id=?",
                    (anchor_id,),
                ).fetchall()
                if action == "retry":
                    if str(anchor["anchor_kind"]) == "legacy":
                        # legacy 没有可验证 authority；retry 不能把它重新交给
                        # 任意后续用户，只能由管理员 discard 或发送新的明确消息。
                        self._conn.commit()
                        return 0
                    else:
                        new_anchor_id = uuid.uuid4().hex
                        self._conn.execute(
                            "DELETE FROM onebot_queue_trigger WHERE request_id=?",
                            (anchor_id,),
                        )
                        self._conn.execute(
                            """
                            INSERT INTO onebot_queue_trigger(
                                request_id,chat_id,message_key,reason,caller_user_id,
                                caller_user_name,status,anchor_seq,anchor_kind,batch_start_seq,
                                control_message_id,failure_count,next_attempt_at,failure_reason,
                                created_at,updated_at
                            ) VALUES (?,?,?,?,?,?, 'pending', ?, ?, ?, ?, 0, NULL, NULL, ?, ?)
                            """,
                            (
                                new_anchor_id,
                                str(anchor["chat_id"]),
                                str(anchor["message_key"]),
                                f"{str(anchor['reason'])}:admin_retry",
                                str(anchor["caller_user_id"]),
                                str(anchor["caller_user_name"]),
                                anchor["anchor_seq"],
                                str(anchor["anchor_kind"]),
                                anchor["batch_start_seq"],
                                anchor["control_message_id"],
                                now,
                                now,
                            ),
                        )
                        self._conn.execute(
                            """
                            UPDATE onebot_queue_message
                            SET state='pending',anchor_id=?,lease_id=NULL,lease_until=NULL,
                                lease_owner=NULL,lease_phase='pending',outbound_started=0,
                                uncertain_reason=NULL,failure_reason=NULL,failure_count=0,
                                next_attempt_at=NULL,updated_at=?
                            WHERE anchor_id=?
                            """,
                            (new_anchor_id, now, anchor_id),
                        )
                        self._conn.execute(
                            """
                            UPDATE onebot_queue_reaction
                            SET anchor_id=?,lease_id=NULL,updated_at=?
                            WHERE anchor_id=?
                            """,
                            (new_anchor_id, now, anchor_id),
                        )
                else:
                    self._conn.execute(
                        "DELETE FROM onebot_queue_reaction WHERE anchor_id=?",
                        (anchor_id,),
                    )
                    self._conn.execute(
                        """
                        INSERT OR REPLACE INTO onebot_queue_dedupe(
                            chat_id, message_key, seq, created_at
                        )
                        SELECT chat_id, message_key, seq, ?
                        FROM onebot_queue_message
                        WHERE anchor_id=?
                        """,
                        (now, anchor_id),
                    )
                    self._conn.execute(
                        "DELETE FROM onebot_queue_message WHERE anchor_id=?", (anchor_id,)
                    )
                    self._conn.execute(
                        "DELETE FROM onebot_queue_trigger WHERE request_id=?", (anchor_id,)
                    )
                self._conn.commit()
                return len(rows)
            except Exception:
                self._conn.rollback()
                raise

    def record_reaction(
        self,
        lease_id: str,
        chat_id: str,
        message_id: str,
        *,
        state: str = "pending",
        anchor_id: str | None = None,
        reaction_kind: str = "processing",
        emoji_id: str = "",
    ) -> None:
        """在 set 前按 anchor/阶段持久化目标；旧 lease 调用自动映射。"""
        if state not in {"pending", "maybe_set"}:
            raise ValueError("reaction state 必须是 pending 或 maybe_set")
        if reaction_kind not in {"queued", "processing", "legacy_processing"}:
            raise ValueError("未知 reaction_kind")
        with self._operation():
            now = self._now()
            resolved_anchor = str(anchor_id or "")
            if not resolved_anchor:
                row = self._conn.execute(
                    "SELECT request_id FROM onebot_queue_trigger WHERE lease_id=? LIMIT 1",
                    (str(lease_id),),
                ).fetchone()
                resolved_anchor = str(row[0]) if row is not None else f"legacy-{lease_id}"
            same_target = self._conn.execute(
                """
                SELECT anchor_id FROM onebot_queue_reaction
                WHERE chat_id=? AND message_id=? AND reaction_kind=?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (str(chat_id), str(message_id), reaction_kind),
            ).fetchone()
            if same_target is not None and str(same_target["anchor_id"]) != resolved_anchor:
                current_target = self._conn.execute(
                    """
                    SELECT 1 FROM onebot_queue_reaction
                    WHERE anchor_id=? AND reaction_kind=?
                    """,
                    (resolved_anchor, reaction_kind),
                ).fetchone()
                if current_target is None:
                    self._conn.execute(
                        """
                        UPDATE onebot_queue_reaction
                        SET anchor_id=?,lease_id=?,chat_id=?,message_id=?,emoji_id=?,
                            updated_at=?
                        WHERE anchor_id=? AND reaction_kind=?
                        """,
                        (
                            resolved_anchor,
                            str(lease_id) or None,
                            str(chat_id),
                            str(message_id),
                            str(emoji_id),
                            now,
                            str(same_target["anchor_id"]),
                            reaction_kind,
                        ),
                    )
                else:
                    self._conn.execute(
                        """
                        DELETE FROM onebot_queue_reaction
                        WHERE anchor_id=? AND reaction_kind=?
                        """,
                        (str(same_target["anchor_id"]), reaction_kind),
                    )
            self._conn.execute(
                """
                INSERT INTO onebot_queue_reaction(
                    anchor_id,reaction_kind,lease_id,chat_id,message_id,emoji_id,
                    state,created_at,updated_at,attempts,next_attempt_at,last_error
                ) VALUES (?,?,?,?,?,?,?,?,?,0,NULL,NULL)
                ON CONFLICT(anchor_id,reaction_kind) DO UPDATE SET
                    lease_id=excluded.lease_id,
                    chat_id=excluded.chat_id,
                    message_id=excluded.message_id,
                    emoji_id=excluded.emoji_id,
                    state=CASE
                        WHEN onebot_queue_reaction.state='maybe_set'
                        THEN 'maybe_set'
                        ELSE excluded.state
                    END,
                    updated_at=excluded.updated_at,
                    attempts=CASE
                        WHEN onebot_queue_reaction.state='maybe_set'
                        THEN onebot_queue_reaction.attempts
                        ELSE 0
                    END,
                    next_attempt_at=CASE
                        WHEN onebot_queue_reaction.state='maybe_set'
                        THEN onebot_queue_reaction.next_attempt_at
                        ELSE NULL
                    END,
                    last_error=CASE
                        WHEN onebot_queue_reaction.state='maybe_set'
                        THEN onebot_queue_reaction.last_error
                        ELSE NULL
                    END
                """,
                (
                    resolved_anchor,
                    reaction_kind,
                    str(lease_id) or None,
                    str(chat_id),
                    str(message_id),
                    str(emoji_id),
                    state,
                    now,
                    now,
                ),
            )
            self._conn.commit()

    def wake_llm_for_new_message(self, chat_id: str, seq: int) -> None:
        """新消息只清除 selector 退避，不回退已完成判断的游标。"""
        with self._operation():
            row = self._conn.execute(
                "SELECT llm_judged_seq FROM onebot_queue_chat WHERE chat_id=?",
                (str(chat_id),),
            ).fetchone()
            if row is None or int(seq) <= int(row[0] or 0):
                return
            self._conn.execute(
                """
                UPDATE onebot_queue_chat
                SET llm_next_attempt_at=NULL,llm_failure_count=0,llm_last_error=NULL,
                    updated_at=?
                WHERE chat_id=?
                """,
                (self._now(), str(chat_id)),
            )
            self._conn.commit()

    def mark_reaction_set(
        self,
        identifier: str,
        *,
        reaction_kind: str = "processing",
    ) -> bool:
        """标记 reaction 可能已成功添加，后续只允许恢复 unset。"""
        with self._operation():
            cursor = self._conn.execute(
                """
                UPDATE onebot_queue_reaction
                SET state='maybe_set', updated_at=?, attempts=0,
                    next_attempt_at=NULL, last_error=NULL
                WHERE reaction_kind=? AND (anchor_id=? OR lease_id=?)
                """,
                (self._now(), reaction_kind, str(identifier), str(identifier)),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def mark_reaction_cleanup_failed(
        self,
        identifier: str,
        reason: str,
        *,
        reaction_kind: str | None = None,
        max_attempts: int = 3,
        backoff_seconds: tuple[float, ...] = DEFAULT_BACKOFF_SECONDS,
    ) -> bool:
        """记录 unset 失败并按有限退避安排后续清理。"""
        with self._operation():
            try:
                self._transaction()
                row = self._conn.execute(
                    """
                    SELECT anchor_id,reaction_kind,attempts FROM onebot_queue_reaction
                    WHERE (anchor_id=? OR lease_id=?)
                      AND (? IS NULL OR reaction_kind=?)
                      AND state IN ('pending','maybe_set')
                    ORDER BY CASE reaction_kind WHEN 'processing' THEN 0 ELSE 1 END
                    LIMIT 1
                    """,
                    (
                        str(identifier),
                        str(identifier),
                        reaction_kind,
                        reaction_kind,
                    ),
                ).fetchone()
                if row is None:
                    self._conn.commit()
                    return False
                anchor_id = str(row["anchor_id"])
                selected_kind = str(row["reaction_kind"])
                attempts = int(row["attempts"]) + 1
                now = self._now()
                delays = tuple(max(0.0, float(item)) for item in backoff_seconds) or DEFAULT_BACKOFF_SECONDS
                if attempts >= max(1, int(max_attempts)):
                    next_attempt = None
                else:
                    delay = min(MAX_BACKOFF_SECONDS, delays[min(attempts - 1, len(delays) - 1)])
                    next_attempt = now + delay
                self._conn.execute(
                    """
                    UPDATE onebot_queue_reaction
                    SET state='maybe_set', attempts=?, next_attempt_at=?, last_error=?, updated_at=?
                    WHERE anchor_id=? AND reaction_kind=?
                      AND state IN ('pending','maybe_set')
                    """,
                    (
                        attempts,
                        next_attempt,
                        str(reason)[:512],
                        now,
                        anchor_id,
                        selected_kind,
                    ),
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def pending_reaction_cleanups(self, now: float | None = None) -> tuple[ReactionRecord, ...]:
        """读取不再代表有效 UI 状态且可进行 unset 的 reaction。"""
        with self._operation():
            current = self._now() if now is None else float(now)
            rows = self._conn.execute(
                """
                SELECT reaction.*
                FROM onebot_queue_reaction AS reaction
                WHERE reaction.state IN ('pending','maybe_set')
                  AND reaction.attempts < ?
                  AND (reaction.next_attempt_at IS NULL OR reaction.next_attempt_at<=?)
                   AND (
                       (reaction.reaction_kind='queued' AND NOT EXISTS (
                           SELECT 1 FROM onebot_queue_trigger AS anchor
                           WHERE anchor.request_id=reaction.anchor_id
                             AND anchor.status='pending'
                       ))
                       OR
                        (reaction.reaction_kind IN ('processing','legacy_processing')
                         AND NOT EXISTS (
                            SELECT 1 FROM onebot_queue_message AS message
                            WHERE message.lease_id=reaction.lease_id
                              AND message.state='leased'
                              AND message.lease_until IS NOT NULL
                              AND message.lease_until>?
                        )
                         AND NOT EXISTS (
                            SELECT 1 FROM onebot_queue_trigger AS anchor
                            WHERE anchor.request_id=reaction.anchor_id
                              AND anchor.status='pending'
                        ))
                   )
                ORDER BY reaction.updated_at
                """,
                (3, current, current),
            ).fetchall()
            return tuple(self._row_to_reaction(row) for row in rows)

    def reaction(
        self,
        identifier: str,
        *,
        reaction_kind: str = "processing",
    ) -> ReactionRecord | None:
        """按 anchor 或 lease 读取一个阶段的持久 reaction 状态。"""
        with self._operation():
            row = self._conn.execute(
                """
                SELECT * FROM onebot_queue_reaction
                WHERE reaction_kind=? AND (anchor_id=? OR lease_id=?)
                LIMIT 1
                """,
                (reaction_kind, str(identifier), str(identifier)),
            ).fetchone()
            return self._row_to_reaction(row) if row is not None else None

    def delete_reaction(
        self,
        identifier: str,
        *,
        reaction_kind: str | None = None,
    ) -> bool:
        """删除已经确定清理完成或确定未添加成功的 reaction 记录。"""
        with self._operation():
            cursor = self._conn.execute(
                """
                DELETE FROM onebot_queue_reaction
                WHERE (anchor_id=? OR lease_id=?)
                  AND (? IS NULL OR reaction_kind=?)
                """,
                (str(identifier), str(identifier), reaction_kind, reaction_kind),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def status(self, chat_id: str) -> dict[str, Any]:
        """读取队列数量、阶段、退避原因、摘要和暂停状态。"""
        with self._operation():
            rows = self._conn.execute(
                "SELECT state, COUNT(*) AS count, COALESCE(SUM(byte_size),0) AS bytes FROM onebot_queue_message WHERE chat_id=? GROUP BY state",
                (str(chat_id),),
            ).fetchall()
            counts = {str(row["state"]): int(row["count"]) for row in rows}
            bytes_total = int(sum(int(row["bytes"]) for row in rows))
            chat = self._conn.execute(
                """
                SELECT summary, paused, next_seq, last_trigger_at,
                       llm_judged_seq, llm_next_attempt_at,
                       llm_failure_count, llm_last_error
                FROM onebot_queue_chat WHERE chat_id=?
                """,
                (str(chat_id),),
            ).fetchone()
            trigger = self._conn.execute(
                "SELECT COUNT(*) FROM onebot_queue_trigger WHERE chat_id=? AND status IN ('pending','claimed','uncertain','failed')",
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
            failure_reasons = self._conn.execute(
                """
                SELECT DISTINCT failure_reason FROM onebot_queue_message
                WHERE chat_id=? AND failure_reason IS NOT NULL
                UNION
                SELECT DISTINCT failure_reason FROM onebot_queue_trigger
                WHERE chat_id=? AND failure_reason IS NOT NULL
                """,
                (str(chat_id), str(chat_id)),
            ).fetchall()
            uncertain_reasons = self._conn.execute(
                """
                SELECT DISTINCT uncertain_reason FROM onebot_queue_message
                WHERE chat_id=? AND uncertain_reason IS NOT NULL
                UNION
                SELECT DISTINCT uncertain_reason FROM onebot_queue_trigger
                WHERE chat_id=? AND uncertain_reason IS NOT NULL
                """,
                (str(chat_id), str(chat_id)),
            ).fetchall()
            next_retry = self._conn.execute(
                """
                SELECT MIN(next_attempt_at) FROM (
                    SELECT next_attempt_at
                    FROM onebot_queue_message
                    WHERE chat_id=? AND state='pending' AND next_attempt_at IS NOT NULL
                    UNION ALL
                    SELECT next_attempt_at
                    FROM onebot_queue_trigger
                    WHERE chat_id=? AND status='pending' AND next_attempt_at IS NOT NULL
                )
                """,
                (str(chat_id), str(chat_id)),
            ).fetchall()
            phase = self._conn.execute(
                """
                SELECT
                    CASE
                        WHEN MAX(CASE WHEN state='leased' THEN 1 ELSE 0 END)=0
                        THEN MIN(lease_phase)
                        WHEN MAX(outbound_started)=1
                             OR MAX(CASE WHEN lease_phase='outbound_started' THEN 1 ELSE 0 END)=1
                        THEN 'outbound_started'
                        ELSE MIN(lease_phase)
                    END AS lease_phase,
                    MAX(outbound_started) AS outbound_started
                FROM onebot_queue_message
                WHERE chat_id=? AND state IN ('leased','uncertain','failed')
                HAVING COUNT(*) > 0
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
            reaction_stats = self._conn.execute(
                """
                SELECT
                    SUM(CASE WHEN state IN ('pending','maybe_set') AND attempts < 3 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state IN ('pending','maybe_set') AND attempts >= 3 THEN 1 ELSE 0 END),
                    MAX(last_error)
                FROM onebot_queue_reaction
                WHERE chat_id=?
                """,
                (str(chat_id),),
            ).fetchone()
            chat_type = self._conn.execute(
                "SELECT chat_type FROM onebot_queue_message WHERE chat_id=? LIMIT 1",
                (str(chat_id),),
            ).fetchone()
            earliest_anchor = self._conn.execute(
                """
                SELECT * FROM onebot_queue_trigger WHERE chat_id=?
                ORDER BY COALESCE(anchor_seq, 9223372036854775807),created_at,request_id
                LIMIT 1
                """,
                (str(chat_id),),
            ).fetchone()
            anchor_view = None
            if earliest_anchor is not None:
                anchor_view = {
                    "anchor_id": str(earliest_anchor["request_id"]),
                    "kind": str(earliest_anchor["anchor_kind"] or "message"),
                    "seq": (
                        int(earliest_anchor["anchor_seq"])
                        if earliest_anchor["anchor_seq"] is not None
                        else None
                    ),
                    "status": str(earliest_anchor["status"]),
                    "reason": str(earliest_anchor["reason"]),
                    "caller_user_id": str(earliest_anchor["caller_user_id"]),
                    "caller_user_name": str(earliest_anchor["caller_user_name"]),
                    "failure_count": int(earliest_anchor["failure_count"] or 0),
                    "next_attempt_at": (
                        float(earliest_anchor["next_attempt_at"])
                        if earliest_anchor["next_attempt_at"] is not None
                        else None
                    ),
                    "failure_reason": (
                        str(earliest_anchor["failure_reason"])
                        if earliest_anchor["failure_reason"] is not None
                        else None
                    ),
                    "uncertain_reason": (
                        str(earliest_anchor["uncertain_reason"])
                        if earliest_anchor["uncertain_reason"] is not None
                        else None
                    ),
                }
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
                "earliest_anchor": anchor_view,
                "lease_until": float(active_lease[0]) if active_lease and active_lease[0] is not None else None,
                "lease_phase": str(phase[0]) if phase else None,
                "outbound_started": bool(phase[1]) if phase else False,
                "failure_count": int(failure_stats[0]) if failure_stats else 0,
                "attempts": int(failure_stats[1]) if failure_stats else 0,
                "next_retry_at": float(next_retry[0][0]) if next_retry and next_retry[0][0] is not None else None,
                "failure_reasons": [str(row[0]) for row in failure_reasons],
                "uncertain_reasons": [str(row[0]) for row in uncertain_reasons],
                "paused": bool(chat["paused"]) if chat else False,
                "next_seq": int(chat["next_seq"]) if chat else 1,
                "summary": str(chat["summary"]) if chat else "",
                "last_trigger_at": (
                    float(chat["last_trigger_at"])
                    if chat and chat["last_trigger_at"] is not None
                    else None
                ),
                "llm_judged_seq": int(chat["llm_judged_seq"]) if chat else 0,
                "llm_next_attempt_at": (
                    float(chat["llm_next_attempt_at"])
                    if chat and chat["llm_next_attempt_at"] is not None
                    else None
                ),
                "llm_failure_count": int(chat["llm_failure_count"]) if chat else 0,
                "llm_last_error": (
                    str(chat["llm_last_error"])
                    if chat and chat["llm_last_error"]
                    else None
                ),
                "pending_reaction_cleanups": int(reaction_stats[0] or 0) if reaction_stats else 0,
                "exhausted_reaction_cleanups": int(reaction_stats[1] or 0) if reaction_stats else 0,
                "reaction_last_error": (
                    str(reaction_stats[2])
                    if reaction_stats and reaction_stats[2] is not None
                    else None
                ),
            }

    def status_for_lease(self, lease_id: str) -> dict[str, Any]:
        """读取 lease 的持久阶段，供 adapter 在 completion 时判定结果。"""
        with self._operation():
            row = self._conn.execute(
                """
                SELECT
                    CASE
                        WHEN MAX(outbound_started)=1
                             OR MAX(CASE WHEN lease_phase='outbound_started' THEN 1 ELSE 0 END)=1
                        THEN 'outbound_started'
                        ELSE MIN(lease_phase)
                    END AS lease_phase,
                    MAX(outbound_started) AS outbound_started,
                    MIN(state) AS state,
                    MIN(lease_until) AS lease_until,
                    MIN(chat_id) AS chat_id,
                    MIN(chat_type) AS chat_type
                FROM onebot_queue_message
                WHERE lease_id=?
                """,
                (str(lease_id),),
            ).fetchone()
            if row is None or row["chat_id"] is None:
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
        with self._operation():
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
        with self._operation():
            self._conn.execute(
                "INSERT INTO onebot_queue_chat(chat_id, paused, updated_at) VALUES (?, ?, ?) ON CONFLICT(chat_id) DO UPDATE SET paused=excluded.paused, updated_at=excluded.updated_at",
                (str(chat_id), int(bool(paused)), self._now()),
            )
            self._conn.commit()

    def recover_trigger_requests(
        self,
        allowed_chat_ids: tuple[str, ...] | set[str] | frozenset[str] | None = None,
    ) -> tuple[TriggerRequest, ...]:
        """启动恢复过期 lease，并返回仍需 dispatch 的持久触发请求。"""
        with self._operation():
            try:
                self._transaction()
                now = self._now()
                trigger_update_scope, trigger_scope_args = self._chat_scope(
                    "chat_id", allowed_chat_ids
                )
                trigger_select_scope, _ = self._chat_scope("trigger.chat_id", allowed_chat_ids)
                message_scope, message_scope_args = self._chat_scope(
                    "onebot_queue_message.chat_id", allowed_chat_ids
                )
                message_select_scope, _ = self._chat_scope("message.chat_id", allowed_chat_ids)
                claimed_chats = self._conn.execute(
                    f"""
                    SELECT DISTINCT chat_id
                    FROM onebot_queue_trigger
                    WHERE status='claimed' {trigger_update_scope}
                    """,
                    trigger_scope_args,
                ).fetchall()
                for claimed_chat in claimed_chats:
                    self._recover_expired_anchors(str(claimed_chat[0]), now)
                self._conn.execute(
                    f"""
                    UPDATE onebot_queue_message
                    SET state='uncertain',
                        lease_id=NULL, lease_until=NULL, lease_owner=NULL,
                        lease_phase='uncertain',outbound_started=1,
                        uncertain_reason='过期 lease 缺少可验证 TurnAnchor，需管理员处理',
                        updated_at=?
                    WHERE state='leased' {message_scope}
                      AND (lease_until IS NULL OR lease_until<=?)
                      AND NOT EXISTS (
                          SELECT 1 FROM onebot_queue_trigger AS trigger
                          WHERE trigger.request_id=onebot_queue_message.anchor_id
                             OR trigger.lease_id=onebot_queue_message.lease_id
                      )
                    """,
                    (now, *message_scope_args, now),
                )
                self._conn.execute(
                    f"""
                    DELETE FROM onebot_queue_trigger
                    WHERE status='pending' {trigger_update_scope}
                      AND anchor_kind='message'
                      AND NOT EXISTS (
                          SELECT 1 FROM onebot_queue_message
                          WHERE onebot_queue_message.chat_id=onebot_queue_trigger.chat_id
                            AND onebot_queue_message.anchor_id=onebot_queue_trigger.request_id
                      )
                    """,
                    trigger_scope_args,
                )
                self._conn.commit()
                rows = self._conn.execute(
                    f"""
                    SELECT trigger.* FROM onebot_queue_trigger AS trigger
                    WHERE trigger.status='pending' {trigger_select_scope}
                      AND trigger.anchor_kind NOT IN ('legacy','service')
                      AND EXISTS (
                        SELECT 1 FROM onebot_queue_message AS message
                        WHERE message.chat_id=trigger.chat_id
                          {message_select_scope}
                          AND message.state='pending'
                          AND (
                              message.anchor_id=trigger.request_id
                              OR (trigger.anchor_kind='operator'
                                  AND message.anchor_id IS NULL
                                  AND message.seq<=trigger.anchor_seq)
                          )
                    ) ORDER BY trigger.created_at
                    """,
                    (*trigger_scope_args, *message_scope_args),
                ).fetchall()
                return tuple(self._row_to_trigger(row) for row in rows)
            except Exception:
                self._conn.rollback()
                raise

    def _build_batch_summary(self, rows: list[sqlite3.Row]) -> str:
        """生成当前 lease 的早期消息摘要，不跨 ack 保存上下文。"""
        summary_rows = rows[:-self.recent_originals] if self.recent_originals else rows
        if not summary_rows:
            return ""
        lines = [
            f"#{row['seq']} [{row['user_name']}] {str(row['text'])}"
            for row in summary_rows
        ]
        result = "\n".join(lines)
        if len(result.encode("utf-8")) <= self.max_summary_bytes:
            return result
        marker = "[本次队列摘要较大，较早消息已裁剪]"
        marker_bytes = len(marker.encode("utf-8")) + 1
        tail = self._truncate_utf8_tail(
            result,
            max(0, self.max_summary_bytes - marker_bytes),
        )
        return f"{marker}\n{tail}" if tail else marker[: self.max_summary_bytes]

    def _row_to_message(self, row: sqlite3.Row) -> QueueMessage:
        """把 SQLite 行转换为不可变队列消息。"""
        try:
            metadata = json.loads(str(row["metadata_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        return QueueMessage(
            chat_id=str(row["chat_id"]),
            chat_type=str(row["chat_type"]),
            message_id=str(row["message_id"] or ""),
            user_id=str(row["user_id"]),
            user_name=str(row["user_name"]),
            text=str(row["text"]),
            raw_text=str(row["raw_text"] or ""),
            metadata=metadata if isinstance(metadata, dict) else {},
            created_at=float(row["created_at"]),
            message_key=str(row["message_key"]),
            seq=int(row["seq"]),
            byte_size=int(row["byte_size"]),
            anchor_id=str(row["anchor_id"]) if row["anchor_id"] is not None else None,
        )

    def _row_to_trigger(self, row: sqlite3.Row) -> TriggerRequest:
        """把 SQLite 行转换为 TurnAnchor。"""
        return TurnAnchor(
            request_id=str(row["request_id"]),
            chat_id=str(row["chat_id"]),
            message_key=str(row["message_key"]),
            reason=str(row["reason"]),
            caller_user_id=str(row["caller_user_id"]),
            caller_user_name=str(row["caller_user_name"]),
            created_at=float(row["created_at"]),
            anchor_seq=(int(row["anchor_seq"]) if row["anchor_seq"] is not None else None),
            anchor_kind=str(row["anchor_kind"] or "message"),
            batch_start_seq=(
                int(row["batch_start_seq"])
                if row["batch_start_seq"] is not None
                else None
            ),
            control_message_id=(
                str(row["control_message_id"])
                if row["control_message_id"] is not None
                else None
            ),
            failure_count=int(row["failure_count"] or 0),
            next_attempt_at=(
                float(row["next_attempt_at"])
                if row["next_attempt_at"] is not None
                else None
            ),
            failure_reason=(
                str(row["failure_reason"])
                if row["failure_reason"] is not None
                else None
            ),
            status=str(row["status"]),
            lease_id=str(row["lease_id"]) if row["lease_id"] is not None else None,
            uncertain_reason=(
                str(row["uncertain_reason"])
                if row["uncertain_reason"] is not None
                else None
            ),
        )

    def _row_to_reaction(self, row: sqlite3.Row) -> ReactionRecord:
        """把 SQLite reaction 行转换为不可变记录。"""
        return ReactionRecord(
            lease_id=str(row["lease_id"] or ""),
            chat_id=str(row["chat_id"]),
            message_id=str(row["message_id"]),
            state=str(row["state"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            attempts=int(row["attempts"]),
            next_attempt_at=(
                float(row["next_attempt_at"]) if row["next_attempt_at"] is not None else None
            ),
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
            anchor_id=str(row["anchor_id"]),
            reaction_kind=str(row["reaction_kind"]),
            emoji_id=str(row["emoji_id"] or ""),
        )

    def close(self) -> None:
        """等待已进入的同步操作完成后关闭 SQLite 连接。"""
        with self._condition:
            if self._closed:
                return
            self._closed = True
            while self._active_operations > 0:
                self._condition.wait()
            self._conn.close()
