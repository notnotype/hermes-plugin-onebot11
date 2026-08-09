"""SQLite 队列的租约、恢复和去重合同测试。"""

import sqlite3

import pytest

from onebot11.queue import SCHEMA_VERSION, QueueError, QueueMessage, QueueStore, TriggerRequest


def _message(message_id: str, *, chat_id: str = "888", text: str = "消息") -> QueueMessage:
    """构造一个稳定的群消息。"""
    return QueueMessage(
        chat_id=chat_id,
        chat_type="group",
        message_id=message_id,
        user_id="123",
        user_name="小明",
        text=text,
        message_key=f"group:{message_id}",
    )


def _trigger(message: QueueMessage, reason: str = "mention") -> TriggerRequest:
    """构造与消息一一对应的持久触发请求。"""
    return TriggerRequest.create(
        message.chat_id,
        str(message.message_key),
        reason,
        message.user_id,
        message.user_name,
    )


def test_一个lease覆盖并确认整批trigger(tmp_path):
    """每个消息 anchor 独立消费，多个 anchor 仍按群内顺序串行。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    for message_id in ("1", "2", "3"):
        message = _message(message_id)
        store.enqueue(message, _trigger(message))

    lease = store.claim("888")
    assert lease is not None
    assert [message.message_id for message in lease.messages] == ["1"]
    assert store.status("888")["trigger_requests"] == 2
    assert store.ack(lease)
    assert store.status("888")["pending"] == 2
    second = store.claim("888")
    assert second is not None
    assert [message.message_id for message in second.messages] == ["2"]
    assert store.ack(second)
    third = store.claim("888")
    assert third is not None
    assert [message.message_id for message in third.messages] == ["3"]
    assert store.ack(third)
    assert store.status("888")["trigger_requests"] == 0
    assert store.peek("888") == ()
    assert store.recover_trigger_requests() == ()
    store.close()


def test_恢复不抢占仍有效的其他进程租约(tmp_path, monkeypatch):
    """启动第二个进程时，只能恢复已过期 lease。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    owner = QueueStore(path)
    other = QueueStore(path)
    message = _message("1")
    owner.enqueue(message, _trigger(message))
    lease = owner.claim("888", lease_seconds=30)
    assert lease is not None

    assert other.recover_trigger_requests() == ()
    assert other.claim("888") is None

    now[0] = 1031.0
    assert other.recover_trigger_requests() == ()
    now[0] = 1033.1
    recovered = other.recover_trigger_requests()
    assert len(recovered) == 1
    assert recovered[0].request_id == lease.trigger.request_id
    assert other.claim("888") is not None
    owner.close()
    other.close()


def test_clear保留消息去重事实(tmp_path):
    """管理员 clear 后，OneBot 重放的旧事件不能重新入队。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("1")
    assert store.enqueue(message).inserted
    assert store.clear("888") == 1
    result = store.enqueue(message)
    assert result.duplicate
    assert not result.inserted
    assert store.status("888")["pending"] == 0
    store.close()


def test_discard_uncertain保留消息去重事实(tmp_path):
    """人工 discard 后，重复事件仍被 tombstone 拦截。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("1")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    assert store.mark_uncertain(lease, "发送状态未知")
    assert store.resolve_uncertain("888", "discard") == 1
    result = store.enqueue(message)
    assert result.duplicate
    assert store.status("888")["uncertain"] == 0
    store.close()


def test_租约失败释放后按退避重新认领(tmp_path, monkeypatch):
    """明确失败只释放当前 lease，并按退避时间重新认领。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("1")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    assert store.release(lease, reason="明确失败")
    assert store.status("888")["pending"] == 1
    assert store.status("888")["failure_count"] == 1
    assert store.status("888")["failure_reasons"] == ["明确失败"]
    assert store.claim("888") is None
    monkeypatch.setattr("onebot11.queue.time.time", lambda: lease.claimed_at + 2.1)
    retried = store.claim("888")
    assert retried is not None
    store.close()


def test_退避期间恢复不会反复唤醒触发请求(tmp_path, monkeypatch):
    """自动失败退避期间不应被恢复轮询提前重新 dispatch。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("backoff-recovery")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    assert store.release(lease)

    assert store.pending_chat_ids() == ()
    assert store.recover_trigger_requests() == ()

    now[0] = 1002.1
    assert store.pending_chat_ids() == ("888",)
    assert len(store.recover_trigger_requests()) == 1
    store.close()


def test_一个群退避不会阻塞其他群恢复(tmp_path, monkeypatch):
    """同群按顺序等待退避，但不能用全局 break 卡住其他群。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "multi-chat-recovery.sqlite3")

    blocked = _message("blocked", chat_id="888")
    store.enqueue(blocked, _trigger(blocked))
    lease = store.claim("888")
    assert lease is not None
    assert store.release(lease, reason="暂时失败")

    now[0] = 1000.1
    ready = _message("ready", chat_id="889")
    store.enqueue(ready, _trigger(ready))

    recovered = store.recover_trigger_requests()

    assert [trigger.chat_id for trigger in recovered] == ["889"]
    store.close()


def test_成功出站可以确认并删除消息(tmp_path):
    """出站阶段已开始不等于结果未知；明确成功仍可 ack。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("1")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    assert store.mark_outbound_started(lease)
    assert store.ack(lease)
    assert store.status("888")["pending"] == 0
    assert store.status("888")["uncertain"] == 0
    store.close()


def test_出站marker后过期不会自动release(tmp_path, monkeypatch):
    """非幂等出站已开始时，lease 到期只能进入 uncertain。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("marker")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888", lease_seconds=5)
    assert lease is not None
    assert store.mark_outbound_started(lease)
    assert store.release(lease, reason="明确失败", allow_after_outbound=True)
    assert store.status("888")["uncertain"] == 1
    assert store.status("888")["lease_phase"] == "uncertain"
    now[0] = 1006.0
    assert store.recover_trigger_requests() == ()
    assert store.status("888")["uncertain"] == 1
    store.close()


def test_旧schema活动lease迁移为uncertain(tmp_path):
    """旧文件无法证明出站阶段时，启动恢复必须 fail-closed。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("legacy")
    store.enqueue(message, _trigger(message))
    assert store.claim("888", lease_seconds=30) is not None
    store.close()
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=4")
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    assert migrated.status("888")["uncertain"] == 1
    assert migrated.recover_trigger_requests() == ()
    migrated.close()


def test_真实v7表结构先补列再建索引(tmp_path):
    """真实旧表没有 anchor_id 时，迁移不能在创建索引阶段提前失败。"""
    path = tmp_path / "queue-v7.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE onebot_queue_chat (
            chat_id TEXT PRIMARY KEY,
            next_seq INTEGER NOT NULL DEFAULT 1,
            summary TEXT NOT NULL DEFAULT '',
            paused INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL
        );
        CREATE TABLE onebot_queue_message (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            message_key TEXT NOT NULL,
            chat_type TEXT NOT NULL,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            text TEXT NOT NULL,
            raw_text TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            seq INTEGER NOT NULL,
            byte_size INTEGER NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','leased','uncertain')),
            lease_id TEXT,
            lease_until REAL,
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(chat_id, message_key)
        );
        CREATE TABLE onebot_queue_trigger (
            request_id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            message_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            caller_user_id TEXT NOT NULL,
            caller_user_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending','claimed','uncertain')),
            lease_id TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(chat_id, message_key)
        );
        INSERT INTO onebot_queue_chat(chat_id, next_seq, updated_at)
        VALUES ('888', 2, 1000);
        INSERT INTO onebot_queue_message(
            chat_id, message_key, chat_type, user_id, user_name, text, raw_text,
            metadata_json, seq, byte_size, state, lease_id, lease_until,
            attempts, created_at, updated_at
        ) VALUES (
            '888', 'group:legacy-v7', 'group', '123', '小明', '旧消息', '旧消息',
            '{}', 1, 100, 'leased', 'legacy-lease', 990, 1, 900, 900
        );
        INSERT INTO onebot_queue_trigger(
            request_id, chat_id, message_key, reason, caller_user_id,
            caller_user_name, status, lease_id, created_at, updated_at
        ) VALUES (
            'legacy-trigger', '888', 'group:legacy-v7', 'mention', '123',
            '小明', 'claimed', 'legacy-lease', 900, 900
        );
        PRAGMA user_version=7;
        """
    )
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    columns = {
        str(row[1])
        for row in migrated._conn.execute(
            "PRAGMA table_info(onebot_queue_message)"
        ).fetchall()
    }
    assert "anchor_id" in columns
    assert migrated.status("888")["uncertain"] == 1
    migrated.close()


def _write_legacy_queue_schema(path, version: int) -> None:
    """写入 v7/v8/v9/v10 的真实表形状，而不是只修改 user_version。"""
    message_extra = ""
    if version >= 8:
        message_extra = """
            message_id TEXT NOT NULL DEFAULT '',
            lease_owner TEXT,
            uncertain_reason TEXT,
            lease_phase TEXT NOT NULL DEFAULT 'pending',
            outbound_started INTEGER NOT NULL DEFAULT 0,
            failure_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL,
            failure_reason TEXT,
        """
    message_anchor = "anchor_id TEXT," if version >= 9 else ""
    message_state = (
        "CHECK(state IN ('pending','leased','uncertain','failed'))"
        if version >= 8
        else "CHECK(state IN ('pending','leased','uncertain'))"
    )
    trigger_extra = ""
    if version >= 9:
        trigger_extra = """
            anchor_seq INTEGER,
            anchor_kind TEXT NOT NULL DEFAULT 'message',
            batch_start_seq INTEGER,
            control_message_id TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL,
            failure_reason TEXT,
        """
    trigger_lease_extra = ""
    if version >= 8:
        trigger_lease_extra = """
            lease_owner TEXT,
            uncertain_reason TEXT,
        """
    authority_extra = ""
    authority_values = "NULL, NULL,"
    if version >= 10:
        authority_extra = """
            authority_role TEXT,
            authority_tools_json TEXT,
        """
        authority_values = "'user', '[\"qq_get_message\"]',"
    connection = sqlite3.connect(path)
    connection.executescript(
        f"""
        CREATE TABLE onebot_queue_chat (
            chat_id TEXT PRIMARY KEY,
            next_seq INTEGER NOT NULL DEFAULT 1,
            summary TEXT NOT NULL DEFAULT '',
            paused INTEGER NOT NULL DEFAULT 0,
            revision INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL
        );
        CREATE TABLE onebot_queue_message (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            message_key TEXT NOT NULL,
            chat_type TEXT NOT NULL,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            text TEXT NOT NULL,
            raw_text TEXT NOT NULL,
            {message_extra}
            metadata_json TEXT NOT NULL,
            seq INTEGER NOT NULL,
            byte_size INTEGER NOT NULL,
            state TEXT NOT NULL {message_state},
            lease_id TEXT,
            lease_until REAL,
            attempts INTEGER NOT NULL DEFAULT 0,
            {message_anchor}
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(chat_id, message_key)
        );
        CREATE TABLE onebot_queue_trigger (
            request_id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            message_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            caller_user_id TEXT NOT NULL,
            caller_user_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending','claimed','uncertain','failed')),
            lease_id TEXT,
            {trigger_lease_extra}
            {trigger_extra}
            {authority_extra}
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(chat_id, message_key)
        );
        CREATE TABLE onebot_queue_reaction (
            lease_id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        INSERT INTO onebot_queue_chat(chat_id, next_seq, updated_at)
        VALUES ('888', 2, 1000);
        INSERT INTO onebot_queue_message(
            chat_id,message_key,chat_type,user_id,user_name,text,raw_text,
            metadata_json,seq,byte_size,state,lease_id,lease_until,attempts,
            created_at,updated_at
        ) VALUES (
            '888','group:legacy-{version}','group','123','小明','旧消息','旧消息',
            '{{}}',1,100,'pending',NULL,NULL,0,900,900
        );
        INSERT INTO onebot_queue_trigger(
            request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
            status,lease_id,{ "anchor_seq,anchor_kind,batch_start_seq,control_message_id," if version >= 9 else "" }
            { "failure_count,next_attempt_at,failure_reason," if version >= 9 else "" }
            { "authority_role,authority_tools_json," if version >= 10 else "" }
            created_at,updated_at
        ) VALUES (
            'legacy-trigger-{version}','888','group:legacy-{version}','mention','123','小明',
            'pending',NULL,{ "NULL,'message',NULL,NULL," if version >= 9 else "" }
            { "0,NULL,NULL," if version >= 9 else "" }
            { authority_values if version >= 10 else "" }
            900,900
        );
        CREATE INDEX idx_legacy_trigger_status
            ON onebot_queue_trigger(status{", anchor_seq" if version >= 9 else ""});
        CREATE INDEX idx_legacy_message_state
            ON onebot_queue_message(chat_id, state{", anchor_id" if version >= 9 else ""});
        PRAGMA user_version={version};
        """
    )
    connection.commit()
    connection.close()


@pytest.mark.parametrize("version", [7, 8, 9, 10])
def test_v7到v10真实表结构迁移到schema11(tmp_path, version):
    """四个已部署版本都必须保留核心消息并安全处理 authority/reaction。"""
    path = tmp_path / f"queue-v{version}.sqlite3"
    _write_legacy_queue_schema(path, version)

    migrated = QueueStore(path)
    assert migrated._conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    assert migrated._conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='onebot_queue_reaction'"
    ).fetchone()[0] == 0
    assert migrated.status("888")["pending"] == (1 if version == 10 else 0)
    assert migrated.status("888")["uncertain"] == (0 if version == 10 else 1)
    if version == 10:
        assert len(migrated.recover_trigger_requests()) == 1
    else:
        assert migrated.recover_trigger_requests() == ()
    migrated.close()


def test_v10有完整authority和phase的活动lease不被误标unknown(tmp_path):
    """补列本身不能把仍可证明的 v10 活动 lease 变成 uncertain。"""
    path = tmp_path / "queue-v10-active.sqlite3"
    _write_legacy_queue_schema(path, 10)
    connection = sqlite3.connect(path)
    connection.execute(
        """
        UPDATE onebot_queue_message
        SET state='leased', lease_id='v10-lease', lease_until=9999999999,
            lease_owner='old-owner', lease_phase='agent_running',
            outbound_started=0
        WHERE chat_id='888'
        """
    )
    connection.execute(
        """
        UPDATE onebot_queue_trigger
        SET status='claimed', lease_id='v10-lease', lease_owner='old-owner'
        WHERE chat_id='888'
        """
    )
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    assert migrated.status("888")["leased"] == 1
    assert migrated.status("888")["uncertain"] == 0
    migrated.close()


def test_恢复白名单只触碰允许群(tmp_path):
    """恢复时不能因为本地旧数据存在就启动白名单外群。"""
    store = QueueStore(tmp_path / "queue-scope.sqlite3")
    for chat_id in ("1072992996", "9999999999"):
        message = _message(f"scope-{chat_id}", chat_id=chat_id)
        store.enqueue(message, _trigger(message))
    assert [item.chat_id for item in store.recover_trigger_requests({"1072992996"})] == [
        "1072992996"
    ]
    assert store.status("9999999999")["pending"] == 1
    assert store.status("9999999999")["pending_trigger_requests"] == 1
    store.close()


def test_迁移失败后连接已关闭(tmp_path, monkeypatch):
    """初始化迁移异常不能留下仍占用文件的 SQLite 连接。"""
    opened: list[sqlite3.Connection] = []
    original_open = QueueStore._open_connection

    def tracked_open(self: QueueStore) -> sqlite3.Connection:
        connection = original_open(self)
        opened.append(connection)
        return connection

    def fail_migrate(self: QueueStore) -> None:
        raise QueueError("synthetic migration failure")

    monkeypatch.setattr(QueueStore, "_open_connection", tracked_open)
    monkeypatch.setattr(QueueStore, "_migrate", fail_migrate)
    with pytest.raises(QueueError, match="synthetic migration failure"):
        QueueStore(tmp_path / "migration-failure.sqlite3")
    assert opened
    with pytest.raises(sqlite3.ProgrammingError):
        opened[0].execute("SELECT 1")


def test_reopen迁移失败后仍保持closed(tmp_path, monkeypatch):
    """同实例 reconnect 的迁移异常也不能留下半开的连接。"""
    path = tmp_path / "reopen-failure.sqlite3"
    store = QueueStore(path)
    store.close()
    opened: list[sqlite3.Connection] = []
    original_open = QueueStore._open_connection
    original_migrate = QueueStore._migrate

    def tracked_open(self: QueueStore) -> sqlite3.Connection:
        connection = original_open(self)
        opened.append(connection)
        return connection

    def fail_migrate(self: QueueStore) -> None:
        raise QueueError("synthetic reopen failure")

    monkeypatch.setattr(QueueStore, "_open_connection", tracked_open)
    monkeypatch.setattr(QueueStore, "_migrate", fail_migrate)
    with pytest.raises(QueueError, match="synthetic reopen failure"):
        store.reopen()
    assert store.closed
    with pytest.raises(sqlite3.ProgrammingError):
        opened[0].execute("SELECT 1")
    monkeypatch.setattr(QueueStore, "_migrate", original_migrate)


def test_未知更高schema版本拒绝启动(tmp_path):
    """不能把未来 schema 当成当前 schema 静默打开。"""
    path = tmp_path / "queue-future.sqlite3"
    store = QueueStore(path)
    store.close()
    connection = sqlite3.connect(path)
    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION + 1}")
    connection.commit()
    connection.close()

    with pytest.raises(QueueError, match="高于支持版本"):
        QueueStore(path)


def test_pending_chat_ids只返回待处理群(tmp_path):
    """启动时可以发现没有 durable trigger 的遗留消息。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("1")
    store.enqueue(message)
    assert store.pending_chat_ids() == ("888",)
    row = store._conn.execute(
        "SELECT state, lease_phase FROM onebot_queue_message WHERE message_key=?",
        ("group:1",),
    ).fetchone()
    assert tuple(row) == ("pending", "pending")
    lease = store.claim("888")
    assert lease is None
    assert store.pending_chat_ids() == ("888",)
    store.close()


def test_重启时规范化无租约pending阶段(tmp_path):
    """旧文件中无 lease 的 pending 消息不能伪装成 agent_running。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("legacy-pending")
    store.enqueue(message)
    store._conn.execute(
        "UPDATE onebot_queue_message SET lease_phase='agent_running' WHERE message_key=?",
        ("group:legacy-pending",),
    )
    store._conn.commit()
    store.close()

    reopened = QueueStore(path)
    row = reopened._conn.execute(
        "SELECT state, lease_phase FROM onebot_queue_message WHERE message_key=?",
        ("group:legacy-pending",),
    ).fetchone()
    assert tuple(row) == ("pending", "pending")
    reopened.close()


def test_队列保留调用方提供的原文(tmp_path):
    """原文字段不能被错误地替换成已经规范化的正文。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="raw",
        user_id="123",
        user_name="小明",
        text="规范化正文",
        raw_text="[CQ:face,id=1]规范化正文",
        message_key="group:raw",
    )
    assert store.enqueue(message).inserted
    assert store.peek("888")[0].raw_text == "[CQ:face,id=1]规范化正文"
    store.close()


def test_同实例close后reopen可以继续读写(tmp_path):
    """adapter reconnect 复用同一路径时，SQLite 连接和 owner 都恢复。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("reopen-1")
    store.enqueue(message)
    store.close()
    store.reopen()
    assert store.peek("888")[0].message_id == "reopen-1"
    assert store.enqueue(_message("reopen-2")).inserted
    store.close()


def test_断开结算未出站和已出站lease(tmp_path):
    """断开时未开始出站的 turn 可恢复，已写 marker 的 turn 必须 uncertain。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = _message("disconnect-pending", chat_id="888")
    second = _message("disconnect-uncertain", chat_id="889")
    store.enqueue(first, _trigger(first))
    store.enqueue(second, _trigger(second))
    first_lease = store.claim("888")
    assert first_lease is not None
    assert store.abandon_owner_leases() == {"pending": 1, "uncertain": 0}
    uncertain = store.claim("889")
    assert uncertain is not None
    assert store.mark_outbound_started(uncertain)
    assert store.abandon_owner_leases() == {"pending": 0, "uncertain": 1}
    assert store.status("889")["uncertain"] == 1
    store.close()


def test_管理动作台账崩溃恢复和人工resolve(tmp_path):
    """started 重启后变 unknown，retry 只解除阻断，不直接执行动作。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    started = store.start_operation(
        fingerprint="fingerprint-1",
        tool_name="qq_set_group_ban",
        chat_type="group",
        chat_id="888",
        caller_user_id="123",
        params={"user_id": "456", "duration": 60},
    )
    assert started.started
    operation_id = started.operation.operation_id
    store.close()

    reopened = QueueStore(path)
    assert reopened.operation_records("888")[0].status == "unknown"
    blocked = reopened.start_operation(
        fingerprint="fingerprint-1",
        tool_name="qq_set_group_ban",
        chat_type="group",
        chat_id="888",
        caller_user_id="123",
        params={"user_id": "456", "duration": 60},
    )
    assert blocked.blocked
    armed = reopened.resolve_operation(
        operation_id,
        "retry",
        chat_type="group",
        chat_id="888",
        caller_user_id="123",
    )
    assert armed is not None and armed.status == "retry_armed"
    retried = reopened.start_operation(
        fingerprint="fingerprint-1",
        tool_name="qq_set_group_ban",
        chat_type="group",
        chat_id="888",
        caller_user_id="123",
        params={"user_id": "456", "duration": 60},
    )
    assert retried.started
    assert reopened.finish_operation(retried.operation.operation_id, "succeeded")
    assert reopened.unknown_operation_count("888") == 0
    assert reopened.resolve_operation(
        operation_id,
        "discard",
        chat_type="group",
        chat_id="999",
        caller_user_id="123",
    ) is None
    reopened.close()


def test_release与已有pending_trigger按anchor顺序恢复(tmp_path, monkeypatch):
    """旧 lease 释放时不能与后来的同群 trigger 冲突或丢失重试入口。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = _message("merge-release")
    store.enqueue(first, _trigger(first))
    lease = store.claim("888")
    assert lease is not None
    later = _message("merge-release-later")
    store.enqueue(later, _trigger(later))

    assert store.release(lease, reason="agent failed")
    assert store.status("888")["pending_trigger_requests"] == 2
    assert store.claim("888") is None
    now[0] = 1002.1
    recovered = store.claim("888")
    assert recovered is not None
    assert [item.message_id for item in recovered.messages] == ["merge-release"]
    assert store.ack(recovered)
    followup = store.claim("888")
    assert followup is not None
    assert [item.message_id for item in followup.messages] == ["merge-release-later"]
    assert store.ack(followup)
    assert store.status("888")["pending_trigger_requests"] == 0

    store.close()


def test_recover与已有pending_trigger合并(tmp_path, monkeypatch):
    """过期 lease 恢复时应保留唯一 pending trigger。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    owner = QueueStore(path)
    other = QueueStore(path)
    first = _message("merge-recover")
    owner.enqueue(first, _trigger(first))
    lease = owner.claim("888", lease_seconds=5)
    assert lease is not None
    later = _message("merge-recover-later")
    owner.enqueue(later, _trigger(later))

    now[0] = 1006.0
    assert other.recover_trigger_requests() == ()
    assert other.status("888")["pending_trigger_requests"] == 2
    owner.close()
    other.close()


def test_abandon与已有pending_trigger合并(tmp_path):
    """断开结算未出站 lease 时不能把同群 trigger 撞成唯一索引错误。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = _message("merge-abandon")
    store.enqueue(first, _trigger(first))
    assert store.claim("888") is not None
    later = _message("merge-abandon-later")
    store.enqueue(later, _trigger(later))

    assert store.abandon_owner_leases() == {"pending": 1, "uncertain": 0}
    assert store.status("888")["pending_trigger_requests"] == 2
    store.close()


def test_abandon缺失旧trigger不吸收后续anchor(tmp_path):
    """断线时旧 trigger 丢失也必须优先恢复原 lease 的最早消息。"""
    store = QueueStore(tmp_path / "queue-abandon-missing-trigger.sqlite3")
    first = _message("abandon-missing")
    store.enqueue(first, _trigger(first))
    assert store.claim("888") is not None
    store._conn.execute(
        "DELETE FROM onebot_queue_trigger WHERE message_key=?",
        (first.message_key,),
    )
    store._conn.commit()
    later = _message("abandon-missing-later")
    store.enqueue(later, _trigger(later))

    assert store.abandon_owner_leases() == {"pending": 1, "uncertain": 0}
    recovered = store.claim("888")
    assert recovered is not None
    assert recovered.trigger.message_key == first.message_key
    assert [message.message_key for message in recovered.messages] == [first.message_key]
    store.close()


def test_release缺失旧trigger不吸收后续anchor(tmp_path):
    """明确失败时旧 trigger 丢失也必须按最早消息创建恢复 anchor。"""
    store = QueueStore(tmp_path / "queue-release-missing-trigger.sqlite3")
    first = _message("release-missing")
    store.enqueue(first, _trigger(first))
    lease = store.claim("888")
    assert lease is not None
    store._conn.execute(
        "DELETE FROM onebot_queue_trigger WHERE message_key=?",
        (first.message_key,),
    )
    store._conn.commit()
    later = _message("release-missing-later")
    store.enqueue(later, _trigger(later))

    assert store.release(lease, reason="Agent failed")
    recovered = store.claim("888")
    assert recovered is not None
    assert recovered.trigger.message_key == first.message_key
    assert [message.message_key for message in recovered.messages] == [first.message_key]
    store.close()


def test_recover缺失旧trigger不吸收后续anchor(tmp_path, monkeypatch):
    """过期 lease 的恢复也不能因旧 trigger 丢失而跳到后续 anchor。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue-recover-missing-trigger.sqlite3")
    first = _message("recover-missing")
    store.enqueue(first, _trigger(first))
    lease = store.claim("888", lease_seconds=5)
    assert lease is not None
    store._conn.execute(
        "DELETE FROM onebot_queue_trigger WHERE message_key=?",
        (first.message_key,),
    )
    store._conn.commit()
    later = _message("recover-missing-later")
    store.enqueue(later, _trigger(later))

    now[0] = 1006.0
    recovered_requests = store.recover_trigger_requests()
    assert recovered_requests
    assert recovered_requests[0].message_key == first.message_key
    recovered = store.claim("888")
    assert recovered is not None
    assert recovered.trigger.message_key == first.message_key
    assert [message.message_key for message in recovered.messages] == [first.message_key]
    store.close()


def test_缺失旧trigger时整批消息不会永久悬挂(tmp_path):
    """旧 batch 的上下文和 anchor 都要能在恢复后继续按序处理。"""
    store = QueueStore(tmp_path / "queue-missing-trigger-batch.sqlite3")
    context = _message("missing-context")
    anchor = _message("missing-anchor")
    store.enqueue(context)
    store.enqueue(anchor, _trigger(anchor))
    lease = store.claim("888")
    assert lease is not None
    assert [message.message_key for message in lease.messages] == [
        context.message_key,
        anchor.message_key,
    ]
    store._conn.execute(
        "DELETE FROM onebot_queue_trigger WHERE message_key=?",
        (anchor.message_key,),
    )
    store._conn.commit()

    assert store.abandon_owner_leases() == {"pending": 1, "uncertain": 0}
    first_recovery = store.claim("888")
    assert first_recovery is not None
    assert [message.message_key for message in first_recovery.messages] == [
        context.message_key
    ]
    assert store.ack(first_recovery)
    second_recovery = store.claim("888")
    assert second_recovery is not None
    assert [message.message_key for message in second_recovery.messages] == [
        anchor.message_key
    ]
    store.close()


def test_resolve_retry与已有pending_trigger合并(tmp_path):
    """管理员 retry uncertain 消息时，旧 trigger 与新 trigger 必须合并。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = _message("merge-resolve")
    store.enqueue(first, _trigger(first))
    lease = store.claim("888")
    assert lease is not None
    assert store.mark_uncertain(lease, "unknown")
    later = _message("merge-resolve-later")
    store.enqueue(later, _trigger(later))

    assert store.resolve_uncertain("888", "retry") == 1
    assert store.status("888")["pending_trigger_requests"] == 2
    store.close()


def test_hard_trigger覆盖旧恢复请求并清除退避(tmp_path):
    """新的 @/关键词/flush 必须 retarget 最新消息并立即解除退避。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = _message("hard-first")
    second = _message("hard-second")
    store.enqueue(first)
    store.enqueue(second)
    store.create_trigger("888", "queue_recovery", "old-user", "旧用户", first.message_key)
    store._conn.execute(
        "UPDATE onebot_queue_message SET next_attempt_at=999999 WHERE message_key=?",
        (second.message_key,),
    )
    store._conn.commit()

    store.create_trigger("888", "mention", "new-user", "新用户", second.message_key)

    trigger = store._conn.execute(
        """
        SELECT message_key, reason, caller_user_id, caller_user_name
        FROM onebot_queue_trigger
        WHERE chat_id=? AND message_key=?
        """,
        ("888", second.message_key),
    ).fetchone()
    assert tuple(trigger) == ("group:hard-second", "mention", "new-user", "新用户")
    assert store._conn.execute(
        "SELECT next_attempt_at FROM onebot_queue_message WHERE message_key=?",
        (second.message_key,),
    ).fetchone()[0] is None
    store.close()


def test_hard_trigger覆盖selector时更新anchor_kind(tmp_path):
    """硬触发重新命中同一消息时，持久化类型不能继续伪装成 selector。"""
    store = QueueStore(tmp_path / "queue-anchor-kind.sqlite3")
    message = _message("anchor-kind")
    store.enqueue(message)
    selector_id = store.create_trigger(
        "888",
        "llm",
        message.user_id,
        message.user_name,
        message.message_key,
        anchor_kind="selector",
    )
    assert selector_id is not None

    assert (
        store.create_trigger(
            "888",
            "mention",
            "new-user",
            "新用户",
            message.message_key,
            anchor_kind="hard",
        )
        == selector_id
    )
    row = store._conn.execute(
        "SELECT reason, caller_user_id, anchor_kind FROM onebot_queue_trigger WHERE request_id=?",
        (selector_id,),
    ).fetchone()
    assert tuple(row) == ("mention", "new-user", "hard")
    store.close()


def test_turn_anchor固定当前batch边界():
    """一个 anchor 只能消费它之前的消息，后续消息留给下一轮。"""
    store = QueueStore(":memory:")
    first = _message("anchor-first", text="第一条")
    second = _message("anchor-second", text="第二条")
    store.enqueue(first, _trigger(first))
    store.enqueue(second, _trigger(second))

    lease = store.claim("888", lease_seconds=60)

    assert lease is not None
    assert [message.message_key for message in lease.messages] == ["group:anchor-first"]
    assert lease.trigger.anchor_seq == 1
    assert lease.trigger.anchor_kind == "message"
    assert store.ack(lease)

    next_lease = store.claim("888", lease_seconds=60)
    assert next_lease is not None
    assert [message.message_key for message in next_lease.messages] == ["group:anchor-second"]
    assert next_lease.trigger.anchor_seq == 2
    store.close()


def test_unknown_phase的release和恢复都进入uncertain(tmp_path, monkeypatch):
    """未知 phase 不能被当成无副作用失败自动重放。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("unknown-phase")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888", lease_seconds=5)
    assert lease is not None
    store._conn.execute(
        "UPDATE onebot_queue_message SET lease_phase='mystery' WHERE lease_id=?",
        (lease.lease_id,),
    )
    store._conn.commit()
    assert store.release(lease, reason="cannot classify") is True
    assert store.status("888")["uncertain"] == 1
    store.close()

    reopened = QueueStore(tmp_path / "queue-recovery.sqlite3")
    recovery_message = _message("unknown-recovery")
    reopened.enqueue(recovery_message, _trigger(recovery_message))
    recovery_lease = reopened.claim("888", lease_seconds=5)
    assert recovery_lease is not None
    reopened._conn.execute(
        "UPDATE onebot_queue_message SET lease_phase='mystery' WHERE lease_id=?",
        (recovery_lease.lease_id,),
    )
    reopened._conn.commit()
    now[0] = 1006.0
    assert reopened.recover_trigger_requests() == ()
    assert reopened.status("888")["uncertain"] == 1
    reopened.close()


def test_resolve_retry没有旧trigger时补建恢复入口(tmp_path):
    """旧文件只有 uncertain 消息时，人工 retry 仍能恢复 durable trigger。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("retry-without-trigger")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    assert store.mark_uncertain(lease, "unknown")
    store._conn.execute("DELETE FROM onebot_queue_trigger WHERE chat_id=?", ("888",))
    store._conn.commit()
    assert store.resolve_uncertain("888", "retry") == 1
    assert store.status("888")["pending_trigger_requests"] == 1
    store.close()


def test_resolve_retry只清理被阻塞anchor(tmp_path):
    """恢复一个 blocked anchor 时不能抹掉同群其他 pending anchor。"""
    store = QueueStore(tmp_path / "queue-resolve-scope.sqlite3")
    blocked = _message("blocked")
    later = _message("later")
    store.enqueue(blocked, _trigger(blocked))
    lease = store.claim("888")
    assert lease is not None
    assert store.mark_uncertain(lease, "unknown")
    store._conn.execute(
        "DELETE FROM onebot_queue_trigger WHERE message_key=?",
        (blocked.message_key,),
    )
    store._conn.commit()
    store.enqueue(later, _trigger(later))

    assert store.resolve_uncertain("888", "retry") == 1
    blocked_anchor = store._conn.execute(
        "SELECT anchor_id FROM onebot_queue_message WHERE message_key=?",
        (blocked.message_key,),
    ).fetchone()[0]
    later_anchor = store._conn.execute(
        "SELECT anchor_id FROM onebot_queue_message WHERE message_key=?",
        (later.message_key,),
    ).fetchone()[0]
    assert blocked_anchor is not None
    assert later_anchor is not None
    assert blocked_anchor != later_anchor
    store.close()


def test_缺失旧trigger时恢复anchor不能吸收后续消息(tmp_path):
    """旧 trigger 丢失后，恢复入口必须保留原来的 batch 边界。"""
    store = QueueStore(tmp_path / "queue-resolve-order.sqlite3")
    blocked = _message("blocked-order")
    later = _message("later-order")
    store.enqueue(blocked, _trigger(blocked))
    lease = store.claim("888")
    assert lease is not None
    assert store.mark_uncertain(lease, "unknown")
    store._conn.execute(
        "DELETE FROM onebot_queue_trigger WHERE message_key=?",
        (blocked.message_key,),
    )
    store._conn.commit()
    store.enqueue(later, _trigger(later))

    assert store.resolve_uncertain("888", "retry") == 1
    recovered = store.claim("888")

    assert recovered is not None
    assert recovered.trigger.reason == "queue_recovery"
    assert [message.message_key for message in recovered.messages] == [blocked.message_key]
    store.close()


def test_显式anchor消息消失时不静默改绑到最早消息(tmp_path):
    """selector 的旧判断失效时必须丢弃结果，不能把权限锚点换成另一条消息。"""
    store = QueueStore(tmp_path / "queue-anchor-race.sqlite3")
    first = _message("anchor-race-first")
    selected = _message("anchor-race-selected")
    store.enqueue(first)
    store.enqueue(selected)
    store._conn.execute(
        "DELETE FROM onebot_queue_message WHERE message_key=?",
        (selected.message_key,),
    )
    store._conn.commit()
    assert (
        store.create_trigger(
            "888",
            "llm",
            selected.user_id,
            selected.user_name,
            selected.message_key,
            anchor_kind="selector",
        )
        is None
    )
    assert store.status("888")["pending_trigger_requests"] == 0
    assert store.peek("888")[0].message_key == first.message_key
    store.close()
