"""SQLite 队列的租约、恢复和去重合同测试。"""

import sqlite3
import threading
import time

import pytest

from onebot11.queue import QueueBusy, QueueError, QueueMessage, QueueStore, TriggerRequest


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


def _replace_reaction_table_with_v8(
    store: QueueStore,
    *,
    lease_id: str,
    state: str,
    created_at: float,
    updated_at: float,
    attempts: int,
    next_attempt_at: float | None,
    last_error: str | None,
) -> None:
    """把测试数据库中的 reaction 表精确还原为 schema v8。"""
    store._conn.execute("DROP INDEX IF EXISTS idx_onebot_queue_reaction_cleanup")
    store._conn.execute("DROP TABLE onebot_queue_reaction")
    store._conn.execute(
        """
        CREATE TABLE onebot_queue_reaction (
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
    store._conn.execute(
        """
        CREATE INDEX idx_onebot_queue_reaction_cleanup
        ON onebot_queue_reaction(state, next_attempt_at, updated_at)
        """
    )
    store._conn.execute(
        """
        INSERT INTO onebot_queue_reaction(
            lease_id,chat_id,message_id,state,created_at,updated_at,
            attempts,next_attempt_at,last_error
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            lease_id,
            "888",
            "reaction-message",
            state,
            created_at,
            updated_at,
            attempts,
            next_attempt_at,
            last_error,
        ),
    )
    store._conn.commit()


def test_每个锚点独立认领消息范围(tmp_path):
    """两个用户的精确触发必须形成两个独立 lease 和权限边界。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    for message_id in ("1", "2", "3"):
        message = _message(message_id)
        store.enqueue(message, _trigger(message))

    first = store.claim("888")
    assert first is not None
    assert [message.message_id for message in first.messages] == ["1"]
    assert first.trigger.anchor_seq == 1
    assert store.status("888")["pending_trigger_requests"] == 2
    assert store.ack(first)

    second = store.claim("888")
    assert second is not None
    assert [message.message_id for message in second.messages] == ["2"]
    assert second.trigger.anchor_seq == 2
    assert store.ack(second)

    third = store.claim("888")
    assert third is not None
    assert [message.message_id for message in third.messages] == ["3"]
    assert third.trigger.anchor_seq == 3
    assert store.ack(third)
    assert store.peek("888") == ()
    assert store.recover_trigger_requests() == ()
    store.close()


def test_普通消息只归入下一个锚点且后续消息不越界(tmp_path):
    """anchor batch 只覆盖上个完成边界到当前 anchor，不吞掉未来消息。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    ordinary = _message("1", text="普通上下文")
    anchor_a = _message("2", text="A 的请求")
    between = _message("3", text="后续上下文")
    anchor_b = _message("4", text="B 的请求")
    future = _message("5", text="未来消息")
    store.enqueue(ordinary)
    store.enqueue(anchor_a, _trigger(anchor_a))
    store.enqueue(between)
    store.enqueue(anchor_b, _trigger(anchor_b))
    store.enqueue(future)

    first = store.claim("888")
    assert first is not None
    assert [message.message_id for message in first.messages] == ["1", "2"]
    assert store.ack(first)

    second = store.claim("888")
    assert second is not None
    assert [message.message_id for message in second.messages] == ["3", "4"]
    assert store.ack(second)
    assert [message.message_id for message in store.peek("888")] == ["5"]
    store.close()


def test_blocking锚点阻止后续锚点越过(tmp_path):
    """最早 anchor 进入 uncertain 后，后续 anchor 不能执行。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    for message_id in ("1", "2"):
        message = _message(message_id)
        store.enqueue(message, _trigger(message))
    first = store.claim("888")
    assert first is not None
    assert store.mark_uncertain(first, "出站未知")
    assert store.claim("888") is None
    assert store.status("888")["blocked_trigger_requests"] == 1
    store.close()


def test_retry只恢复最早锚点原消息范围(tmp_path):
    """管理员 retry 不得把后来的 pending 消息扩大进旧 anchor。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    first_message = _message("1")
    second_message = _message("2")
    store.enqueue(first_message, _trigger(first_message))
    lease = store.claim("888")
    assert lease is not None
    assert store.mark_uncertain(lease, "未知")
    store.enqueue(second_message, _trigger(second_message))

    assert store.resolve_uncertain("888", "retry") == 1
    retried = store.claim("888")
    assert retried is not None
    assert retried.trigger.request_id != lease.trigger.request_id
    assert retried.trigger.caller_user_id == lease.trigger.caller_user_id
    assert [message.message_id for message in retried.messages] == ["1"]
    store.close()


def test_过期agent_lease恢复使用退避并在第三次进入failed(tmp_path):
    """崩溃恢复不能绕过自动失败上限，避免恢复风暴。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("crash")
    store.enqueue(message, _trigger(message))
    for expected_failure_count in (1, 2, 3):
        lease = store.claim("888")
        if lease is None:
            store._conn.execute(
                "UPDATE onebot_queue_trigger SET next_attempt_at=0 WHERE chat_id=?",
                ("888",),
            )
            store._conn.execute(
                "UPDATE onebot_queue_message SET next_attempt_at=0 WHERE chat_id=?",
                ("888",),
            )
            store._conn.commit()
            lease = store.claim("888")
        assert lease is not None
        store._conn.execute(
            "UPDATE onebot_queue_message SET lease_until=0 WHERE lease_id=?",
            (lease.lease_id,),
        )
        store._conn.commit()
        store.close()
        store = QueueStore(path)
        store.recover_trigger_requests({"888"})
        assert store.claim("888") is None
        status = store.status("888")
        assert status["failure_count"] == expected_failure_count
        if expected_failure_count < 3:
            assert status["pending"] == 1
            assert status["next_retry_at"] is not None
        else:
            assert status["failed"] == 1
            assert status["pending_trigger_requests"] == 0
    store.close()


def test_lease_phase未知即使marker为零也进入uncertain(tmp_path):
    """state/phase 不一致时按更保守的出站未知处理。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("phase-unknown")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    store._conn.execute(
        """
        UPDATE onebot_queue_message
        SET lease_until=0,lease_phase='unexpected',outbound_started=0
        WHERE lease_id=?
        """,
        (lease.lease_id,),
    )
    store._conn.commit()
    store.close()
    recovered = QueueStore(path)
    recovered.recover_trigger_requests({"888"})
    status = recovered.status("888")
    assert status["uncertain"] == 1
    assert status["pending"] == 0
    recovered.close()


def test_当前lease_phase未知时成功结果也不能ack(tmp_path):
    """活动 lease 的阶段字段损坏时，ack 必须先转 uncertain。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("phase-active")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    store._conn.execute(
        "UPDATE onebot_queue_message SET lease_phase='unexpected' WHERE lease_id=?",
        (lease.lease_id,),
    )
    store._conn.commit()
    assert not store.ack(lease)
    status = store.status("888")
    assert status["uncertain"] == 1
    assert status["pending"] == 0
    store.close()


def test_自动锚点窗口不重新选择已有更晚精确锚点之前的消息(tmp_path):
    """精确锚点已排队时，自动选择器不能再插入其前方并改写执行顺序。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    ordinary = _message("1", text="更早的群聊上下文")
    explicit = _message("2", text="@bot 明确任务")
    later = _message("3", text="之后的新请求？")
    store.enqueue(ordinary)
    explicit_result = store.enqueue(explicit, _trigger(explicit))
    store.enqueue(later)

    assert explicit_result.trigger_request_id is not None
    assert [message.message_id for message in store.peek_unanchored("888")] == ["3"]
    assert store.create_message_anchor("888", 3, "automatic") is not None
    assert [anchor.anchor_seq for anchor in store.list_anchors("888")] == [2, 3]
    store.close()


def test_operator_anchor固定命令时边界和管理员authority(tmp_path):
    """flush 只覆盖命令前未锚定消息，后来消息留给下一 anchor。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    store.enqueue(_message("1"))
    anchor_id = store.create_operator_anchor(
        "888",
        "admin_flush",
        "999",
        "管理员",
        control_message_id="9000",
    )
    assert anchor_id is not None
    store.enqueue(_message("2"))
    lease = store.claim("888")
    assert lease is not None
    assert [message.message_id for message in lease.messages] == ["1"]
    assert lease.anchor.anchor_kind == "operator"
    assert lease.anchor.caller_user_id == "999"
    assert lease.anchor.control_message_id == "9000"
    assert store.ack(lease)
    assert [message.message_id for message in store.peek_unanchored("888")] == ["2"]
    store.close()


def test_ack不再追加跨轮滚动摘要(tmp_path):
    """成功 batch 由 Hermes session 历史承载，SQLite 不重复保存并注入。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = _message("1", text="第一轮")
    store.enqueue(first, _trigger(first))
    lease = store.claim("888")
    assert lease is not None
    assert lease.summary == ""
    assert store.ack(lease)
    assert store.status("888")["summary"] == ""
    second = _message("2", text="第二轮")
    store.enqueue(second, _trigger(second))
    next_lease = store.claim("888")
    assert next_lease is not None
    assert "第一轮" not in next_lease.summary
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
    recovered = other.recover_trigger_requests()
    assert len(recovered) == 1
    assert recovered[0].request_id == lease.trigger.request_id
    now[0] = 1033.0
    assert other.claim("888") is not None
    owner.close()
    other.close()


def test_恢复白名单在修改lease前过滤目标(tmp_path, monkeypatch):
    """允许恢复的群转 pending，未授权群的 lease 和 trigger 保持不变。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    owner = QueueStore(path)
    recovery = QueueStore(path)
    for chat_id in ("888", "777"):
        message = _message(chat_id, chat_id=chat_id)
        owner.enqueue(message, _trigger(message))
        assert owner.claim(chat_id, lease_seconds=5) is not None
    now[0] = 1006.0

    recovered = recovery.recover_trigger_requests({"888"})
    assert [item.chat_id for item in recovered] == ["888"]
    assert recovery.status("888")["pending"] == 1
    assert recovery.status("888")["pending_trigger_requests"] == 1
    assert recovery.status("777")["leased"] == 1
    assert recovery.status("777")["pending_trigger_requests"] == 0
    owner.close()
    recovery.close()


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
    assert store.release(lease)
    assert store.status("888")["pending"] == 1
    assert store.status("888")["failure_count"] == 1
    assert store.claim("888") is None
    monkeypatch.setattr("onebot11.queue.time.time", lambda: lease.claimed_at + 2.1)
    retried = store.claim("888")
    assert retried is not None
    store.close()


def test_status分开显示failure和uncertain原因(tmp_path):
    """管理员 status 不能把可重试失败和未知结果混成同一类。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("reasons")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888")
    assert lease is not None
    assert store.release(lease, reason="网络明确失败")
    status = store.status("888")
    assert status["failure_reasons"] == ["网络明确失败"]
    assert status["uncertain_reasons"] == []
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


def test_v5旧batch升级到v9进入legacy_hold(tmp_path, monkeypatch):
    """旧 batch 无法重建单锚点权限时必须保守 hold，不能自动续跑。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("v5")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888", lease_seconds=30)
    assert lease is not None
    store.close()
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=5")
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    assert migrated.status("888")["leased"] == 0
    assert migrated.status("888")["uncertain"] == 1
    assert migrated.status("888")["blocked_trigger_requests"] == 1
    migrated.close()


def test_v8迁移只隔离阻塞消息并保留后续精确锚点(tmp_path):
    """旧 blocked batch 不能把同群后来 pending 消息一起升级为 uncertain。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    blocked_message = _message("blocked")
    store.enqueue(blocked_message, _trigger(blocked_message))
    assert store.claim("888", lease_seconds=60) is not None
    store._conn.execute(
        "UPDATE onebot_queue_message SET lease_until=0 WHERE message_key=?",
        (blocked_message.message_key,),
    )
    store._conn.commit()
    pending_message = _message("pending")
    store.enqueue(pending_message, _trigger(pending_message))
    store.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=8")
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    status = migrated.status("888")
    assert status["uncertain"] == 1
    assert status["pending"] == 1
    assert status["pending_trigger_requests"] == 1
    assert any(anchor.anchor_kind == "legacy" for anchor in migrated.list_anchors("888"))
    migrated.close()


def test_v8有效lease拒绝迁移并保留旧reaction原状态(tmp_path):
    """有效旧 lease 必须阻断升级，reaction 表和原状态随事务完整保留。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("active-v8")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888", lease_seconds=3600)
    assert lease is not None
    _replace_reaction_table_with_v8(
        store,
        lease_id=lease.lease_id,
        state="pending",
        created_at=100.0,
        updated_at=200.0,
        attempts=1,
        next_attempt_at=300.0,
        last_error="旧记录仍等待清理",
    )
    store.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=8")
    connection.commit()
    connection.close()

    with pytest.raises(QueueBusy, match="仍有效的 v8 lease"):
        QueueStore(path)
    connection = sqlite3.connect(path)
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 8
    columns = {
        str(row[1])
        for row in connection.execute(
            "PRAGMA table_info(onebot_queue_reaction)"
        ).fetchall()
    }
    assert "reaction_kind" not in columns
    assert connection.execute(
        """
        SELECT lease_id,chat_id,message_id,state,created_at,updated_at,
               attempts,next_attempt_at,last_error
        FROM onebot_queue_reaction
        """
    ).fetchone() == (
        lease.lease_id,
        "888",
        "reaction-message",
        "pending",
        100.0,
        200.0,
        1,
        300.0,
        "旧记录仍等待清理",
    )
    connection.close()


def test_v8过期lease的reaction迁移为legacy_processing且只恢复unset(
    tmp_path,
    monkeypatch,
):
    """过期旧 reaction 保留状态字段，只进入 legacy unset 恢复路径。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("expired-v8")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888", lease_seconds=5)
    assert lease is not None
    _replace_reaction_table_with_v8(
        store,
        lease_id=lease.lease_id,
        state="maybe_set",
        created_at=900.0,
        updated_at=990.0,
        attempts=2,
        next_attempt_at=1005.0,
        last_error="旧 unset 超时",
    )
    store.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=8")
    connection.commit()
    connection.close()
    now[0] = 1006.0

    migrated = QueueStore(path)
    legacy = migrated.reaction(
        lease.lease_id,
        reaction_kind="legacy_processing",
    )
    assert legacy is not None
    assert legacy.anchor_id == f"legacy-reaction-{lease.lease_id}"
    assert legacy.reaction_kind == "legacy_processing"
    assert legacy.lease_id == lease.lease_id
    assert legacy.chat_id == "888"
    assert legacy.message_id == "reaction-message"
    assert legacy.state == "maybe_set"
    assert legacy.created_at == 900.0
    assert legacy.updated_at == 990.0
    assert legacy.attempts == 2
    assert legacy.next_attempt_at == 1005.0
    assert legacy.last_error == "旧 unset 超时"
    assert legacy.emoji_id == ""

    assert migrated.reaction(lease.lease_id, reaction_kind="queued") is None
    assert migrated.reaction(lease.lease_id, reaction_kind="processing") is None
    assert not migrated.mark_reaction_set(lease.lease_id)
    assert migrated.pending_reaction_cleanups(now=now[0]) == (legacy,)
    migrated.close()


def test_v8_pending_llm锚点迁移为未锚定消息(tmp_path):
    """旧 LLM trigger 没有可验证 authority，迁移时删除 trigger 但保留消息。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("old-llm")
    store.enqueue(message, _trigger(message, reason="llm"))
    store._conn.execute(
        "UPDATE onebot_queue_message SET anchor_id=NULL WHERE message_key=?",
        (message.message_key,),
    )
    store._conn.execute(
        """
        UPDATE onebot_queue_trigger
        SET anchor_seq=NULL, anchor_kind='message'
        WHERE message_key=?
        """,
        (message.message_key,),
    )
    store._conn.commit()
    store.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=8")
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    assert migrated.list_anchors("888") == ()
    assert [item.message_id for item in migrated.peek_unanchored("888")] == ["old-llm"]
    migrated.close()


def test_v8孤儿trigger迁移为legacy_hold(tmp_path):
    """找不到对应消息的旧 trigger 不能借用其他消息的 authority。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    store.enqueue(_message("ordinary"))
    now = time.time()
    store._conn.execute(
        """
        INSERT INTO onebot_queue_trigger(
            request_id,chat_id,message_key,reason,caller_user_id,caller_user_name,
            status,anchor_seq,anchor_kind,created_at,updated_at
        ) VALUES (?,?,?,?,?,?, 'pending', NULL, 'message', ?, ?)
        """,
        ("orphan", "888", "group:missing", "mention", "123", "小明", now, now),
    )
    store._conn.commit()
    store.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=8")
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    anchors = migrated.list_anchors("888")
    assert len(anchors) == 1
    assert anchors[0].anchor_kind == "legacy"
    assert anchors[0].status == "uncertain"
    assert migrated.status("888")["pending"] == 1
    migrated.close()


def test_v7旧格式保留revision并补齐当前扩展(tmp_path):
    """已部署过的 schema 7 只能增量补齐，不能丢掉 revision 或拒绝启动。"""
    path = tmp_path / "queue.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE onebot_queue_chat (
            chat_id TEXT PRIMARY KEY,
            next_seq INTEGER NOT NULL DEFAULT 1,
            summary TEXT NOT NULL DEFAULT '',
            paused INTEGER NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0
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
            message_id TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL,
            seq INTEGER NOT NULL,
            byte_size INTEGER NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','leased','uncertain','failed')),
            lease_id TEXT,
            lease_until REAL,
            lease_owner TEXT,
            lease_phase TEXT NOT NULL DEFAULT 'agent_running',
            outbound_started INTEGER NOT NULL DEFAULT 0,
            uncertain_reason TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            failure_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL,
            failure_reason TEXT,
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
            lease_owner TEXT,
            uncertain_reason TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(chat_id, message_key)
        );
        INSERT INTO onebot_queue_chat(chat_id, next_seq, revision, updated_at)
        VALUES ('888', 2, 41, 1000);
        INSERT INTO onebot_queue_message(
            chat_id, message_key, chat_type, user_id, user_name, text, raw_text,
            metadata_json, seq, byte_size, state, created_at, updated_at
        ) VALUES (
            '888', 'group:legacy', 'group', '123', '小明', '旧消息', '旧消息',
            '{}', 1, 8, 'pending', 1000, 1000
        );
        INSERT INTO onebot_queue_trigger(
            request_id, chat_id, message_key, reason, caller_user_id,
            caller_user_name, status, created_at, updated_at
        ) VALUES (
            'trigger-legacy', '888', 'group:legacy', 'mention', '123',
            '小明', 'pending', 1000, 1000
        );
        PRAGMA user_version=7;
        """
    )
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    chat_columns = {
        str(row[1])
        for row in migrated._conn.execute(
            "PRAGMA table_info(onebot_queue_chat)"
        ).fetchall()
    }
    assert {"revision", "last_trigger_at"}.issubset(chat_columns)
    assert migrated._conn.execute(
        "SELECT revision FROM onebot_queue_chat WHERE chat_id='888'"
    ).fetchone()[0] == 41
    assert migrated._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='onebot_queue_reaction'"
    ).fetchone() is not None
    assert migrated.status("888")["pending"] == 1

    assert migrated.enqueue(_message("new")).inserted
    assert migrated._conn.execute(
        "SELECT revision FROM onebot_queue_chat WHERE chat_id='888'"
    ).fetchone()[0] == 42
    migrated.close()


def test_v7迁移补齐LLM判断列(tmp_path):
    """schema 7 文件升级后应具备持久 LLM 游标和退避列。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    store.close()
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=7")
    connection.commit()
    connection.close()

    migrated = QueueStore(path)
    columns = {
        str(row[1])
        for row in migrated._conn.execute(
            "PRAGMA table_info(onebot_queue_chat)"
        ).fetchall()
    }
    assert {
        "llm_judged_seq",
        "llm_next_attempt_at",
        "llm_failure_count",
        "llm_last_error",
    }.issubset(columns)
    migrated.close()


def test_llm判断游标和退避持久化(tmp_path, monkeypatch):
    """LLM false 不重复判断；失败有退避且下一次重启仍可读取。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    store.enqueue(_message("llm"))
    assert store.llm_judgment("888")["judged_seq"] == 0
    store.mark_llm_judged("888", 1)
    assert store.llm_judgment("888") == {
        "judged_seq": 1,
        "next_attempt_at": None,
        "failure_count": 0,
        "last_error": None,
    }
    store.mark_llm_failure("888", 1, "timeout")
    state = store.llm_judgment("888")
    assert state["judged_seq"] == 1
    assert state["failure_count"] == 1
    assert state["next_attempt_at"] == 1002.0
    store.close()

    reopened = QueueStore(path)
    assert reopened.llm_judgment("888")["next_attempt_at"] == 1002.0
    reopened.close()


def test关闭等待已进入SQLite操作且关闭后拒绝新操作(tmp_path):
    """close 不得抢先关闭正在执行的同步操作，也不得让新操作碰到 sqlite。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocking_operation() -> None:
        with store._operation():
            entered.set()
            release.wait(timeout=2)

    worker = threading.Thread(target=blocking_operation)
    worker.start()
    assert entered.wait(timeout=1)
    closer = threading.Thread(target=lambda: (store.close(), finished.set()))
    closer.start()
    time.sleep(0.05)
    assert not finished.is_set()
    release.set()
    worker.join(timeout=1)
    closer.join(timeout=1)
    assert finished.is_set()
    with pytest.raises(QueueError, match="QueueStore 已关闭"):
        store.peek("888")


def test_未知更高schema仍然拒绝启动(tmp_path):
    """schema 10 及以上不能被当前插件猜测迁移。"""
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    store.close()
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=10")
    connection.commit()
    connection.close()

    try:
        QueueStore(path)
    except Exception as exc:
        assert "高于支持版本" in str(exc)
    else:
        raise AssertionError("未知更高 schema 未被拒绝")


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


def test_reaction在活动lease期间不恢复_失效后可恢复(tmp_path, monkeypatch):
    """reaction 只保存可能已设置成功的状态，不重放 set。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("reaction")
    store.enqueue(message, _trigger(message))
    lease = store.claim("888", lease_seconds=5)
    assert lease is not None
    store.record_reaction(lease.lease_id, "888", "123")
    assert store.mark_reaction_set(lease.lease_id)
    assert store.pending_reaction_cleanups() == ()
    assert store.mark_uncertain(lease, "处理取消")
    now[0] = 1006.0
    pending = store.pending_reaction_cleanups()
    assert len(pending) == 1
    assert pending[0].message_id == "123"
    assert store.delete_reaction(lease.lease_id)
    store.close()


def test_queued与processing_reaction按anchor独立保存(tmp_path, monkeypatch):
    """⏳ 和 👀 共享 anchor，但清理状态互不覆盖且 set 不会被恢复重放。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("two-reactions")
    result = store.enqueue(message, _trigger(message))
    assert result.trigger_request_id is not None
    anchor_id = result.trigger_request_id
    store.record_reaction(
        "",
        "888",
        "1001",
        anchor_id=anchor_id,
        reaction_kind="queued",
        emoji_id="hourglass",
    )
    assert store.mark_reaction_set(anchor_id, reaction_kind="queued")
    assert store.pending_reaction_cleanups() == ()

    lease = store.claim("888", lease_seconds=5)
    assert lease is not None
    store.record_reaction(
        lease.lease_id,
        "888",
        "1001",
        anchor_id=anchor_id,
        reaction_kind="processing",
        emoji_id="eyes",
    )
    assert store.mark_reaction_set(anchor_id, reaction_kind="processing")
    assert store.delete_reaction(anchor_id, reaction_kind="queued")
    assert store.reaction(anchor_id, reaction_kind="processing") is not None
    assert store.mark_uncertain(lease, "unknown")
    now[0] = 1006.0
    cleanup = store.pending_reaction_cleanups()
    assert [(item.anchor_id, item.reaction_kind) for item in cleanup] == [
        (anchor_id, "processing")
    ]
    store.close()


def test_reaction同目标只保留一条且maybe_set退避不被重置(tmp_path, monkeypatch):
    """retry/重复事件不能制造重复 UI，也不能让 unset 失败重新立即执行。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    store = QueueStore(tmp_path / "queue.sqlite3")
    store.record_reaction(
        "lease-1",
        "888",
        "1001",
        anchor_id="anchor-1",
        reaction_kind="processing",
        emoji_id="eyes",
    )
    assert store.mark_reaction_set("anchor-1", reaction_kind="processing")
    assert store.mark_reaction_cleanup_failed(
        "anchor-1",
        "first unset failed",
        reaction_kind="processing",
    )
    before = store.reaction("anchor-1", reaction_kind="processing")
    assert before is not None
    store.record_reaction(
        "lease-2",
        "888",
        "1001",
        anchor_id="anchor-2",
        reaction_kind="processing",
        emoji_id="eyes",
    )
    after = store.reaction("anchor-2", reaction_kind="processing")
    assert after is not None
    assert store.reaction("anchor-1", reaction_kind="processing") is None
    assert after.state == "maybe_set"
    assert after.attempts == before.attempts
    assert after.next_attempt_at == before.next_attempt_at
    store.close()


def test_status_for_lease聚合整个batch的出站marker(tmp_path):
    """多消息 batch 不能只看第一条消息判断是否已开始出站。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1001",
        user_id="123",
        user_name="小明",
        text="第一条",
        message_key="group:1001",
    )
    second = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="1002",
        user_id="456",
        user_name="小红",
        text="第二条",
        message_key="group:1002",
    )
    trigger = TriggerRequest.create("888", "group:1002", "mention", "456", "小红")
    store.enqueue(first)
    store.enqueue(second, trigger)
    lease = store.claim("888")
    assert lease is not None

    # 模拟旧进程在多行 lease 上留下了部分阶段 marker。
    with store._lock:
        store._conn.execute(
            """
            UPDATE onebot_queue_message
            SET lease_phase='outbound_started', outbound_started=1
            WHERE lease_id=? AND message_key=?
            """,
            (lease.lease_id, "group:1002"),
        )
        store._conn.commit()

    status = store.status_for_lease(lease.lease_id)
    assert status["lease_phase"] == "outbound_started"
    assert status["outbound_started"] is True
    queue_status = store.status("888")
    assert queue_status["lease_phase"] == "outbound_started"
    assert queue_status["outbound_started"] is True
    store.close()
