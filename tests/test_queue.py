"""SQLite 队列的租约、恢复和去重合同测试。"""

import sqlite3

from onebot11.queue import QueueMessage, QueueStore, TriggerRequest


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
    """同一批消息的多个 trigger 必须随 lease 一起完成。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    for message_id in ("1", "2", "3"):
        message = _message(message_id)
        store.enqueue(message, _trigger(message))

    lease = store.claim("888")
    assert lease is not None
    assert [message.message_id for message in lease.messages] == ["1", "2", "3"]
    assert store.status("888")["trigger_requests"] == 0
    assert store.ack(lease)
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
    assert store.release(lease)
    assert store.status("888")["pending"] == 1
    assert store.status("888")["failure_count"] == 1
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
