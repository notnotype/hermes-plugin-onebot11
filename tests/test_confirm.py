"""群管理确认令牌测试。"""

from onebot11.confirm import ConfirmationStore


def test_确认令牌同目标单次消费():
    """错误用户不能消费令牌，正确用户消费后不能再次使用。"""
    store = ConfirmationStore(ttl_seconds=60)
    confirmation = store.issue(
        "qq_set_group_ban",
        {"user_id": "123", "duration": 60},
        user_id="10001",
        chat_type="group",
        chat_id="888",
    )
    assert store.consume(confirmation.token, user_id="10002", chat_type="group", chat_id="888") is None
    consumed = store.consume(confirmation.token, user_id="10001", chat_type="group", chat_id="888")
    assert consumed is not None
    assert store.consume(confirmation.token, user_id="10001", chat_type="group", chat_id="888") is None
