
"""
pytest -q
"""
import json
import time
import threading
from typing import Dict
import fakeredis
import pytest
from redis_ext import RedisWork

@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)

def test_basic_flow(fake_redis: fakeredis.FakeRedis) -> None:
    """端到端：push -> 消费 -> 验证结果"""
    lst_key = "test:queue"
    result_box: list[int] = []


    def handler(task: Dict) -> None:
        result_box.append(task["x"])

        rw = RedisWork(lst_key, max_workers=2, redis_client=fake_redis)
        rw.register_handler(handler)

        # 后台启动监听线程
        t = threading.Thread(target=rw.start, daemon=True)
        t.start()
        time.sleep(0.1)  # 让监听线程跑起来

        # 推送 10 个任务
        for i in range(10):
            fake_redis.rpush(lst_key, json.dumps({"x": i}))

        # 等待全部任务被消费（最多 5 秒）
        for _ in range(50):
            if len(result_box) == 10:
                break
            time.sleep(0.1)

        rw.shutdown()
        assert len(result_box) == 10
        assert result_box == list(range(10))


def test_max_workers(fake_redis: fakeredis.FakeRedis) -> None:
    """验证并发度不会超过 max_workers"""
    lst_key = "test:queue2"
    running = 0
    max_running = 0

    def handler(_: Dict) -> None:
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        time.sleep(0.2)
        running -= 1

    rw = RedisWork(lst_key, max_workers=3, redis_client=fake_redis)
    rw.register_handler(handler)

    t = threading.Thread(target=rw.start, daemon=True)
    t.start()
    time.sleep(0.1)

    # 一次推 6 个任务
    for _ in range(6):
        fake_redis.rpush(lst_key, json.dumps({}))

    time.sleep(0.5)
    rw.shutdown()
    assert max_running <= 3


def test_stop_on_error(fake_redis: fakeredis.FakeRedis) -> None:
    """任务抛错后应停止监听，并把已提交任务跑完"""
    lst_key = "test:err"
    processed: list[int] = []

    def handler(task: Dict) -> None:
        processed.append(task["x"])
        if task["x"] == 3:
            raise ValueError("intentional")

    rw = RedisWork(lst_key, max_workers=2, redis_client=fake_redis)
    rw.register_handler(handler)

    # 推 5 条任务
    for i in range(5):
        fake_redis.rpush(lst_key, json.dumps({"x": i}))

    # 启动监听，会抛 ValueError
    with pytest.raises(ValueError, match="intentional"):
        rw.start()

    # 已提交任务仍被消费完
    assert fake_redis.llen(lst_key) == 0
    assert 3 in processed  # 出错任务也至少被尝试了一次
