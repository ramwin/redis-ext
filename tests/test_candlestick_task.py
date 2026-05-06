# SPDX-FileCopyrightText: 2025-present Xiang Wang <ramwin@qq.com>
#
# SPDX-License-Identifier: MIT

import time
import threading
from typing import Generator
import pytest
import fakeredis

from redis_ext.candlestick_task import CandleStickTask


@pytest.fixture
def fake_redis() -> Generator[fakeredis.FakeRedis, None, None]:
    """提供独立的 fakeredis 实例"""
    client = fakeredis.FakeRedis()
    yield client
    client.flushall()


class TestCandleStickTask:
    """CandleStickTask 单元测试"""

    def _past_timestamp(self, period: int = 60) -> float:
        """返回一个已经完整结束的窗口内的时间戳"""
        now = time.time()
        window = int(now // period) * period
        return window - period + 1

    def test_add_task_and_pop(self, fake_redis: fakeredis.FakeRedis) -> None:
        """基本添加与弹出流程"""
        cst = CandleStickTask(fake_redis, 60)
        past_ts = self._past_timestamp(60)

        added = cst.add_task("000001", past_ts)
        assert added is True
        assert cst.get_pending_count() == 1

        tasks = cst.pop_tasks()
        assert len(tasks) == 1
        assert tasks[0]["stock"] == "000001"
        assert tasks[0]["period"] == 60
        assert tasks[0]["window"] == int(past_ts // 60) * 60
        assert cst.get_pending_count() == 0

    def test_deduplication_same_window(self, fake_redis: fakeredis.FakeRedis) -> None:
        """同一股票同一窗口的任务应当去重"""
        cst = CandleStickTask(fake_redis, 60)
        window_start = int(time.time() // 60) * 60

        assert cst.add_task("000001", window_start + 10) is True
        assert cst.add_task("000001", window_start + 20) is False
        assert cst.add_task("000001", window_start + 59) is False
        assert cst.get_pending_count() == 1

    def test_different_stocks_same_window(
        self, fake_redis: fakeredis.FakeRedis
    ) -> None:
        """不同股票同一窗口应产生独立任务"""
        cst = CandleStickTask(fake_redis, 60)
        window_start = int(time.time() // 60) * 60

        assert cst.add_task("000001", window_start + 10) is True
        assert cst.add_task("000002", window_start + 20) is True
        assert cst.get_pending_count() == 2

    def test_different_windows(self, fake_redis: fakeredis.FakeRedis) -> None:
        """不同时间窗口应当产生独立任务"""
        cst = CandleStickTask(fake_redis, 60)
        past_ts = self._past_timestamp(60)

        assert cst.add_task("000001", past_ts) is True
        assert cst.add_task("000001", past_ts + 65) is True
        assert cst.get_pending_count() == 2

    def test_wait_returns_immediately(
        self, fake_redis: fakeredis.FakeRedis
    ) -> None:
        """wait 在任务已到期时应立即返回"""
        cst = CandleStickTask(fake_redis, 60, delay_seconds=0)
        cst.add_task("000001", self._past_timestamp(60))

        tasks = cst.wait(timeout=2)
        assert len(tasks) == 1

    def test_wait_blocks_until_task_ready(
        self, fake_redis: fakeredis.FakeRedis
    ) -> None:
        """wait 应当阻塞直到任务到期"""
        cst = CandleStickTask(fake_redis, 60, delay_seconds=0.3)
        past_ts = self._past_timestamp(60)
        cst.add_task("000001", past_ts)

        window = cst._get_window(past_ts)
        new_score = time.time() + 0.3
        cst.client.zadd(cst.key, {cst._serialize_task("000001", window): new_score}, xx=True)

        tasks = cst.wait(timeout=2)
        assert len(tasks) == 1

    def test_wait_timeout(self, fake_redis: fakeredis.FakeRedis) -> None:
        """wait 超时应返回空列表"""
        cst = CandleStickTask(fake_redis, 60, delay_seconds=10)
        cst.add_task("000001", time.time())

        tasks = cst.wait(timeout=0.2, interval=0.05)
        assert tasks == []

    def test_wait_concurrent_producer(
        self, fake_redis: fakeredis.FakeRedis
    ) -> None:
        """后台 wait 时，前台添加任务应能唤醒等待"""
        cst = CandleStickTask(fake_redis, 60, delay_seconds=0)
        result: list = []

        def consumer() -> None:
            tasks = cst.wait(timeout=3)
            result.extend(tasks)

        t = threading.Thread(target=consumer)
        t.start()

        time.sleep(0.2)
        cst.add_task("000001", self._past_timestamp(60))
        t.join(timeout=3)

        assert len(result) == 1
        assert result[0]["stock"] == "000001"

    def test_custom_delay(self, fake_redis: fakeredis.FakeRedis) -> None:
        """自定义延迟时间"""
        cst = CandleStickTask(fake_redis, 60, delay_seconds=2.0)
        past_ts = self._past_timestamp(60)
        cst.add_task("000001", past_ts)

        window = cst._get_window(past_ts)
        new_score = time.time() + 2.0
        cst.client.zadd(cst.key, {cst._serialize_task("000001", window): new_score}, xx=True)

        assert cst.pop_tasks() == []
        time.sleep(2.1)
        tasks = cst.pop_tasks()
        assert len(tasks) == 1

    def test_different_periods_isolated(
        self, fake_redis: fakeredis.FakeRedis
    ) -> None:
        """不同周期的任务应当隔离"""
        cst_1m = CandleStickTask(fake_redis, 60)
        cst_5m = CandleStickTask(fake_redis, 300)
        past_ts = self._past_timestamp(60)

        assert cst_1m.add_task("000001", past_ts) is True
        assert cst_5m.add_task("000001", past_ts) is True
        assert cst_1m.get_pending_count() == 1
        assert cst_5m.get_pending_count() == 1

    def test_pop_tasks_count_limit(
        self, fake_redis: fakeredis.FakeRedis
    ) -> None:
        """pop_tasks 的 count 限制"""
        cst = CandleStickTask(fake_redis, 60, delay_seconds=0)
        past_ts = self._past_timestamp(60)

        for i in range(5):
            cst.add_task(f"STK{i:03d}", past_ts - i * 65)

        tasks = cst.pop_tasks(count=3)
        assert len(tasks) == 3
        assert cst.get_pending_count() == 2

    def test_clear(self, fake_redis: fakeredis.FakeRedis) -> None:
        """清空任务"""
        cst = CandleStickTask(fake_redis, 60)
        cst.add_task("000001", self._past_timestamp(60))
        assert cst.get_pending_count() == 1

        cst.clear()
        assert cst.get_pending_count() == 0

    def test_str_representation(self, fake_redis: fakeredis.FakeRedis) -> None:
        """字符串表示"""
        cst = CandleStickTask(fake_redis, 60, key_prefix="kline")
        assert str(cst) == "kline:60"

    def test_window_boundary(self, fake_redis: fakeredis.FakeRedis) -> None:
        """窗口边界计算正确性"""
        cst = CandleStickTask(fake_redis, 60)
        assert cst._get_window(1715000000) == 1714999980
        assert cst._get_window(1715000000.5) == 1714999980

        cst_5m = CandleStickTask(fake_redis, 300)
        assert cst_5m._get_window(1715000000) == 1714999800
