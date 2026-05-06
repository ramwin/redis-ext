# SPDX-FileCopyrightText: 2025-present Xiang Wang <ramwin@qq.com>
#
# SPDX-License-Identifier: MIT

import json
import time
from typing import List, Optional, TypedDict

from .types import RedisClient


class CandleStickTaskData(TypedDict):
    """K线统计任务数据类型"""
    stock: str       # 股票代码
    period: int      # 统计周期（秒）
    window: int      # 时间窗口起始时间戳


class CandleStickTask:
    """
    股票K线统计任务调度器

    基于Redis Sorted Set实现，一个实例管理**所有股票**在同一统计周期下的
    K线统计任务。后台脚本通过 ``wait`` 或 ``pop_tasks`` 批量获取已到期的任务，
    再到数据库中查询对应股票对应窗口的交易量、最低价、最高价等数据。

    同一股票、同一时间窗口的任务会自动去重。

    Usage:
        >>> cst = CandleStickTask(redis_client, period=60)
        >>> cst.add_task("000001.SZ", time.time())   # 新成交
        >>> cst.add_task("000002.SZ", time.time())   # 另一只股票
        >>> tasks = cst.wait()                        # 后台阻塞等待
        >>> for task in tasks:
        ...     print(f"统计 {task['stock']} 窗口 {task['window']}")
    """

    def __init__(
        self,
        client: RedisClient,
        period: int,
        *,
        delay_seconds: float = 1.0,
        key_prefix: str = "candlestick",
    ) -> None:
        """
        初始化K线任务调度器

        Args:
            client: Redis 客户端实例
            period: 统计周期（秒），如 ``60`` 表示1分钟K线
            delay_seconds: 任务延迟执行时间（秒），默认1秒，
                即窗口结束后再延迟该时间才允许被弹出
            key_prefix: Redis key 前缀
        """
        self.client = client
        self.period = period
        self.delay_seconds = delay_seconds
        self.key = f"{key_prefix}:{period}"

    def _get_window(self, timestamp: float) -> int:
        """根据时间戳计算所属时间窗口的起始时间"""
        return int(timestamp // self.period) * self.period

    def _serialize_task(self, stock: str, window: int) -> str:
        """序列化任务数据为存储格式"""
        return json.dumps({"stock": stock, "window": window}, sort_keys=True)

    def add_task(self, stock: str, timestamp: float) -> bool:
        """
        添加价格更新任务

        根据股票代码和交易时间戳计算所属时间窗口，为该窗口创建一个
        延迟执行的统计任务。同一股票相同窗口的任务会被去重。

        Args:
            stock: 股票代码，如 ``"000001.SZ"``
            timestamp: 交易发生的时间戳（秒级，支持浮点数）

        Returns:
            bool: 任务是否成功添加。``False`` 表示同一股票同一窗口任务已存在。
        """
        window = self._get_window(timestamp)
        execute_at = window + self.period + self.delay_seconds
        task_data = self._serialize_task(stock, window)
        added = self.client.zadd(self.key, {task_data: execute_at}, nx=True)
        return added == 1

    def pop_tasks(self, count: int = 10) -> List[CandleStickTaskData]:
        """
        弹出已到执行时间的任务

        先查询到期任务，再逐个 ``zrem`` 尝试删除；
        只有 ``zrem`` 返回 1（真正删除成功）的任务才会被返回，
        从而保证多进程/多实例并发调用时不会出现重复处理。

        Args:
            count: 本次最多弹出的任务数量

        Returns:
            任务字典列表，每个字典包含 ``stock``、``period``、
            ``window`` 字段，按到期时间升序排列。
        """
        current_time = time.time()
        tasks_data = self.client.zrangebyscore(
            self.key, "-inf", current_time,
            start=0, num=count, withscores=False,
        )
        if not tasks_data:
            return []

        # 逐个尝试删除，只有真正删成功的才是本进程抢到的
        pipeline = self.client.pipeline()
        for task_bytes in tasks_data:
            pipeline.zrem(self.key, task_bytes)
        results = pipeline.execute()

        tasks: List[CandleStickTaskData] = []
        for task_bytes, removed in zip(tasks_data, results):
            if not removed:
                continue
            task_str = (
                task_bytes.decode("utf-8")
                if isinstance(task_bytes, bytes)
                else task_bytes
            )
            task = json.loads(task_str)
            task["period"] = self.period
            tasks.append(task)

        return tasks

    def wait(
        self,
        timeout: Optional[float] = None,
        *,
        interval: float = 0.1,
        count: int = 10,
    ) -> List[CandleStickTaskData]:
        """
        阻塞等待直到有可执行的任务

        采用轮询方式检查到期任务，拿到任务后立即返回。

        Args:
            timeout: 最大等待时间（秒），``None`` 表示无限等待
            interval: 每次轮询的间隔（秒）
            count: 每次检查弹出的最大任务数

        Returns:
            可执行的任务列表。若超时仍未有任务，返回空列表。
        """
        start_time = time.time()
        while True:
            tasks = self.pop_tasks(count=count)
            if tasks:
                return tasks

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return []

            time.sleep(interval)

    def get_pending_count(self) -> int:
        """获取当前待处理任务总数"""
        return self.client.zcard(self.key)

    def clear(self) -> None:
        """清空所有任务"""
        self.client.delete(self.key)

    def __str__(self) -> str:
        return self.key
