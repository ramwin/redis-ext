import time
import json
from typing import TypeVar, Generic, List, Any

from .types import RedisClient

T = TypeVar('T')


class JsonTaskQueue(Generic[T]):
    """
    基于Redis Sorted Set实现的去重队列

    支持FIFO顺序（按插入时间），基于JSON序列化内容去重。
    相同内容的任务不会重复添加。
    """

    def __init__(self, client: RedisClient, key: str):
        self.client = client
        self.key = key

    def _serialize(self, info: T) -> str:
        """序列化任务数据为字符串"""
        if isinstance(info, dict):
            return json.dumps(info, ensure_ascii=False, sort_keys=True)
        return json.dumps(info, ensure_ascii=False)

    def _deserialize(self, task_str: str) -> Any:
        """反序列化任务数据"""
        return json.loads(task_str)

    def push(self, info: T) -> bool:
        """
        添加任务，以当前时间戳为score

        Args:
            info: 任务数据，支持任意可JSON序列化的类型

        Returns:
            bool: 是否成功添加（如果相同内容已存在则返回False）
        """
        task_data = self._serialize(info)
        # 使用当前时间戳作为score，nx参数确保member不存在时才添加
        added = self.client.zadd(self.key, {task_data: time.time()}, nx=True)
        return added == 1

    def pop(self, count: int = 1) -> List[T]:
        """
        弹出score最小的任务（先插入的先弹出）

        Args:
            count: 要弹出的任务数量，默认1个

        Returns:
            List[T]: 弹出的任务数据列表
        """
        # 使用zpopmin原子弹出score最小的任务
        items = self.client.zpopmin(self.key, count)
        if not items:
            return []

        # 反序列化任务数据（items为[(member, score), ...]）
        tasks = []
        for member, _score in items:
            task_str = member.decode('utf-8') if isinstance(member, bytes) else member
            tasks.append(self._deserialize(task_str))

        return tasks

    def peek(self, count: int = 1) -> List[T]:
        """
        查看队列头部的任务，不移除

        Args:
            count: 要查看的任务数量，默认1个

        Returns:
            List[T]: 队列头部的任务数据列表
        """
        task_bytes_list = self.client.zrange(self.key, 0, count - 1)
        tasks = []
        for tb in task_bytes_list:
            task_str = tb.decode('utf-8') if isinstance(tb, bytes) else tb
            tasks.append(self._deserialize(task_str))
        return tasks

    def remove(self, info: T) -> bool:
        """
        删除指定内容的任务

        Args:
            info: 要删除的任务数据

        Returns:
            bool: 是否成功删除
        """
        task_data = self._serialize(info)
        return self.client.zrem(self.key, task_data) == 1

    def __contains__(self, info: T) -> bool:
        """
        检查任务是否存在于队列中

        Args:
            info: 要检查的任务数据

        Returns:
            bool: 任务是否存在
        """
        task_data = self._serialize(info)
        return self.client.zscore(self.key, task_data) is not None

    def __len__(self) -> int:
        """获取队列中的任务数量"""
        return self.client.zcard(self.key)

    def clear(self) -> None:
        """清空队列"""
        self.client.delete(self.key)
