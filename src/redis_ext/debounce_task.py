import time
import json
from typing import TypeVar, Generic, List, Any, Optional
import redis

T = TypeVar('T')  # 泛型类型

class DebounceInfoTask(Generic[T]):
    """
    使用Redis Sorted Set实现的任务去重和调度类
    基于序列化后的结果进行去重，相同内容的任务不会重复添加
    """

    def __init__(self, client: redis.Redis, key: str, delay_seconds: float = 0):
        """
        初始化DebounceInfoTask实例

        Args:
            client: Redis客户端实例
            key: Sorted Set的键名
            delay_seconds: 默认的延迟执行时间（秒），默认0表示立即执行
        """
        self.client = client
        self.key = key
        self.default_delay_seconds = delay_seconds

    def _serialize_task(self, info: T) -> str:
        """序列化任务数据为字符串"""
        # 使用排序的json确保相同内容序列化结果一致
        if isinstance(info, dict):
            return json.dumps(info, ensure_ascii=False, sort_keys=True)
        return json.dumps(info, ensure_ascii=False)

    def _deserialize_task(self, task_str: str) -> Any:
        """反序列化任务数据"""
        return json.loads(task_str)

    def add_task(self, info: T, delay_seconds: Optional[float] = None) -> bool:
        """
        添加任务到sorted set，基于序列化结果去重

        Args:
            info: 任务数据，支持任意可JSON序列化的类型
            delay_seconds: 延迟执行的时间（秒），如果为None则使用类初始化时的默认值

        Returns:
            bool: 任务是否成功添加（如果相同内容已存在则返回False）
        """
        # 使用类默认延迟时间或指定的时间
        actual_delay = delay_seconds if delay_seconds is not None else self.default_delay_seconds

        # 序列化任务数据
        task_data = self._serialize_task(info)

        # 计算执行时间戳
        execute_at = time.time() + actual_delay

        # 使用 NX 参数，只在成员不存在时才添加
        # 返回值表示添加的成员数量，0 表示已存在，1 表示添加成功
        added_count = self.client.zadd(self.key, {task_data: execute_at}, nx=True)

        return added_count == 1

    def pop_tasks(self, count: int = 10) -> List[T]:
        """
        弹出已到执行时间的任务（分数最小的任务）

        Args:
            count: 要弹出的任务数量，默认10个

        Returns:
            List[T]: 任务数据列表，按执行时间排序
        """
        current_time = time.time()

        # 查询已到期的任务（只查询，不删除）
        tasks_data = self.client.zrangebyscore(
            self.key, '-inf', current_time,
            start=0, num=count, withscores=False
        )

        if not tasks_data:
            return []

        # 使用pipeline批量删除这些具体的任务（不是按分数范围删除）
        pipeline = self.client.pipeline()
        for task_bytes in tasks_data:
            pipeline.zrem(self.key, task_bytes)
        pipeline.execute()

        # 反序列化任务数据
        tasks = []
        for task_bytes in tasks_data:
            task_str = task_bytes.decode('utf-8') if isinstance(task_bytes, bytes) else task_bytes
            task = self._deserialize_task(task_str)
            tasks.append(task)

        return tasks

    def get_pending_count(self) -> int:
        """获取待执行任务数量"""
        return self.client.zcard(self.key)

    def remove_task(self, info: T) -> bool:
        """手动删除指定内容的任务"""
        task_data = self._serialize_task(info)
        return self.client.zrem(self.key, task_data) == 1

    def clear(self) -> None:
        """清空所有任务"""
        self.client.delete(self.key)


# 测试用例
if __name__ == '__main__':
    # 连接Redis
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)

    # 测试1: 默认延迟时间
    print("=== 测试1: 默认延迟时间 ===")
    task_key = 'test_default_delay'
    r.delete(task_key)

    # 创建实例时设置默认延迟3秒
    task_manager = DebounceInfoTask[dict](r, task_key, delay_seconds=3)

    task1 = {"message": "delayed by default"}
    start_time = time.time()
    task_manager.add_task(task1)  # 使用默认延迟

    # 立即尝试弹出，应该为空
    tasks = task_manager.pop_tasks(count=1)
    assert len(tasks) == 0, "使用默认延迟的任务不应该立即弹出"
    assert task_manager.get_pending_count() == 1, "应该有1个待处理任务"

    # 等待3秒后弹出
    time.sleep(3)
    tasks = task_manager.pop_tasks(count=1)
    elapsed = time.time() - start_time
    assert elapsed >= 3, f"应该至少等待3秒，实际等待{elapsed:.2f}秒"
    assert len(tasks) == 1, "应该弹出1个任务"
    assert tasks[0] == task1, "弹出的任务数据应该正确"

    # 测试2: 覆盖默认延迟时间
    print("\n=== 测试2: 覆盖默认延迟时间 ===")
    task_key2 = 'test_override_delay'
    r.delete(task_key2)

    # 创建实例时设置默认延迟5秒
    task_manager2 = DebounceInfoTask[dict](r, task_key2, delay_seconds=5)

    task2 = {"message": "using override delay"}
    task3 = {"message": "using default delay"}

    # 覆盖默认延迟，只延迟1秒
    task_manager2.add_task(task2, delay_seconds=1)
    # 使用默认延迟5秒
    task_manager2.add_task(task3)

    # 等待1.5秒
    time.sleep(1.5)

    # 应该只弹出task2
    tasks = task_manager2.pop_tasks(count=5)
    assert len(tasks) == 1, "应该只弹出1个任务"
    assert tasks[0] == task2, "应该弹出使用1秒延迟的任务"
    assert task_manager2.get_pending_count() == 1, "应该还有1个任务（使用默认延迟的）"

    # 清理
    r.delete(task_key, task_key2)

    # 测试3: 不指定延迟时间（立即执行）
    print("\n=== 测试3: 立即执行 ===")
    task_key3 = 'test_immediate'
    r.delete(task_key3)

    task_manager3 = DebounceInfoTask[dict](r, task_key3)  # delay_seconds默认0

    immediate_task = {"message": "immediate"}
    task_manager3.add_task(immediate_task)  # 立即执行

    tasks = task_manager3.pop_tasks(count=1)
    assert len(tasks) == 1, "应该立即弹出任务"
    assert tasks[0] == immediate_task, "弹出的任务数据应该正确"

    # 测试4: 验证去重功能仍然正常
    print("\n=== 测试4: 去重功能验证 ===")
    task_key4 = 'test_dedup_with_delay'
    r.delete(task_key4)

    task_manager4 = DebounceInfoTask[dict](r, task_key4, delay_seconds=2)

    task4 = {"task": "dedup_test"}

    # 添加两次，应该只有第一次成功
    added1 = task_manager4.add_task(task4)
    added2 = task_manager4.add_task(task4)  # 重复
    assert added1 is True, "第一次添加应该成功"
    assert added2 is False, "重复添加应该失败"
    assert task_manager4.get_pending_count() == 1, "去重后应该只有1个任务"

    # 清理
    r.delete(task_key3, task_key4)

    print("\n=== 所有测试通过! ===")
