import time
import json
from typing import TypeVar, Generic, List, Any, Optional, TypedDict
import redis

T = TypeVar('T')  # 泛型类型


class DebounceInfo(TypedDict):
    """任务统计信息类型"""
    remain_cnt: int  # 剩余任务总数
    overtime_cnt: int  # 已超时但未处理的任务数


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

    def get_info(self) -> DebounceInfo:
        """
        获取任务统计信息

        Returns:
            DebounceInfo: 包含remain_cnt和overtime_cnt的字典
        """
        current_time = time.time()
        # 统计已超时（分数小于等于当前时间）的任务数量
        overtime_cnt = self.client.zcount(self.key, '-inf', current_time)
        # 获取剩余任务总数
        remain_cnt = self.get_pending_count()

        return {
            "remain_cnt": remain_cnt,
            "overtime_cnt": overtime_cnt
        }


# 测试用例
if __name__ == '__main__':
    # 连接Redis
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)

    # 创建任务管理器
    task_key = 'test_debounce_tasks_complete'
    r.delete(task_key)  # 清理测试环境

    task_manager = DebounceInfoTask[dict](r, task_key)

    # 测试1: 基本任务添加和弹出
    print("=== 测试1: 基本任务添加和弹出 ===")
    task1 = {"type": "email", "user_id": 123, "content": "Welcome!"}
    added = task_manager.add_task(task1)
    assert added is True, "第一次添加任务应该成功"
    assert task_manager.get_pending_count() == 1, "应该有1个待处理任务"

    tasks = task_manager.pop_tasks(count=1)
    assert len(tasks) == 1, "应该弹出1个任务"
    assert tasks[0] == task1, "弹出的任务数据应该与添加的一致"
    assert task_manager.get_pending_count() == 0, "弹出后应该没有待处理任务"

    # 测试2: 基于内容去重的功能
    print("\n=== 测试2: 基于内容去重 ===")
    task2 = {"type": "sms", "user_id": 456, "content": "Verification code"}

    added1 = task_manager.add_task(task2)
    added2 = task_manager.add_task(task2)  # 完全相同的内容
    assert added1 is True, "第一次添加应该成功"
    assert added2 is False, "重复内容添加应该失败"
    assert task_manager.get_pending_count() == 1, "应该只有1个任务（去重后）"

    # 测试3: 字典顺序不同但内容相同的任务应该被视为重复
    print("\n=== 测试3: 字典顺序不影响去重 ===")
    task3a = {"a": 1, "b": 2, "c": 3}
    task3b = {"c": 3, "b": 2, "a": 1}  # 顺序不同

    added1 = task_manager.add_task(task3a)
    added2 = task_manager.add_task(task3b)
    assert added1 is True, "第一次添加应该成功"
    assert added2 is False, "顺序不同但内容相同的任务应该被去重"

    task_manager.pop_tasks(count=10)  # 清理

    # 测试4: 延迟任务
    print("\n=== 测试4: 延迟任务 ===")
    task4 = {"type": "delayed", "message": "This is delayed"}

    start_time = time.time()
    task_manager.add_task(task4, delay_seconds=2)

    # 立即尝试弹出，应该为空
    tasks = task_manager.pop_tasks(count=1)
    assert len(tasks) == 0, "延迟任务不应该立即弹出"
    assert task_manager.get_pending_count() == 1, "应该有1个待处理的延迟任务"

    # 等待2秒后弹出
    time.sleep(2)
    tasks = task_manager.pop_tasks(count=1)
    elapsed = time.time() - start_time
    assert elapsed >= 2, f"应该至少等待2秒，实际等待{elapsed:.2f}秒"
    assert len(tasks) == 1, "应该弹出1个延迟任务"
    assert tasks[0] == task4, "弹出的延迟任务数据应该正确"

    # 测试5: pop_tasks的count限制
    print("\n=== 测试5: pop_tasks的count限制 ===")
    # 清理环境，确保测试不受之前测试影响
    task_manager.clear()
    r.delete(task_key)
    assert task_manager.get_pending_count() == 0, "清理后应该没有任务"

    tasks_to_add = [
        {"id": 1, "data": "task_1"},
        {"id": 2, "data": "task_2"},
        {"id": 3, "data": "task_3"},
        {"id": 4, "data": "task_4"},
        {"id": 5, "data": "task_5"},
    ]

    for task in tasks_to_add:
        task_manager.add_task(task)

    assert task_manager.get_pending_count() == 5, "应该有5个任务"

    # 只弹出3个
    tasks = task_manager.pop_tasks(count=3)
    assert len(tasks) == 3, "应该只弹出3个任务"
    assert task_manager.get_pending_count() == 2, "应该还剩2个任务"

    # 弹出剩余任务
    tasks = task_manager.pop_tasks(count=10)  # count大于实际数量
    assert len(tasks) == 2, "应该弹出剩余的2个任务"
    assert task_manager.get_pending_count() == 0, "应该没有剩余任务"

    # 测试6: remove_task功能
    print("\n=== 测试6: remove_task功能 ===")
    task6 = {"type": "removable", "data": "to be removed"}

    task_manager.add_task(task6)
    assert task_manager.get_pending_count() == 1, "应该有1个任务"

    removed = task_manager.remove_task(task6)
    assert removed is True, "删除应该成功"
    assert task_manager.get_pending_count() == 0, "删除后应该没有任务"

    # 测试7: 复杂数据类型
    print("\n=== 测试7: 复杂数据类型 ===")
    complex_task = {
        "nested": {"key": "value", "list": [1, 2, 3]},
        "numbers": [42, 3.14, 1e10],
        "boolean": True,
        "null_value": None
    }

    task_manager.add_task(complex_task)
    tasks = task_manager.pop_tasks(count=1)
    assert len(tasks) == 1, "应该弹出1个任务"
    assert tasks[0] == complex_task, "复杂数据类型应该正确序列化和反序列化"

    # 测试8: 不同数据类型的泛型支持
    print("\n=== 测试8: 不同数据类型的泛型支持 ===")

    # 字符串类型
    str_manager = DebounceInfoTask[str](r, 'test_str_tasks_complete')
    r.delete('test_str_tasks_complete')

    str_manager.add_task("simple_string")
    str_tasks = str_manager.pop_tasks(count=1)
    assert len(str_tasks) == 1, "应该弹出1个字符串任务"
    assert str_tasks[0] == "simple_string", "字符串任务应该正确"

    # 测试字符串去重
    str_manager.add_task("duplicate_string")
    str_manager.add_task("duplicate_string")  # 重复字符串
    assert str_manager.get_pending_count() == 1, "重复字符串应该被去重"

    # 列表类型
    list_manager = DebounceInfoTask[list](r, 'test_list_tasks_complete')
    r.delete('test_list_tasks_complete')

    test_list = [1, 2, {"nested": "dict"}]
    list_manager.add_task(test_list)
    list_tasks = list_manager.pop_tasks(count=1)
    assert len(list_tasks) == 1, "应该弹出1个列表任务"
    assert list_tasks[0] == test_list, "列表任务应该正确"

    # 测试列表去重
    duplicate_list = [1, 2, {"nested": "dict"}]  # 相同内容
    list_manager.add_task(test_list)
    list_manager.add_task(duplicate_list)
    assert list_manager.get_pending_count() == 1, "相同内容的列表应该被去重"

    # 测试9: 相同时间戳的任务删除验证
    print("\n=== 测试9: 相同时间戳的任务删除验证 ===")
    task_manager.clear()
    r.delete(task_key)

    # 添加3个任务，使用相同的时间戳（delay_seconds=0）
    task_a = {"task": "a"}
    task_b = {"task": "b"}
    task_c = {"task": "c"}

    task_manager.add_task(task_a)  # 立即添加，分数应该相同或非常接近
    task_manager.add_task(task_b)  # 立即添加
    task_manager.add_task(task_c)  # 立即添加

    assert task_manager.get_pending_count() == 3, "应该有3个任务"

    # 只弹出2个
    tasks = task_manager.pop_tasks(count=2)
    assert len(tasks) == 2, "应该弹出2个任务"
    assert task_manager.get_pending_count() == 1, "应该还剩1个任务"

    # 弹出剩余的1个
    tasks = task_manager.pop_tasks(count=5)
    assert len(tasks) == 1, "应该弹出剩余的1个任务"
    assert task_manager.get_pending_count() == 0, "应该没有剩余任务"

    # 测试10: 任务排序（按延迟时间）
    print("\n=== 测试10: 任务排序 ===")
    task_manager.clear()
    r.delete(task_key)

    # 添加3个任务，不同的延迟时间
    task_manager.add_task({"task": "b"}, delay_seconds=3)
    task_manager.add_task({"task": "a"}, delay_seconds=1)
    task_manager.add_task({"task": "c"}, delay_seconds=5)

    # 等待1.5秒
    time.sleep(1.5)

    # 应该只弹出task 'a'
    tasks = task_manager.pop_tasks(count=5)
    assert len(tasks) == 1, "应该只弹出1个任务（延迟1秒的那个）"
    assert tasks[0] == {"task": "a"}, "应该弹出延迟最短的任务"
    assert task_manager.get_pending_count() == 2, "应该还剩2个任务（b和c）"

    # 等待2秒（总共3.5秒，让b任务到期，但c任务还没到5秒）
    time.sleep(2)

    # 应该只弹出task 'b'
    tasks = task_manager.pop_tasks(count=5)
    assert len(tasks) == 1, "应该弹出1个任务"
    assert tasks[0] == {"task": "b"}, "应该弹出延迟3秒的任务"
    assert task_manager.get_pending_count() == 1, "应该还剩1个任务（c）"

    # 再等待2秒（总共5.5秒，让c任务也到期）
    time.sleep(2)

    # 弹出最后一个任务
    tasks = task_manager.pop_tasks(count=5)
    assert len(tasks) == 1, "应该弹出最后1个任务"
    assert tasks[0] == {"task": "c"}, "应该弹出延迟5秒的任务"
    assert task_manager.get_pending_count() == 0, "应该没有剩余任务"

    # 测试11: 默认延迟时间
    print("\n=== 测试11: 默认延迟时间 ===")
    task_key_default = 'test_default_delay'
    r.delete(task_key_default)

    # 创建实例时设置默认延迟3秒
    task_manager_default = DebounceInfoTask[dict](r, task_key_default, delay_seconds=3)

    task11 = {"message": "delayed by default"}
    start_time = time.time()
    added = task_manager_default.add_task(task11)  # 使用默认延迟
    assert added is True, "添加任务应该成功"

    # 立即尝试弹出，应该为空
    tasks = task_manager_default.pop_tasks(count=1)
    assert len(tasks) == 0, "使用默认延迟的任务不应该立即弹出"
    assert task_manager_default.get_pending_count() == 1, "应该有1个待处理任务"

    # 等待3秒后弹出
    time.sleep(3)
    tasks = task_manager_default.pop_tasks(count=1)
    elapsed = time.time() - start_time
    assert elapsed >= 3, f"应该至少等待3秒，实际等待{elapsed:.2f}秒"
    assert len(tasks) == 1, "应该弹出1个任务"
    assert tasks[0] == task11, "弹出的任务数据应该正确"

    # 测试12: 覆盖默认延迟时间
    print("\n=== 测试12: 覆盖默认延迟时间 ===")
    task_key_override = 'test_override_delay'
    r.delete(task_key_override)

    # 创建实例时设置默认延迟5秒
    task_manager_override = DebounceInfoTask[dict](r, task_key_override, delay_seconds=5)

    task12a = {"message": "using override delay"}
    task12b = {"message": "using default delay"}

    # 覆盖默认延迟，只延迟1秒
    task_manager_override.add_task(task12a, delay_seconds=1)
    # 使用默认延迟5秒
    task_manager_override.add_task(task12b)

    assert task_manager_override.get_pending_count() == 2, "应该有2个任务"

    # 等待1.5秒
    time.sleep(1.5)

    # 应该只弹出task12a
    tasks = task_manager_override.pop_tasks(count=5)
    assert len(tasks) == 1, "应该只弹出1个任务"
    assert tasks[0] == task12a, "应该弹出使用1秒延迟的任务"
    assert task_manager_override.get_pending_count() == 1, "应该还有1个任务（使用默认延迟的）"

    # 测试13: 立即执行（delay_seconds=0）
    print("\n=== 测试13: 立即执行 ===")
    task_key_immediate = 'test_immediate'
    r.delete(task_key_immediate)

    task_manager_immediate = DebounceInfoTask[dict](r, task_key_immediate, delay_seconds=0)

    immediate_task = {"message": "immediate"}
    task_manager_immediate.add_task(immediate_task)

    tasks = task_manager_immediate.pop_tasks(count=1)
    assert len(tasks) == 1, "应该立即弹出任务"
    assert tasks[0] == immediate_task, "弹出的任务数据应该正确"

    # 测试14: 去重功能验证（带默认延迟）
    print("\n=== 测试14: 去重功能验证（带默认延迟）===")
    task_key_dedup = 'test_dedup_with_delay'
    r.delete(task_key_dedup)

    task_manager_dedup = DebounceInfoTask[dict](r, task_key_dedup, delay_seconds=2)

    task14 = {"task": "dedup_test"}

    # 添加两次，应该只有第一次成功
    added1 = task_manager_dedup.add_task(task14)
    added2 = task_manager_dedup.add_task(task14)  # 重复
    assert added1 is True, "第一次添加应该成功"
    assert added2 is False, "重复添加应该失败"
    assert task_manager_dedup.get_pending_count() == 1, "去重后应该只有1个任务"

    # 测试覆盖延迟时间时也能正确去重
    added3 = task_manager_dedup.add_task(task14, delay_seconds=5)  # 不同的延迟时间
    assert added3 is False, "即使延迟时间不同，相同内容也应该被去重"

    # 测试15: get_info方法 - 获取任务统计信息
    print("\n=== 测试15: get_info方法 ===")
    task_key_info = 'test_get_info'
    r.delete(task_key_info)

    task_manager_info = DebounceInfoTask[dict](r, task_key_info)

    # 初始状态
    info = task_manager_info.get_info()
    assert info["remain_cnt"] == 0, "初始remain_cnt应该为0"
    assert info["overtime_cnt"] == 0, "初始overtime_cnt应该为0"

    # 添加3个任务：2个已超时，1个未超时
    task_manager_info.add_task({"task": "overtime1"}, delay_seconds=0)  # 立即超时
    time.sleep(0.1)  # 确保时间差
    task_manager_info.add_task({"task": "overtime2"}, delay_seconds=0)  # 立即超时
    task_manager_info.add_task({"task": "future"}, delay_seconds=5)  # 5秒后超时

    info = task_manager_info.get_info()
    assert info["remain_cnt"] == 3, "应该总共有3个任务"
    assert info["overtime_cnt"] == 2, "应该有2个已超时任务"

    # 弹出1个超时任务
    task_manager_info.pop_tasks(count=1)
    info = task_manager_info.get_info()
    assert info["remain_cnt"] == 2, "弹出1个后应该剩余2个任务"
    assert info["overtime_cnt"] == 1, "弹出1个后应该剩余1个超时任务"

    print("\n=== 所有测试通过! ===")

    # 清理所有测试key
    test_keys = [
        task_key,
        'test_str_tasks_complete',
        'test_list_tasks_complete',
        task_key_default,
        task_key_override,
        task_key_immediate,
        task_key_dedup,
        task_key_info
    ]
    r.delete(*test_keys)
