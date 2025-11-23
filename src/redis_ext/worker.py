import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait as futures_wait
from typing import Any, Callable, Optional, Union
from redis import Redis

logger = logging.getLogger(__name__)

class RedisWork:
    """
    监听 Redis list，线程池并发处理任务。


    1. 任务回调抛错 -> 停止监听，等线程池清场后再把原始异常重新抛出。
    2. 并发达到 max_workers -> 暂停监听，让别的进程消费；
       线程池出现空闲后可自动恢复（resume_when_idle 控制）。
    """

    def __init__(
        self,
        list_key: str,
        max_workers: int = 4,
        redis_client: Optional[Redis] = None,
        *,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        timeout: int = 5,
        decode_responses: bool = True,
        resume_when_idle: bool = True,
    ):
        self.list_key = list_key
        self.timeout = timeout
        self.resume_when_idle = resume_when_idle

        # Redis 连接
        if redis_client is None:
            self.rdb = Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=decode_responses,
            )
        else:
            self.rdb = redis_client

        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._max_workers = max_workers

        # 生命周期
        self._stop_evt = threading.Event()
        self._handler: Optional[Callable[[dict], Any]] = None
        self._first_exc: Optional[BaseException] = None
        self._exc_lock = threading.Lock()

        # 调度器状态
        self._futures = set()
        self._idle = threading.Event()
        self._idle.set()  # 初始空闲

    # ---------------- 公共 API ----------------
    def register_handler(self, func: Callable[[dict], Any]) -> None:
        self._handler = func

    def start(self) -> None:
        """阻塞运行，直到手动 stop、任务抛错或线程池打满且 resume_when_idle=False。"""
        if self._handler is None:
            raise RuntimeError("必须先 register_handler")

        logger.info("RedisWork started, listening key='%s'", self.list_key)
        try:
            while not self._stop_evt.is_set():
                # 1. 如果已有异常，结束
                if self._first_exc is not None:
                    break

                # 2. 如果线程池已满，暂停监听
                if not self._idle.is_set():
                    logger.warning("并发达到上限，暂停监听 Redis")
                    self._idle.wait()
                    if not self.resume_when_idle:
                        logger.info("resume_when_idle=False，退出监听")
                        break
                    logger.info("线程池出现空闲，恢复监听")
                    continue

                # 3. 正常取任务
                raw_task = self.rdb.blpop(self.list_key, timeout=self.timeout)
                if raw_task is None:
                    continue

                _, raw = raw_task
                try:
                    task = json.loads(raw)
                except Exception as exc:
                    logger.error("Bad task format: %s", raw, exc_info=exc)
                    continue

                # 4. 提交到线程池
                fut = self.executor.submit(self._safe_handler, task)
                with self._exc_lock:
                    self._futures.add(fut)
                fut.add_done_callback(self._on_future_done)

        except KeyboardInterrupt:
            logger.info("Caught Ctrl-C, shutting down…")
        finally:
            self.shutdown()
            # 如果是任务抛错导致跳出循环，在这里重新抛出
            if self._first_exc is not None:
                raise self._first_exc

    def shutdown(self, wait: bool = True) -> None:
        """优雅关闭：停止监听 + 等待线程池完成。"""
        self._stop_evt.set()
        logger.info("等待线程池清场…")
        if wait:
            futures_wait(self._futures, timeout=None)
        self.executor.shutdown(wait=wait)
        logger.info("RedisWork stopped.")

    # ---------------- 内部 ----------------
    def _safe_handler(self, task: dict) -> None:
        try:
            self._handler(task)  # type: ignore
        except Exception as exc:
            with self._exc_lock:
                if self._first_exc is None:
                    self._first_exc = exc
                    logger.exception("Task handler failed，将停止监听")
            raise

    def _on_future_done(self, fut) -> None:
        """任务完成回调：维护 futures 集合 和 空闲状态。"""
        with self._exc_lock:
            self._futures.discard(fut)
            if len(self._futures) < self._max_workers:
                self._idle.set()
            else:
                self._idle.clear()
