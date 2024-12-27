import datetime
import hashlib
import json
import os
import signal
import time
import uuid
from typing import (
    Any,
    Collection,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import redis
from redis.client import PubSub
from redis.exceptions import LockError
from structlog.stdlib import BoundLogger

from ._internal import (
    ACTIVE,
    ERROR,
    QUEUED,
    SCHEDULED,
    dotted_parts,
    queue_matches,
)
from .exceptions import TaskNotFound
from .redis_scripts import RedisScripts
from .redis_semaphore import Semaphore
from .stats import StatsThread
from .task import Task
from .timeouts import JobTimeoutException
from .utils import redis_glob_escape

LOCK_REDIS_KEY = "qslock"

log: BoundLogger

class Worker:
    def __init__(
        self,
        config:Optional[Dict] = None,
    ) -> None:
        """
        Internal method to initialize a worker.
        """
        bound_logger = log.bind(pid=os.getpid())
        assert isinstance(bound_logger, BoundLogger)
        self.log = bound_logger

        self.connection = redis.Redis(
            decode_responses=True
        )
        self.scripts = RedisScripts(self.connection)
        self.config = config
        self.redis_prefix = config["REDIS_PREFIX"]
        self._did_work = True
        self._last_task_check = 0.0
        self.stats_thread: Optional[StatsThread] = None
        self.id = str(uuid.uuid4())
        self._queue = "RayJobQueue"
        self.max_workers_per_queue = 1
        self._stop_requested = False

    def _install_signal_handlers(self) -> None:
        """
        Sets up signal handlers for safely stopping the worker.
        """

        def request_stop(signum: int, frame: Any) -> None:
            self._stop_requested = True
            self.log.info("stop requested, waiting for task to finish")

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

    def _uninstall_signal_handlers(self) -> None:
        """
        Restores default signal handlers.
        """
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    # SCHEDULED -> QUEUED
    def _worker_queue_scheduled_tasks(self) -> None:
        """
        Helper method that takes due tasks from the SCHEDULED queue and puts
        them in the QUEUED queue for execution. This should be called
        periodically.
        """
        timeout = self.config["QUEUE_SCHEDULED_TASKS_TIME"]
        if timeout > 0:
            lock_name = self._key(
                "lockv2", "queue_scheduled_tasks", self.worker_group_name
            )
            lock = self.connection.lock(lock_name, timeout=timeout)

            # See if any worker has recently queued scheduled tasks.
            if not lock.acquire(blocking=False):
                return

        queues = set(
            self._filter_queues(self._retrieve_queues(self._key(SCHEDULED)))
        )

        now = time.time()
        for queue in queues:
            result = self.scripts.zpoppush(
                self._key(SCHEDULED, queue),
                self._key(QUEUED, queue),
                self.config["SCHEDULED_TASK_BATCH_SIZE"],
                now,
                now,
                if_exists=("noupdate",),
                on_success=(
                    "update_sets",
                    queue,
                    self._key(SCHEDULED),
                    self._key(QUEUED),
                ),
            )
            self.log.info(f"scheduled tasks queue {queue} qty={len(result)}")
            # XXX: ideally this would be in the same pipeline, but we only want
            # to announce if there was a result.
            if result:
                if self.config["PUBLISH_QUEUED_TASKS"]:
                    self.connection.publish(self._key("activity"), queue)
                self._did_work = True

    def _poll_for_queues(self) -> None:
        if not self._did_work:
            time.sleep(self.config["POLL_TASK_QUEUES_INTERVAL"])
        self._load_consumer_job_queue()

    def _pubsub_for_queues(
        self, timeout: float = 0, batch_timeout: float = 0
    ) -> None:
        new_queue_found = False
        start_time = batch_exit = time.time()
        assert self._pubsub is not None
        while True:
            # Check to see if batch_exit has been updated
            if batch_exit > start_time:
                pubsub_sleep = batch_exit - time.time()
            else:
                pubsub_sleep = start_time + timeout - time.time()
            message = self._pubsub.get_message(
                timeout=0
                if pubsub_sleep < 0 or self._did_work
                else pubsub_sleep
            )

            # Pull remaining messages off of channel
            while message:
                if message["type"] == "message":
                    new_queue_found, batch_exit = self._process_queue_message(
                        message["data"],
                        new_queue_found,
                        batch_exit,
                        start_time,
                        timeout,
                        batch_timeout,
                    )

                message = self._pubsub.get_message()

            if self._did_work:
                # Exit immediately if we did work during the last execution
                # loop because there might be more work to do
                break
            elif time.time() >= batch_exit and new_queue_found:
                # After finding a new queue we can wait until the batch timeout
                # expires
                break
            elif time.time() - start_time > timeout:
                # Always exit after our maximum wait time
                break

    def _worker_queue_expired_tasks(self) -> None:
       
        lock = self.connection.lock(
            self._key("lockv2", "queue_expired_tasks"),
            timeout=self.config["REQUEUE_EXPIRED_TASKS_INTERVAL"],
        )
        if not lock.acquire(blocking=False):
            return

        now = time.time()

        # Get a batch of expired tasks.
        task_data = self.scripts.get_expired_tasks(
            self.config["REDIS_PREFIX"],
            now - self.config["ACTIVE_TASK_UPDATE_TIMEOUT"],
            self.config["REQUEUE_EXPIRED_TASKS_BATCH_SIZE"],
        )

        for (queue, task_id) in task_data:
            self.log.info(f"expiring task, queue={queue}, task_id={task_id}")
            self._did_work = True
            try:
                task = Task.from_id(self.tiger, queue, ACTIVE, task_id)
                if task.should_retry_on(JobTimeoutException, logger=self.log):
                    self.log.info(
                        f"queueing expired task, queue={queue}, task_id={task_id}"
                    )

                    # Task is idempotent and can be requeued. If the task
                    # already exists in the QUEUED queue, don't change its
                    # time.
                    task._move(
                        from_state=ACTIVE, to_state=QUEUED, when=now, mode="nx"
                    )
                else:
                    self.log.error(
                        f"failing expired task, queue={queue}, task_id={task_id}"
                    )

                    # Assume the task can't be retried and move it to the error
                    # queue.
                    task._move(from_state=ACTIVE, to_state=ERROR, when=now)
            except TaskNotFound:
                # Either the task was requeued by another worker, or we
                # have a task without a task object.

                # XXX: Ideally, the following block should be atomic.
                if not self.connection.get(self._key("task", task_id)):
                    self.log.error(f"not found, queue={queue}, task_id={task_id}")
                    task = Task(
                        self.tiger,
                        queue=queue,
                        _data={"id": task_id},
                        _state=ACTIVE,
                    )
                    task._move()

        if len(task_data) == self.config["REQUEUE_EXPIRED_TASKS_BATCH_SIZE"]:
            try:
                lock.release()
            except LockError:
                # Not really a problem if releasing lock fails. It will expire
                # soon anyway.
                self.log.warning(
                    "failed to release lock queue_expired_tasks on full batch"
                )

    def _get_queue_batch_size(self, queue: str) -> int:
        batch_size = 1
        return batch_size

    def _get_queue_lock(self) -> Union[
        Tuple[None, Literal[True]], Tuple[Optional[Semaphore], Literal[False]]
    ]:
        max_workers = self.max_workers_per_queue
        if max_workers:
            queue_lock = Semaphore(
                self.connection,
                self._key(LOCK_REDIS_KEY, self._pending_queue),
                self.id,
                max_locks=max_workers,
                timeout=self.config["ACTIVE_TASK_UPDATE_TIMEOUT"],
            )
            acquired, locks = queue_lock.acquire()
            if not acquired:
                return None, True
        else:
            queue_lock = None

        return queue_lock, False

    def heartbeat(self, queue: str, job_ids: Collection[str]) -> None:
        """
        Updates the heartbeat for the given task IDs to prevent them from
        timing out and being requeued.
        """
        now = time.time()
        mapping = {task_id: now for task_id in job_ids}
        self.connection.zadd(self._key(ACTIVE, queue), mapping)  # type: ignore[arg-type]

    def _process_queue_message(
        self,
        message_queue: str,
        new_queue_found: bool,
        batch_exit: float,
        start_time: float,
        timeout: float,
        batch_timeout: float,
    ) -> Tuple[bool, float]:
        """Process a queue message from activity channel."""

        for queue in self._filter_queues([message_queue]):
            if queue not in self._queue_set:
                if not new_queue_found:
                    new_queue_found = True
                    batch_exit = time.time() + batch_timeout
                    # Limit batch_exit to max timeout
                    if batch_exit > start_time + timeout:
                        batch_exit = start_time + timeout
                self._queue_set.add(queue)
                self.log.info(f"new queue, queue={queue}")

        return new_queue_found, batch_exit

    def _process_queue_jobs(
        self,
        queue: str,
        queue_lock: Optional[Semaphore],
        job_ids: List[str],
        now: float,
        log: BoundLogger,
    ) -> int:
        """Process tasks in queue."""

        processed_count = 0

        serialized_jobs = self.connection.mget(
            [self._key("task", job_id) for job_id in job_ids]
        )
        jobs = []
        for job_id, serialized_job in zip(job_ids, serialized_jobs):
            if serialized_job:
                job_data = json.loads(serialized_job)
            else:
                job_data = {"id": job_id}

            job = Task(
                self.tiger,
                queue=queue,
                _data=job_data,
                _state=ACTIVE,
                _ts=now,
            )

            if not serialized_job:
                # Remove task as per comment above
                log.error("not found", job_id=job_id)
                job._move()
            elif job.id != job_id:
                log.error("task ID mismatch", job_id=job_id)
                job._move()
            else:
                jobs.append(job)

        for job in jobs:
            success = self._execute_job(
                queue, job, queue_lock
            )
            processed_count = processed_count + 1
            log.info(
                "job processed", job_id=job.id, success=success
            )
            self._finish_job_processing(queue, job, success, now)        
            
        return processed_count

    def _process_from_queue(self, queue: str) -> int:
        now = time.time()
        batch_size = self._get_queue_batch_size()

        # queue_lock, failed_to_acquire = self._get_queue_lock()
        # if failed_to_acquire:
        #     return [], -1

        job_ids = self.scripts.zpoppush(
            self._key(QUEUED, queue),
            self._key(SCHEDULED, queue),
            batch_size,
            None,
            now,
            if_exists=(),
            on_success=(
                "update_sets",
                queue,
                self._key(QUEUED),
                self._key(SCHEDULED),
            ),
        )
        log.info(
            "moved jobs",
            src_queue=QUEUED,
            dest_queue=SCHEDULED,
            job_ids=job_ids,
            qty=len(job_ids),
        )
        processed_count = 0
        if job_ids:
            processed_count = self._process_queue_jobs(
                queue, None, job_ids, now, log
            )

        # if queue_lock:
        #     queue_lock.release()
        #     log.debug("released swq lock")
        return job_ids, processed_count

    def _execute_job(
        self,
        queue: str,
        job: Task,
    ) -> bool:
    
        log: BoundLogger = self.log.bind(queue=queue)
        assert isinstance(log, BoundLogger)

        lock_id = None
        lock = self.connection.lock(
            self._key("lockv2", lock_id),
            timeout=self.config["ACTIVE_TASK_UPDATE_TIMEOUT"],
            # Sync worker uses a thread to renew the lock.
            thread_local=False,
        )
        if not lock.acquire(blocking=False):
            log.info("could not acquire lock", job_id=job.id)

            # Reschedule the task (but if the task is already
            # scheduled in case of a unique task, don't prolong
            # the schedule date).
            when = time.time() + self.config["LOCK_RETRY"]
            job._move(
                from_state=ACTIVE,
                to_state=SCHEDULED,
                when=when,
                mode="min",
            )
            
        if self.stats_thread:
            self.stats_thread.report_task_start()

        success = True
        if self.stats_thread:
            self.stats_thread.report_task_end()

        if lock:
            try:
                lock.release()
            except LockError:
                log.warning("could not release lock", lock=lock.name)
        return success

    def _finish_job_processing(
        self, queue: str, task: Task, success: bool, start_time: float
    ) -> None:
       
        log: BoundLogger = self.log.bind(
            queue=queue, func=task.serialized_func, task_id=task.id
        )

        assert isinstance(log, BoundLogger)

        now = time.time()
        processing_duration = now - start_time
        has_job_timeout = False

        log_context = {
            "func": task.serialized_func,
            "processing_duration": processing_duration,
        }

        def _mark_done() -> None:
            # Remove the task from active queue
            task._move(from_state=ACTIVE)
            log.info("done", **log_context)
        _mark_done()
       
    def _worker_run(self) -> None:
        processed_count = self._process_from_queue(self._queue)
    
        if self._stop_requested:
            self.log.info("stop requested, waiting for task to finish")
            return
        if processed_count > 0:
            self._did_work = True
        if (
            time.time() - self._last_task_check > self.config["SELECT_TIMEOUT"]
            and not self._stop_requested
        ):
            self._worker_queue_scheduled_tasks()
            self._worker_queue_expired_tasks()
            self._last_task_check = time.time()

    def _key(self, *parts: str) -> str:
        return ":".join([self.redis_prefix] + list(parts))

    def _queue_periodic_tasks(self) -> None:
        # Only queue periodic tasks for queues this worker is responsible
        # for.
        funcs = [
            f
            for f in self.tiger.periodic_task_funcs.values()
            if self._filter_queues([Task.queue_from_function(f, self.tiger)])
        ]

        if not funcs:
            return

        for func in funcs:
            # Check if task is queued (in scheduled/active queues).
            task = Task(self.tiger, func)

            # Since periodic tasks are unique, we can use the ID to look up
            # the task.
            pipeline = self.tiger.connection.pipeline()
            for state in [QUEUED, ACTIVE, SCHEDULED]:
                pipeline.zscore(self.tiger._key(state, task.queue), task.id)
            results = pipeline.execute()

            # Task is already queued, scheduled, or running.
            if any(results):
                self.log.info(
                    "periodic task already in queue",
                    func=task.serialized_func,
                    result=results,
                )
                continue

            # We can safely queue the task here since periodic tasks are
            # unique.
            when = task._queue_for_next_period()
            self.log.info(
                "queued periodic task", func=task.serialized_func, when=when
            )

    def store_task_execution(self, tasks: List[Task], execution: Dict) -> None:
        serialized_execution = json.dumps(execution)

        for task in tasks:
            executions_key = self._key("task", task.id, "executions")
            executions_count_key = self._key(
                "task", task.id, "executions_count"
            )

            pipeline = self.connection.pipeline()
            pipeline.incr(executions_count_key)
            pipeline.rpush(executions_key, serialized_execution)

            if task.max_stored_executions:
                pipeline.ltrim(executions_key, -task.max_stored_executions, -1)

            pipeline.execute()

    def run(self) -> None:
        self.log.info("worker start run")
        if not self.scripts.can_replicate_commands:
            # Older Redis versions may create additional overhead when
            # executing pipelines.
            self.log.warning("using old Redis version")

        stats_interval = self.config["STATS_INTERVAL"]
        if stats_interval:
            stats_thread = StatsThread(stats_interval)
            self.stats_thread = stats_thread
            stats_thread.start()

        if self.config["POLL_TASK_QUEUES_INTERVAL"]:
            self._pubsub: Optional[PubSub] = None
        else:
            self._pubsub = self.connection.pubsub()
            assert self._pubsub is not None
            self._pubsub.subscribe(self._key("activity"))

        try:
            while True:
                
                if self._pubsub:
                    self._pubsub_for_queues(
                        timeout=self.config["SELECT_TIMEOUT"],
                        batch_timeout=self.config["SELECT_BATCH_TIMEOUT"],
                    )
                else:
                    self._poll_for_queues()

                self._install_signal_handlers()
                self._did_work = False
                self._worker_run()
                self._uninstall_signal_handlers()
                if self._stop_requested:
                    raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass

        except Exception:
            self.log.exception("exception")
            raise

        finally:
            if self.stats_thread:
                self.stats_thread.stop()
                self.stats_thread = None

            # Free up Redis connection
            if self._pubsub:
                self._pubsub.reset()
            self.log.info("done")
