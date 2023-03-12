import asyncio
import datetime

from redis import Redis
from sqlalchemy.orm import Session
from transitions.extensions.asyncio import AsyncMachine

from dagops import constant
from dagops.state import models
from dagops.state import schemas
from dagops.state.status import TaskStatus
from dagops.state.status import WorkerTaskStatus


class Task:
    def __init__(
        self,
        db_obj: models.Task,
        db: Session,
        redis: Redis,
    ) -> None:
        db.refresh(db_obj)
        self.db_obj = db_obj
        self.db = db
        self.redis = redis
        self.machine = AsyncMachine(
            model=self,
            states=TaskStatus,
            initial=TaskStatus.PENDING,
        )

        # MVP: no wait for cache path release
        # todo: add WAIT_CACHE_PATH_RELEASE -> QUEUED_CACHE_CHECK transition

        self.machine.add_transition('wait_upstream', TaskStatus.PENDING, TaskStatus.WAIT_UPSTREAM, conditions=['is_dag'], after=['update_started_at', 'update_db']) # if dag - wait upstream w/o cache check

        self.machine.add_transition('try_queue_cache_check', TaskStatus.PENDING, TaskStatus.WAIT_CACHE_PATH_RELEASE, conditions=['is_cache_path_locked'], after=['update_db'])
        # self.machine.add_transition('try_queue_cache_check', TaskStatus.PENDING, TaskStatus.QUEUED_CACHE_CHECK, conditions=['is_cache_can_be_checked'], after=['send_message_to_worker2', 'update_db'])
        self.machine.add_transition('try_queue_cache_check', TaskStatus.PENDING, TaskStatus.WAIT_UPSTREAM, after=['update_db']) # if not is_cache_can_be_checked - wait upstream w/o cache check

        self.machine.add_transition('queue_cache_check', TaskStatus.PENDING, TaskStatus.QUEUED_CACHE_CHECK, after=['update_db'])



        self.machine.add_transition('run_cache_check', TaskStatus.QUEUED_CACHE_CHECK, TaskStatus.CACHE_CHECK_RUNNING, after=['update_db'])

        # self.machine.add_transition('check_cache', TaskStatus.CACHE_CHECK_RUNNING, TaskStatus.SUCCESS, conditions=['is_cache_path_exists'], after=['update_db'])
        # self.machine.add_transition('check_cache', TaskStatus.CACHE_CHECK_RUNNING, TaskStatus.FAILED, conditions=['is_cache_check_failed'], after=['update_db'])
        # self.machine.add_transition('check_cache', TaskStatus.CACHE_CHECK_RUNNING, TaskStatus.WAIT_UPSTREAM, after=['update_db'])

        self.machine.add_transition('cache_exists', TaskStatus.CACHE_CHECK_RUNNING, TaskStatus.SUCCESS, after=['update_db'])
        self.machine.add_transition('cache_check_failed', TaskStatus.CACHE_CHECK_RUNNING, TaskStatus.FAILED, after=['update_db'])
        self.machine.add_transition('cache_not_exists', TaskStatus.CACHE_CHECK_RUNNING, TaskStatus.WAIT_UPSTREAM, after=['update_db'])

        self.machine.add_transition('check_upstream', TaskStatus.WAIT_UPSTREAM, TaskStatus.SUCCESS, conditions=['all_upstream_success', 'is_dag'], after='update_db')
        self.machine.add_transition('check_upstream', TaskStatus.WAIT_UPSTREAM, TaskStatus.QUEUED_RUN, conditions=['all_upstream_success'], after=['update_db', 'send_message_to_worker'])
        self.machine.add_transition('check_upstream', TaskStatus.WAIT_UPSTREAM, TaskStatus.FAILED, conditions=['any_upstream_failed'], after='update_db')

        self.machine.add_transition('run', TaskStatus.QUEUED_RUN, TaskStatus.RUNNING, unless=['is_dag'], after=['update_started_at', 'update_db'])
        self.machine.add_transition('run', TaskStatus.QUEUED_RUN, TaskStatus.RUNNING, after='update_db')

        self.machine.add_transition('succeed', TaskStatus.RUNNING, TaskStatus.SUCCESS, after='update_db')
        self.machine.add_transition('fail', TaskStatus.RUNNING, TaskStatus.FAILED, after='update_db')
        self.machine.add_transition('cancel', '*', TaskStatus.CANCELED)

        self.machine.on_enter_RUNNING('add_running_worker')
        for method in ('delete_running_worker', 'update_stopped_at'):
            self.machine.on_enter_FAILED(method)
            self.machine.on_enter_SUCCESS(method)

    def is_cache_can_be_checked(self, **kwargs) -> bool:
        return self.db_obj.input_data.get('exists_command') is not None

    def is_cache_path_locked(self, **kwargs) -> bool:
        return False
        # return self.db_obj.cache_path_locked

    def is_cache_path_exists(self, **kwargs) -> bool:
        return False

    def is_cache_check_failed(self, **kwargs) -> bool:
        return False

    def all_upstream_success(self, upstream: list[models.Task], **kwargs) -> bool:
        return all(u.status == TaskStatus.SUCCESS for u in upstream)

    def any_upstream_failed(self, upstream: list[models.Task], **kwargs) -> bool:
        return any(u.status == TaskStatus.FAILED for u in upstream)

    def is_dag(self, **kwargs) -> bool:
        return self.db_obj.type == 'dag'

    def update_db(self, **kwargs):
        obj = schemas.TaskUpdate(
            status=self.state,
            **kwargs,
        )
        for key, value in obj.dict(exclude_unset=True).items():
            setattr(self.db_obj, key, value)
        self.db.commit()
        self.db.refresh(self.db_obj)

    def delete_running_worker(self, **kwargs):
        self.db_obj.running_worker_id = None

    def add_running_worker(self, **kwargs):
        self.db_obj.running_worker_id = self.db_obj.worker_id

    def update_started_at(self, **kwargs):
        self.db_obj.started_at = datetime.datetime.utcnow()

    def update_stopped_at(self, **kwargs):
        self.db_obj.stopped_at = datetime.datetime.utcnow()

    async def send_message_to_worker(self, **kwargs):
        await self.redis.lpush(
            f'{constant.QUEUE_TASK}:{self.db_obj.worker.name}',
            schemas.TaskMessage(
                id=str(self.db_obj.id),
                input_data=self.db_obj.input_data,
                daemon_id=str(self.db_obj.daemon_id),
            ).json(),
        )

    async def send_message_to_worker2(self, **kwargs):
        """
        send check cache message to worker
        should it be added to db?
        """
        input_data = {
            'command': self.db_obj.input_data['exists_command'],
            'env': self.db_obj.input_data['exists_env'],
            'is_cache_check': True,
        }
        await self.redis.lpush(
            f'{constant.QUEUE_TASK}:{self.db_obj.worker.name}',
            schemas.TaskMessage(
                # id=str(self.db_obj.id),
                id=f'cache-{self.db_obj.id}',
                input_data=input_data,
                daemon_id=str(self.db_obj.daemon_id),
            ).json(),
        )


class WorkerTask:
    def __init__(
        self,
        task: schemas.TaskMessage,
        worker,
        redis: Redis,
    ) -> None:
        self.task = task
        self.worker = worker
        self.redis = redis
        self.machine = AsyncMachine(
            model=self,
            states=WorkerTaskStatus,
            initial=WorkerTaskStatus.QUEUED,
        )

        self.machine.add_transition('run', WorkerTaskStatus.QUEUED, WorkerTaskStatus.RUNNING, after=['send_message_to_daemon'])
        self.machine.add_transition('succeed', WorkerTaskStatus.RUNNING, WorkerTaskStatus.SUCCESS, after='complete_task')
        self.machine.add_transition('fail', WorkerTaskStatus.RUNNING, WorkerTaskStatus.FAILED, after='complete_task')

    async def send_message_to_daemon(self):
        task = self.task
        worker = self.worker
        print('run_tasks_from_queue', task)
        worker.aiotask_to_task_id[asyncio.create_task(worker.run_task(task))] = task.id
        pipeline = self.redis.pipeline()
        pipeline.lpush(worker.aio_tasks_channel, task.id)
        pipeline.lpush(
            f'{constant.QUEUE_TASK_STATUS}:{task.daemon_id}', schemas.WorkerTaskStatusMessage(
                id=task.id,
                input_data=self.task.input_data,
                status=WorkerTaskStatus.RUNNING,
            ).json(),
        )
        await pipeline.execute()

    async def complete_task(self, returncode: int):
        task_id = self.task.id
        status_message = schemas.WorkerTaskStatusMessage(
            id=task_id,
            input_data=self.task.input_data,
            output_data={'returncode': returncode},
            status=self.state,
        )
        daemon_id = self.task.daemon_id
        await self.redis.lpush(f'{constant.QUEUE_TASK_STATUS}:{daemon_id}', status_message.json())
        print('EXITING TASK', status_message)
