import asyncio
import datetime
import os
import uuid
from pathlib import Path
from typing import Callable
from typing import Literal

import aiofiles.os
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from dagops import constant
from dagops import fsm
from dagops.dag import Dag
from dagops.state import models
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud
from dagops.state.status import TaskStatus
from dagops.state.status import WorkerTaskStatus


class Daemon:
    def __init__(
        self,
        watch_directory: str,
        db: Session,
        redis: Redis,
        create_dag_func: Callable[[str | list[str]], Dag],
        batch: bool = False,
        storage: Literal['filesystem', 'redis'] = 'filesystem',
    ):
        if storage not in {'filesystem', 'redis'}:
            raise ValueError(f'unsupported storage={storage} It must be filesystem or redis')
        self.id = uuid.uuid4()
        self.watch_directory = Path(watch_directory)
        self.db = db
        self.redis = redis
        self.create_dag_func = create_dag_func
        self.batch = batch
        self.storage = storage
        self.files_channel = f'{constant.CHANNEL_FILES}:{self.watch_directory}'
        self.fsm_tasks = {}
        if MAX_N_SUCCESS := os.environ.get('MAX_N_SUCCESS'):  # this is used for testing only
            self.max_n_success = int(MAX_N_SUCCESS)
        else:
            self.max_n_success = None

    async def handle_worker_messages(self):
        while True:
            kv = await self.redis.brpop(f'{constant.QUEUE_TASK_STATUS}:{self.id}', timeout=constant.SLEEP_TIME)
            if kv is not None:
                _, message = kv
                print(self.watch_directory, 'handle_tasks', message)
                worker_task_status = schemas.WorkerTaskStatusMessage.parse_raw(message)

                task_id = uuid.UUID(worker_task_status.id)
                fsm_task = self.fsm_tasks[task_id]
                if worker_task_status.status == WorkerTaskStatus.RUNNING:
                    await fsm_task.run()
                if worker_task_status.status == WorkerTaskStatus.SUCCESS:
                    await fsm_task.succeed(output_data=worker_task_status.output_data)
                    del self.fsm_tasks[task_id]
                if worker_task_status.status == WorkerTaskStatus.FAILED:
                    await fsm_task.fail(output_data=worker_task_status.output_data)
                    del self.fsm_tasks[task_id]

    async def handle_tasks(self):
        while True:
            for task in (
                self
                .db
                .query(models.Task)
                .filter(models.Task.daemon_id == self.id)
                .filter(models.Task.status == TaskStatus.WAIT_UPSTREAM)
                .all()
            ):
                await self.fsm_tasks[task.id].check_upstream(upstream=task.upstream)
            await asyncio.sleep(constant.SLEEP_TIME)

    async def create_dag(self, file: str) -> models.Task:
        dag = self.create_dag_func(file)
        dag_head_task, tasks = dag_crud.create(
            self.db, schemas.DagCreate(
                type='shell',
                tasks_input_data=dag.input_data,
                graph=dag.id_graph,
                daemon_id=self.id,
            ),
        )
        for task in tasks:  # tasks include dag_head_task and all it's deps
            fsm_task = fsm.Task(task, self.db, self.redis)
            await fsm_task.wait_upstream()
            self.fsm_tasks[task.id] = fsm_task
        print('dag for file', file, 'created')
        return dag_head_task

    async def update_files_dags(self) -> None:
        """create dags for new files"""
        while True:
            _, message = await self.redis.brpop(self.files_channel)
            print(message)
            files = file_crud.read_by_field(self.db, 'directory', str(self.watch_directory))
            files = [file for file in files if file.dag_id is None]
            if not self.batch:
                for file in files:
                    print(f'creating dag for file {file.directory}/{file.file}...')
                    dag_head_task = await self.create_dag(file.file)
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag_head_task.id))
            elif files:
                print(f'batch dag for {len(files)} files creating...')
                dag_head_task = await self.create_dag([file.file for file in files])
                for file in files:
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag_head_task.id))

    async def do_watch_directory(
        self,
        exclude: frozenset[str] = constant.default_files_exclude,
    ) -> None:
        while True:
            if self.storage == 'filesystem':
                files = set(await aiofiles.os.listdir(self.watch_directory)) - exclude
            elif self.storage == 'redis':
                files = {k.removeprefix(str(self.watch_directory)) for k in await self.redis.keys(str(self.watch_directory) + '*')} - exclude

            stale_files_ids = set()
            up_to_date_files_paths = set()
            for file in file_crud.read_by_field(self.db, 'directory', str(self.watch_directory)):
                if file.file in files:
                    up_to_date_files_paths.add(file.file)
                else:
                    stale_files_ids.add(file.id)
            if stale_files_ids:
                print(f'deleting {len(stale_files_ids)} stale files...')
                file_crud.delete_by_field_isin(self.db, 'id', stale_files_ids)
            new_files = files - up_to_date_files_paths
            if new_files:
                print(f'creating {len(new_files)} new files...')
                file_crud.create_many(
                    self.db, [
                        schemas.FileCreate(
                            storage=self.storage,
                            directory=str(self.watch_directory),
                            file=file,
                        )
                        for file in new_files
                    ],
                )
                await self.redis.lpush(self.files_channel, str(len(new_files)))
            else:
                await asyncio.sleep(constant.SLEEP_TIME)

    async def cancel_orphans(self):
        pipeline = self.redis.pipeline()
        pipeline.delete(self.files_channel)
        pipeline.delete(f'{constant.QUEUE_TASK}:*')
        pipeline.delete(f'{constant.QUEUE_TASK_STATUS}:*')
        pipeline.delete(f'{constant.CHANNEL_AIO_TASKS}:*')
        await pipeline.execute()
        orphans = self.db.query(models.Task).filter(models.Task.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING])).all()
        if not orphans:
            return
        print(f'canceling {len(orphans)} orphans tasks...')
        for task in orphans:
            now = datetime.datetime.utcnow()
            task.status = TaskStatus.CANCELED
            if task.started_at is not None:
                task.stopped_at = now
            task.running_worker_id = None
        self.db.commit()
        print(f'canceling {len(orphans)} orphans tasks... done')

    async def check_max_n_success(self):
        while True:
            n_success = task_crud.n_success(self.db)
            print(f'{n_success=} / {self.max_n_success=}')
            if n_success == self.max_n_success:
                print('MAX_N_SUCCESS reached, exiting')
                for task in asyncio.all_tasks():
                    task.cancel()
                raise SystemExit
            elif n_success > self.max_n_success:
                raise RuntimeError(f'n_success={n_success} > max_n_success={self.max_n_success}')
            await asyncio.sleep(constant.SLEEP_TIME)

    async def __call__(self):
        await self.cancel_orphans()

        aws = [
            self.do_watch_directory(),
            self.update_files_dags(),
            self.handle_worker_messages(),
            self.handle_tasks(),
        ]
        if self.max_n_success is not None:
            aws.append(self.check_max_n_success())

        await asyncio.gather(*aws)
