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
from dagops.state import models
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud
from dagops.status import TaskStatus
from dagops.status import WorkerTaskStatus


class Daemon:
    def __init__(
        self,
        watch_directory: str,
        db: Session,
        redis: Redis,
        create_dag_func: Callable[[str | list[str]], schemas.InputDataDag],
        batch: bool = False,
        storage: Literal['filesystem', 'redis'] = 'filesystem',
    ):
        if storage not in {'filesystem', 'redis'}:
            raise ValueError(f'unsupported storage={storage} It must be filesystem or redis')
        self.storage = storage
        self.id = uuid.uuid4()
        self.watch_directory = Path(watch_directory)
        self.db = db
        self.create_dag_func = create_dag_func
        self.batch = batch
        self.redis = redis
        self.files_channel = f'{constant.CHANNEL_FILES}:{self.watch_directory}'
        self.fsm_tasks = {}
        if MAX_N_SUCCESS := os.environ.get('MAX_N_SUCCESS'):
            self.max_n_success = int(MAX_N_SUCCESS)
        else:
            self.max_n_success = None

    async def handle_worker_messages(self):
        kv = await self.redis.brpop(f'{constant.QUEUE_TASK_STATUS}:{self.id}', timeout=constant.SLEEP_TIME)
        if kv is not None:
            _, message = kv
            print(self.watch_directory, 'handle_tasks', message)
            worker_task_status = schemas.WorkerTaskStatusMessage.parse_raw(message)

            fsm_task = self.fsm_tasks[uuid.UUID(worker_task_status.id)]
            if worker_task_status.status == WorkerTaskStatus.RUNNING:
                fsm_task.run(
                    started_at=datetime.datetime.now(tz=datetime.timezone.utc),
                )
            elif worker_task_status.status in {WorkerTaskStatus.SUCCESS, WorkerTaskStatus.FAILED}:
                kwargs = {
                    'stopped_at': datetime.datetime.now(tz=datetime.timezone.utc),
                    'output_data': worker_task_status.output_data,
                }
                if worker_task_status.status == WorkerTaskStatus.SUCCESS:
                    fsm_task.succeed(**kwargs)
                if worker_task_status.status == WorkerTaskStatus.FAILED:
                    fsm_task.fail(**kwargs)

    async def handle_tasks(self):  # noqa: C901
        while True:
            await self.handle_worker_messages()

            pending = (
                self
                .db
                .query(models.Task)
                .filter(models.Task.daemon_id == self.id)
                .filter(models.Task.status == TaskStatus.PENDING)
                .all()
            )
            for task in pending:
                all_upstream_success = True
                for u in task.upstream:
                    if u.status == TaskStatus.SUCCESS:
                        continue
                    all_upstream_success = False
                    if u.status == TaskStatus.FAILED:
                        task_crud.update_by_id(
                            self.db,
                            task.id,
                            schemas.TaskUpdate(
                                status=TaskStatus.FAILED,
                                running_worker_id=None,
                            ),
                        )
                        break
                if all_upstream_success:
                    if task.type == 'dag':
                        task_crud.update_by_id(
                            self.db,
                            task.id,
                            schemas.TaskUpdate(
                                status=TaskStatus.SUCCESS,
                                started_at=min(u.started_at for u in task.upstream),
                                stopped_at=datetime.datetime.now(tz=datetime.timezone.utc),
                                running_worker_id=None,
                            ),
                        )
                    elif task.type == 'shell':
                        fsm_task = self.fsm_tasks[task.id]
                        fsm_task.queue_run()

                        queue = f'{constant.QUEUE_TASK}:{task.worker.name}'
                        print(self.watch_directory, 'handle_tasks', f'pushing task {task.id} to f{queue}')
                        await self.redis.lpush(
                            queue, schemas.TaskMessage(
                                id=str(task.id),
                                input_data=task.input_data,
                                daemon_id=str(self.id),
                            ).json(),
                        )
                    else:
                        raise NotImplementedError(f'unsupported type {task.type}')

            if self.max_n_success is not None:
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

    @staticmethod
    def validate_dag(dag: dict[schemas.ShellTaskInputData, list[schemas.ShellTaskInputData]]):
        for task, deps in dag.items():
            for dep in deps:
                if dep not in dag:
                    raise ValueError(f'dependency {dep} of task {task} not in dag')

    def prepare_dag(self, graph: schemas.InputDataDag) -> schemas.DagCreate:
        input_data = [None] * len(graph)
        task_to_id = {}
        for i, task in enumerate(graph):
            task_to_id[task] = i
            input_data[i] = task
        id_graph = {}
        for task, deps in graph.items():
            id_graph[task_to_id[task]] = [task_to_id[dep] for dep in deps]
        dag = schemas.DagCreate(
            type='shell',
            tasks_input_data=input_data,
            graph=id_graph,
            daemon_id=self.id,
        )
        return dag

    def create_dag(self, file: str) -> models.Task:
        dag = self.create_dag_func(file)
        self.validate_dag(dag)
        dag = self.prepare_dag(dag)
        dag_head_task, task_ids = dag_crud.create(self.db, dag)
        for task_id in task_ids:
            self.fsm_tasks[task_id] = fsm.Task(self.db, task_id)
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
                    dag_head_task = self.create_dag(file.file)
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag_head_task.id))
            elif files:
                print(f'batch dag for {len(files)} files creating...')
                dag_head_task = self.create_dag([file.file for file in files])
                for file in files:
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag_head_task.id))

    async def do_watch_directory(
        self,
        exclude: frozenset[str] = frozenset({'.DS_Store'}),
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
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            task.status = TaskStatus.CANCELED
            if task.started_at is not None:
                task.stopped_at = now
            task.running_worker_id = None
        self.db.commit()
        print(f'canceling {len(orphans)} orphans tasks... done')

    async def __call__(self):
        await self.cancel_orphans()
        await asyncio.gather(
            self.do_watch_directory(),
            self.update_files_dags(),
            self.handle_tasks(),
        )
