import asyncio
import datetime
import os
from pathlib import Path
from typing import Callable

import aiofiles.os
import redis.asyncio as redis
from sqlalchemy.orm import Session

from dagops import constant
from dagops.state import models
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud
from dagops.task_status import TaskStatus


class Daemon:
    def __init__(
        self,
        watch_directory: str,
        db: Session,
        create_dag_func: Callable[[str | list[str]], schemas.InputDataDag],
        batch: bool = False,
    ):
        self.watch_directory = Path(watch_directory)
        self.db = db
        self.create_dag_func = create_dag_func
        self.aiotask_to_task_id = {}
        self.batch = batch
        self.redis = redis.from_url(os.environ['REDIS_URL'])
        self.files_channel = f'files:{self.watch_directory}'
        self.aio_tasks_channel = f'aio_tasks:{self.watch_directory}'
        if MAX_N_SUCCESS := os.environ.get('MAX_N_SUCCESS'):
            self.max_n_success = int(MAX_N_SUCCESS)
        else:
            self.max_n_success = None

    async def handle_tasks(self):  # noqa: C901
        while True:
            for task in task_crud.read_by_field(self.db, 'status', TaskStatus.PENDING):
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
                    now = datetime.datetime.now(tz=datetime.UTC)
                    if task.task_type == 'dag':
                        task_crud.update_by_id(
                            self.db,
                            task.id,
                            schemas.TaskUpdate(
                                status=TaskStatus.SUCCESS,
                                started_at=min(u.started_at for u in task.upstream),
                                stopped_at=now,
                                running_worker_id=None,
                            ),
                        )
                    elif task.task_type == 'shell':
                        if len(task.worker.running_tasks) >= task.worker.maxtasks:
                            print(f'worker {task.worker.name} is busy, skipping task {task.id} {len(task.worker.running_tasks)=}')
                            continue
                        task_crud.update_by_id(
                            self.db,
                            task.id,
                            schemas.TaskUpdate(
                                status=TaskStatus.RUNNING,
                                started_at=now,
                                running_worker_id=task.worker.id,
                            ),
                        )
                        self.aiotask_to_task_id[asyncio.create_task(self.run_tasks(task))] = task.id
                        await self.redis.publish(self.aio_tasks_channel, str(task.id))
                    else:
                        raise NotImplementedError(f'unsupported task_type {task.task_type}')

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

    async def handle_aio_tasks(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.aio_tasks_channel)
        while True:
            message = await pubsub.get_message(timeout=None)
            print(message)
            if not self.aiotask_to_task_id:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            done, running = await asyncio.wait(self.aiotask_to_task_id, return_when=asyncio.FIRST_COMPLETED)
            for aiotask in done:
                p = aiotask.result()
                assert p.returncode is not None
                status = TaskStatus.SUCCESS if p.returncode == 0 else TaskStatus.FAILED

                task_id = self.aiotask_to_task_id[aiotask]
                stopped_at = datetime.datetime.now(tz=datetime.UTC)

                task_crud.update_by_id(
                    self.db,
                    task_id,
                    schemas.TaskUpdate(
                        status=status,
                        stopped_at=stopped_at,
                        output_data={'returncode': p.returncode},
                        running_worker_id=None,
                    ),
                )
                del self.aiotask_to_task_id[aiotask]
                print('EXITING TASK', task_id, status)
            # await asyncio.sleep(constant.SLEEP_TIME)

    async def run_tasks(self, task):
        with open(f'{os.environ["LOGS_DIRECTORY"]}/{task.id}.txt', 'w') as logs_fh:
            p = await asyncio.create_subprocess_exec(
                *task.input_data['command'],
                env=task.input_data['env'],
                stdout=logs_fh,
                stderr=asyncio.subprocess.STDOUT,
            )
            await p.communicate()
            return p

    @staticmethod
    def validate_dag(dag: dict[schemas.ShellTaskInputData, list[schemas.ShellTaskInputData]]):
        for task, deps in dag.items():
            for dep in deps:
                if dep not in dag:
                    raise ValueError(f'dependency {dep} of task {task} not in dag')

    @staticmethod
    def prepare_dag(graph: schemas.InputDataDag) -> schemas.DagCreate:
        input_data = [None] * len(graph)
        task_to_id = {}
        for i, task in enumerate(graph):
            task_to_id[task] = i
            input_data[i] = task
        id_graph = {}
        for task, deps in graph.items():
            id_graph[task_to_id[task]] = [task_to_id[dep] for dep in deps]
        dag = schemas.DagCreate(
            task_type='shell',
            tasks_input_data=input_data,
            graph=id_graph,
        )
        return dag

    def create_dag(self, file: str) -> models.Task:
        dag = self.create_dag_func(file)
        self.validate_dag(dag)
        dag = self.prepare_dag(dag)
        dag_head_task = dag_crud.create(self.db, dag)
        print('dag for file', file, 'created')
        return dag_head_task

    async def update_files_dags(self) -> None:
        """create dags for new files"""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.files_channel)
        while True:
            message = await pubsub.get_message(timeout=None)
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
            files = set(await aiofiles.os.listdir(self.watch_directory)) - exclude
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
                            directory=str(self.watch_directory),
                            file=file,
                        )
                        for file in new_files
                    ],
                )
                await self.redis.publish(self.files_channel, str(len(new_files)))
            else:
                await asyncio.sleep(constant.SLEEP_TIME)

    async def cancel_orphans(self):
        orphans = self.db.query(models.Task).filter(models.Task.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING])).all()
        if not orphans:
            return
        print(f'canceling {len(orphans)} orphans tasks...')
        for task in orphans:
            now = datetime.datetime.now(tz=datetime.UTC)
            task.status = TaskStatus.CANCELED
            if task.started_at is not None:
                task.stopped_at = now
            task.running_worker_id = None
        self.db.commit()
        print(f'canceling {len(orphans)} orphans tasks... done')

    async def __call__(self):
        await self.cancel_orphans()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.do_watch_directory())
            tg.create_task(self.update_files_dags())
            tg.create_task(self.handle_tasks())
            tg.create_task(self.handle_aio_tasks())
