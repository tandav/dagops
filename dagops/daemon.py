import asyncio
import datetime
import os
from pathlib import Path
from typing import Callable

import aiofiles.os
from sqlalchemy.orm import Session

from dagops import constant
from dagops.dag import Dag
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud
from dagops.task import Task
from dagops.task_status import TaskStatus


class Daemon:
    def __init__(
        self,
        watch_path: str,
        db: Session,
        create_dag_func: Callable[[str], schemas.InputDataDag],
    ):
        self.watch_path = Path(watch_path)
        self.db = db
        self.create_dag_func = create_dag_func
        self.dags_pending_queue = asyncio.Queue(maxsize=32)
        self.pending_queue = asyncio.Queue(maxsize=32)
        self.aiotask_to_dag = {}
        self.aiotask_to_task = {}
        self.aiotask_to_task_id = {}

    async def handle_tasks_new(self):
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
                                updated_at=datetime.datetime.now(),
                            ),
                        )
                        break
                if all_upstream_success:
                    now = datetime.datetime.now()
                    if task.task_type == 'dag':
                        task_crud.update_by_id(
                            self.db,
                            task.id,
                            schemas.TaskUpdate(
                                status=TaskStatus.SUCCESS,
                                updated_at=now,
                                started_at=now,
                            ),
                        )
                    elif task.task_type == 'shell':
                        task_crud.update_by_id(
                            self.db,
                            task.id,
                            schemas.TaskUpdate(
                                status=TaskStatus.RUNNING,
                                updated_at=now,
                                started_at=now,
                            ),
                        )
                        self.aiotask_to_task_id[asyncio.create_task(self.run_tasks(task))] = task.id
                    else:
                        raise NotImplementedError(f'unsupported task_type {task.task_type}')

            if not self.aiotask_to_task_id:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue

            done, running = await asyncio.wait(self.aiotask_to_task_id, return_when=asyncio.FIRST_COMPLETED)
            for aiotask in done:
                p = aiotask.result()
                assert p.returncode is not None
                status = TaskStatus.SUCCESS if p.returncode == 0 else TaskStatus.FAILED

                task_id = self.aiotask_to_task_id[aiotask]
                stopped_at = datetime.datetime.now()

                task_crud.update_by_id(
                    self.db,
                    task_id,
                    schemas.TaskUpdate(
                        status=status,
                        stopped_at=stopped_at,
                        updated_at=stopped_at,
                        returncode=p.returncode,
                    ),
                )
                del self.aiotask_to_task_id[aiotask]
                # await task.dag.done_queue.put(task)
                print('EXITING TASK', task_id, status)
            await asyncio.sleep(constant.SLEEP_TIME)

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

    async def handle_pending_tasks(self):
        while True:
            if self.pending_queue.empty():
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            while not self.pending_queue.empty():
                task = await self.pending_queue.get()
                self.aiotask_to_task[asyncio.create_task(task.run())] = task

    async def handle_tasks(self):
        while True:
            if not self.aiotask_to_task:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue

            done, running = await asyncio.wait(self.aiotask_to_task, return_when=asyncio.FIRST_COMPLETED)
            for task_aiotask in done:
                p = task_aiotask.result()
                assert p.returncode is not None
                task = self.aiotask_to_task[task_aiotask]
                status = TaskStatus.SUCCESS if p.returncode == 0 else TaskStatus.FAILED
                stopped_at = datetime.datetime.now()

                task_crud.update_by_id(
                    self.db,
                    task.id,
                    schemas.TaskUpdate(
                        status=status,
                        stopped_at=stopped_at,
                        updated_at=stopped_at,
                        duration=(stopped_at - task.started_at).seconds,
                        returncode=p.returncode,
                    ),
                )
                del self.aiotask_to_task[task_aiotask]
                await task.dag.done_queue.put(task)
                print('EXITING TASK', task.id, task.status)
            await asyncio.sleep(constant.SLEEP_TIME)

    async def handle_pending_dags(self):
        while True:
            if self.dags_pending_queue.empty():
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            while not self.dags_pending_queue.empty():
                dag = await self.dags_pending_queue.get()
                self.aiotask_to_dag[asyncio.create_task(dag.run(self.pending_queue))] = dag

    async def handlers_dags(self):
        while True:
            if not self.aiotask_to_dag:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue

            print(self.aiotask_to_dag)
            done, running = await asyncio.wait(self.aiotask_to_dag, return_when=asyncio.FIRST_COMPLETED)
            print('done', done)
            for dag_aiotask in done:
                dag = self.aiotask_to_dag[dag_aiotask]
                status = TaskStatus.SUCCESS if all(t.status == TaskStatus.SUCCESS for t in dag.tasks) else TaskStatus.FAILED
                success_tasks = sum(1 for t in dag.tasks if t.status == TaskStatus.SUCCESS)
                status = TaskStatus.SUCCESS if success_tasks == len(dag.tasks) else TaskStatus.FAILED

                stopped_at = datetime.datetime.now()
                dag_crud.update_by_id(
                    self.db,
                    dag.id,
                    schemas.DagUpdate(
                        status=status,
                        stopped_at=stopped_at,
                        updated_at=stopped_at,
                        duration=(stopped_at - dag.started_at).seconds,
                        success_tasks=f'{success_tasks}/{len(dag.tasks)}',
                    ),
                )
                del self.aiotask_to_dag[dag_aiotask]
            await asyncio.sleep(constant.SLEEP_TIME)

    @staticmethod
    def validate_dag(dag: dict[schemas.ShellTaskInputData, list[schemas.ShellTaskInputData]]):
        for task, deps in dag.items():
            for dep in deps:
                if dep not in dag:
                    raise ValueError(f'dependency {dep} of task {task} not in dag')

    @staticmethod
    def prepare_dag(graph: dict[Task, list[Task]]) -> schemas.DagCreate:
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

    def create_dag(self, file: str) -> Dag:
        dag = self.create_dag_func(file)
        self.validate_dag(dag)
        dag = self.prepare_dag(dag)
        dag_head_task = dag_crud.create(self.db, dag)
        print('dag for file', file, 'created')
        return dag_head_task

    async def update_files_dags(self) -> None:
        """create dags for new files"""
        while True:
            files = file_crud.read_many(self.db)
            for file in files:
                if file.dag_id is None:
                    print('dag for file', file.path, 'start creating...')
                    dag_head_task = self.create_dag(file.path)
                    # await self.dags_pending_queue.put(dag)
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag_head_task.id))
            await asyncio.sleep(constant.SLEEP_TIME)

    async def watch_directory(self):
        while True:
            files = {str(self.watch_path / p) for p in await aiofiles.os.listdir(self.watch_path)}
            stale_files_ids = set()
            up_to_date_files_paths = set()
            for file in file_crud.read_many(self.db):
                if file.path in files:
                    up_to_date_files_paths.add(file.path)
                else:
                    stale_files_ids.add(file.id)
            file_crud.delete_many_by_ids(self.db, stale_files_ids)
            file_crud.create_many(self.db, [schemas.FileCreate(path=file) for file in files - up_to_date_files_paths])
            await asyncio.sleep(constant.SLEEP_TIME)

    async def cancel_orphaned(self):
        # tasks
        orphaned = task_crud.read_by_field_isin(self.db, 'status', [TaskStatus.PENDING, TaskStatus.RUNNING])
        for task in orphaned:
            print('canceling orphaned task', task.id)
            now = datetime.datetime.now()
            task_crud.update_by_id(
                self.db, task.id, schemas.TaskUpdate(
                    status=TaskStatus.CANCELED,
                    stopped_at=now,
                    updated_at=now,
                ),
            )

    async def __call__(self):
        await self.cancel_orphaned()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.watch_directory())
            tg.create_task(self.update_files_dags())
            tg.create_task(self.handle_tasks_new())
            # tg.create_task(self.handle_pending_tasks())
            # tg.create_task(self.handle_pending_dags())
            # tg.create_task(self.handle_tasks())
            # tg.create_task(self.handlers_dags())
