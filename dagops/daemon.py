import asyncio
import datetime
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
from dagops.task_status import TaskStatus


class Daemon:
    def __init__(
        self,
        watch_path: str,
        db: Session,
        create_dag_func: Callable[[str, Session], Dag],
    ):
        self.watch_path = Path(watch_path)
        self.db = db
        self.create_dag_func = create_dag_func
        self.dags_pending_queue = asyncio.Queue(maxsize=32)
        self.pending_queue = asyncio.Queue(maxsize=32)
        self.aiotask_to_dag = {}
        self.aiotask_to_task = {}

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

    def create_dag(self, file: str) -> Dag:
        return self.create_dag_func(file, self.db)

    async def update_files_dags(self) -> None:
        """create dags for new files"""
        while True:
            files = file_crud.read_many(self.db)
            for file in files:
                if file.dag_id is None:
                    print('dag for file', file.path, 'start creating...')
                    dag = self.create_dag(file.path)
                    await self.dags_pending_queue.put(dag)
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag.id))
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
            task_crud.update_by_id(self.db, task.id, schemas.TaskUpdate(status=TaskStatus.CANCELED))

        # dags
        orphaned = dag_crud.read_by_field_isin(self.db, 'status', [TaskStatus.PENDING, TaskStatus.RUNNING])
        for dag in orphaned:
            print('canceling orphaned dag', dag.id)
            dag_crud.update_by_id(self.db, dag.id, schemas.DagUpdate(status=TaskStatus.CANCELED))

    async def __call__(self):
        await self.cancel_orphaned()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.watch_directory())
            tg.create_task(self.update_files_dags())
            tg.create_task(self.handle_pending_tasks())
            tg.create_task(self.handle_pending_dags())
            tg.create_task(self.handle_tasks())
            tg.create_task(self.handlers_dags())
