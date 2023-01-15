import asyncio
import datetime
import sys

import aiofiles.os

from dagops import constant
from dagops.dag import Dag
from dagops.dependencies import get_db_cm
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud
from dagops.task import ShellTask
from dagops.task_status import TaskStatus


class AsyncWatcher:
    def __init__(self, watch_path: str, db):
        self.watch_path = watch_path
        self.db = db
        self.dags_pending_queue = asyncio.Queue(maxsize=32)
        self.pending_queue = asyncio.Queue(maxsize=32)
        self.dag_to_aiotask = {}
        self.task_to_aiotask = {}

    async def handle_pending_queue(self):
        while True:
            if self.pending_queue.empty():
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            while not self.pending_queue.empty():
                task = await self.pending_queue.get()
                self.task_to_aiotask[task] = asyncio.create_task(task.run())

    async def handle_tasks(self):
        while True:
            if not self.task_to_aiotask:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            aiotask_to_task = {task_aiotask: task for task, task_aiotask in self.task_to_aiotask.items()}

            done, running = await asyncio.wait(aiotask_to_task, return_when=asyncio.FIRST_COMPLETED)
            for aio_task in done:
                p = aio_task.result()
                assert p.returncode is not None
                task = aiotask_to_task[aio_task]
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
                del self.task_to_aiotask[task]
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
                self.dag_to_aiotask[dag] = asyncio.create_task(dag.run())

    async def handlers_dags(self):
        while True:
            if not self.dag_to_aiotask:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue

            print(self.dag_to_aiotask)
            aiotask_to_dag = {dag_aiotask: dag for dag, dag_aiotask in self.dag_to_aiotask.items()}
            done, running = await asyncio.wait(aiotask_to_dag, return_when=asyncio.FIRST_COMPLETED)
            print('done', done)
            for dag_aiotask in done:
                dag = aiotask_to_dag[dag_aiotask]
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
                del self.dag_to_aiotask[dag]
            await asyncio.sleep(constant.SLEEP_TIME)

    async def create_dag(self, file: str) -> Dag:
        print('dag for file', file, 'start creating...')
        command = sys.executable, '-u', 'write_to_mongo.py'
        a = ShellTask(self.db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'a'})
        b = ShellTask(self.db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'b'})
        c = ShellTask(self.db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'c'})
        d = ShellTask(self.db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'd'})
        e = ShellTask(self.db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'e'})
        graph = {
            a: [],
            b: [],
            c: [a, b],
            d: [],
            e: [c, d],
        }
        dag = Dag(self.db, graph, self.pending_queue)
        for task in dag.tasks:
            task.dag = dag

        print('dag for file', file, 'created')
        return dag

    async def update_files_dags(self) -> None:
        """create dags for new files"""
        while True:
            files = file_crud.read_many(self.db)
            for file in files:
                if file.dag_id is None:
                    print('dag for file', file.path, 'start creating...')
                    dag = await self.create_dag(file.path)
                    await self.dags_pending_queue.put(dag)
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag.id))
            await asyncio.sleep(constant.SLEEP_TIME)

    async def watch_directory(self):
        while True:
            files = set(await aiofiles.os.listdir(self.watch_path))
            stale_files_ids = set()
            up_to_date_files_paths = set()
            for file in file_crud.read_many(self.db):
                if file.path not in files:
                    stale_files_ids.add(file.id)
                else:
                    up_to_date_files_paths.add(file.path)
            file_crud.delete_many_by_ids(self.db, stale_files_ids)
            file_crud.create_many(self.db, [schemas.FileCreate(path=file) for file in files - up_to_date_files_paths])
            await asyncio.sleep(constant.SLEEP_TIME)

    async def cancel_orphaned(self):
        # tasks
        orphaned = task_crud.read_many_isin(self.db, 'status', [TaskStatus.PENDING, TaskStatus.RUNNING])
        for task in orphaned:
            print('canceling orphaned task', task.id)
            task_crud.update_by_id(self.db, task.id, schemas.TaskUpdate(status=TaskStatus.CANCELED))

        # dags
        orphaned = dag_crud.read_many_isin(self.db, 'status', [TaskStatus.PENDING, TaskStatus.RUNNING])
        for dag in orphaned:
            print('canceling orphaned dag', dag.id)
            dag_crud.update_by_id(self.db, dag.id, schemas.DagUpdate(status=TaskStatus.CANCELED))

    async def main(self):
        await self.cancel_orphaned()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.watch_directory())
            tg.create_task(self.update_files_dags())
            # tg.create_task(self.process_tasks())
            tg.create_task(self.handle_pending_queue())
            tg.create_task(self.handle_pending_dags())
            tg.create_task(self.handle_tasks())
            tg.create_task(self.handlers_dags())


if __name__ == '__main__':
    with get_db_cm() as db:
        watch_path = 'static/records_tmp'
        asyncio.run(
            AsyncWatcher(
                watch_path,
                db=db,
            ).main(),
        )
