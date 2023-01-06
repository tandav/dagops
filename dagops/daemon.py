import asyncio
import datetime
import json
import sys

import aiofiles.os
import dotenv
from extraredis._async import ExtraRedisAsync
from fastapi import Depends

from dagops.dag import Dag
# from dagops.state import State
from dagops.dependencies import get_db_cm
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud
from dagops.task import ShellTask
from dagops.task import Task
from dagops.task_status import TaskStatus

dotenv.load_dotenv()


class AsyncWatcher:
    def __init__(self, watch_path: str, db):
        self.watch_path = watch_path
        self.pending_queue = asyncio.Queue(maxsize=10)
        self.tasks = {}  # task_id: Task
        self.dags = {}  # dag_id: Dag
        self.running_tasks = {}
        self.running_dags = {}
        self.task_to_dag = {}
        self.extraredis = ExtraRedisAsync(decode_responses=True)
        # self.state = State(self.extraredis)
        self.db = db

    async def run_subprocess(self, cmd, env, logs_fh):
        p = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=logs_fh,
            stderr=asyncio.subprocess.STDOUT,
        )
        await p.communicate()
        return p

    async def handle_tasks(self):
        while True:
            if not self.running_tasks:
                await asyncio.sleep(1)
                continue
            print(self.running_tasks)
            task_to_task_id = {t: k for k, t in self.running_tasks.items()}
            done, running = await asyncio.wait(task_to_task_id, return_when=asyncio.FIRST_COMPLETED)
            for aio_task in done:
                p = aio_task.result()
                assert p.returncode is not None
                task_id = task_to_task_id[aio_task]
                status = TaskStatus.SUCCESS if p.returncode == 0 else TaskStatus.FAILED
                # await self.state.set_task_status(task_id, status)
                # task.stopped_at = datetime.datetime.now()
                stopped_at = datetime.datetime.now()
                await self.extraredis.hset_fields(
                    self.state.TASK_PREFIX, task_id, {
                        'status': status,
                        'stopped_at': str(stopped_at),
                        'duration': (stopped_at - self.tasks[task_id].started_at).seconds,
                        'returncode': p.returncode,
                    },
                )
                # self.logs_handlers[task_id].close()
                # del self.logs_handlers[task_id]
                # assert self.tasks[task_id] in self.task_to_dag[task_id].tasks
                task = self.tasks[task_id]
                task.status = status
                del self.running_tasks[task_id]
                del self.tasks[task_id]
                dag = self.task_to_dag[task_id]
                await dag.done_queue.put(task)
            await asyncio.sleep(1)

    async def handlers_dags(self):
        while True:
            if not self.running_dags:
                await asyncio.sleep(1)
                continue
            print(self.running_dags)
            dag_to_dag_id = {t: k for k, t in self.running_dags.items()}
            done, running = await asyncio.wait(dag_to_dag_id, return_when=asyncio.FIRST_COMPLETED)
            for aio_dag in done:
                # d = aio_dag.result()
                dag_id = dag_to_dag_id[aio_dag]
                dag = self.dags[dag_id]
                status = TaskStatus.SUCCESS if all(t.status == TaskStatus.SUCCESS for t in dag.tasks) else TaskStatus.FAILED
                success_tasks = sum(1 for t in dag.tasks if t.status == TaskStatus.SUCCESS)
                status = TaskStatus.SUCCESS if success_tasks == len(dag.tasks) else TaskStatus.FAILED

                stopped_at = datetime.datetime.now()
                await self.extraredis.hset_fields(
                    self.state.DAG_PREFIX, dag_id, {
                        'status': status,
                        'stopped_at': str(stopped_at),
                        'duration': (stopped_at - dag.started_at).seconds,
                        'success_tasks': f'{success_tasks}/{len(dag.tasks)}',
                    },
                )
                del self.running_dags[dag_id]
                del self.dags[dag_id]
            await asyncio.sleep(1)

        #     task_id = await self.pending_queue.get()
        #     await self.start_task(task_id)

    # async def create_task(self, file: str) -> Task:
    #     cmd = sys.executable, '-u', 'write_to_mongo.py'
    #     env={'TASK_NAME': file}
    #     task = ShellTask(cmd, env)
    #     return task

        # await self.state.set_task_status(task.id, TaskStatus.RUNNING)
        # await self.state.redis.hset(f'{self.state.prefix_tasks}:{task.id}', mapping={
        #     'status': TaskStatus.RUNNING,
        #     'cmd': json.dumps(cmd),
        #     'env': json.dumps(env),
        # })
        # self.processes[task.id] = asyncio.create_task(task.run())

        # key = await self.queue.get()
        # logs_fh = open(f'static/logs/{task_id}.txt', 'w')
        # self.logs_handlers[task_id] = logs_fh
        # task = asyncio.create_task(self.run_subprocess(cmd, env, logs_fh))

            # await asyncio.sleep(1)
            # status = random.choice([TaskStatus.SUCCESS, TaskStatusf.FAILED])
            # await redis.set(key, status)
            # print(await redis.get(key))
            # self.queue.task_done()

    async def start_task(self, task: Task):
        # task = self.tasks[task_id]
        now = datetime.datetime.now()
        task.started_at = now
        await self.extraredis.hset_fields(
            self.state.TASK_PREFIX, task.id, {
                'status': TaskStatus.RUNNING,
                'command': json.dumps(task.command),
                'env': json.dumps(task.env),
                'started_at': str(now),
            },
        )
        dag = self.task_to_dag[task.id]
        dag.started_at = now
        await self.extraredis.hset_fields(
            self.state.DAG_PREFIX, dag.id, {
                'status': TaskStatus.RUNNING,
                'started_at': str(now),
            },
        )

        # await self.state.set_shell_task(task, TaskStatus.RUNNING)
        self.running_tasks[task.id] = asyncio.create_task(task.run())

        # self.running_tasks[task_id] = asyncio.create_task(task.run())
        # task = self.tasks[task_id]
        # self.running_tasks[task_id] = asyncio.create_task(task.run())

    async def stop_task(self):
        pass

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
        # db_dag = dag_crud.create(self.db, schemas.DagCreate(graph=graph))

        pending_queue = asyncio.Queue()
        done_queue = asyncio.Queue()
        dag = Dag(self.db, graph, pending_queue, done_queue)
        # dag = Dag(graph, self.pending_queue, done_queue)
        # dag_tasks = {t.id for t in dag.tasks}

        # for task in dag.tasks:
        #     self.tasks[task.id] = task
        #     self.task_to_dag[task.id] = dag

        # now = datetime.datetime.now()
        # await self.extraredis.redis.sadd(self.state.DAG_SET, dag.id)
        # await self.extraredis.redis.sadd(self.state.TASK_SET, *dag_tasks)
        # await self.extraredis.hset_fields(
        #     self.state.DAG_PREFIX, dag.id, {
        #         'id': dag.id,
        #         'created_at': str(now),
        #         'status': TaskStatus.PENDING,
        #         'graph': json.dumps(dag.id_graph),
        #     },
        # )
        # await self.extraredis.mhset_fields(
        #     self.state.TASK_PREFIX, {
        #         task_id: {
        #             'id': task_id,
        #             'created_at': str(now),
        #             'status': TaskStatus.PENDING,
        #             'dag_id': dag.id,
        #         } for task_id in dag_tasks
        #     },
        # )
        # await self.extraredis.set(self.state.FILE_DAG_PREFIX, file, dag.id)
        # self.running_dags[dag.id] = asyncio.create_task(dag.run())

        print('dag for file', file, 'created')
        return dag

    # async def update_files_tasks(self, files: Iterable[str]) -> None:

    async def update_files_dags(self) -> None:
        """create dags for new files"""
        while True:
            files = file_crud.read_many(self.db)
            for file in files:
                if file.dag_id is None:
                    print('dag for file', file.path, 'start creating...')
                    dag = await self.create_dag(file.path)
                    self.dags[dag.id] = dag
                    file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag.id))
            # files_dags = await self.state.files_dags()
            # to_update = {}

            # for file, dag_id in files_dags.items():
            #     if dag_id is None:
            #         dag = await self.create_dag(file)
            #         self.dags[dag.id] = dag

                    # task = ShellTask(command=[sys.executable, '-u', 'write_to_mongo.py'], env={'TASK_NAME': file})
                    # self.tasks[task.id] = task
                    # to_update[file] = dag.id
            # if len(to_update) == 0:
            #     await asyncio.sleep(1)
            #     continue
            # await self.extraredis.mset(self.state.FILE_DAG_PREFIX, to_update)
            # await self.extraredis.redis.sadd(self.state.DAG_SET, *to_update.values())
            # await self.extraredis.mhset_field(self.state.TASK_PREFIX, 'status', dict.fromkeys(to_update.values(), TaskStatus.PENDING))
            # datetime.datetime.now()
            # await self.extraredis.mhset_fields(
            #     self.state.TASK_PREFIX,
            #     {k: {'created_at': str(now), 'status': TaskStatus.PENDING} for k in to_update.values()},
            # )
            # for dag in to_update.values():
                # await self.pending_queue.put(dag)
                # self.running_dags[dag.id] = asyncio.create_task(dag.run())

            await asyncio.sleep(1)

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
            await asyncio.sleep(1)

    async def cancel_orphaned(self):
        # tasks
        pending = task_crud.read_by_field(self.db, 'status', TaskStatus.PENDING)
        running = task_crud.read_by_field(self.db, 'status', TaskStatus.RUNNING)
        orphaned_tasks = pending + running
        for task in orphaned_tasks:
            task_crud.update_by_id(self.db, task.id, schemas.TaskUpdate(status=TaskStatus.CANCELED))

        # dags
        pending = dag_crud.read_by_field(self.db, 'status', TaskStatus.PENDING)
        running = dag_crud.read_by_field(self.db, 'status', TaskStatus.RUNNING)
        orphaned_dags = pending + running
        for dag in orphaned_dags:
            dag_crud.update_by_id(self.db, dag.id, schemas.DagUpdate(status=TaskStatus.CANCELED))


        # tasks = await self.state.get_tasks()
        # tasks_statuses = await self.extraredis.mhget_field(self.TASK_PREFIX, 'status', tasks)
        # tasks_statuses = await self.state.tasks_statuses()
        # tasks_to_cancel = {}
        # for task_id, status in tasks_statuses.items():
        #     if status in {TaskStatus.PENDING, TaskStatus.RUNNING}:
        #         tasks_to_cancel[task_id] = TaskStatus.CANCELED
        # await self.extraredis.mhset_field(self.state.TASK_PREFIX, 'status', tasks_to_cancel)

    async def process_tasks(self):
        await self.cancel_orphaned()

        while True:
            print('watching', self.watch_path)
            # task = await self.pending_queue.get()
            # await self.start_task(task)

            # statuses = await self.state.tasks_statuses()
            # for task_id, status in statuses.items():
            #     if status in {TaskStatus.PENDING, TaskStatus.RUNNING}:
            #         print(task_id, status)
            #     if status == TaskStatus.PENDING:
            #         await self.start_task(task_id)
            await asyncio.sleep(1)

    async def main(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.watch_directory())
            tg.create_task(self.update_files_dags())
            tg.create_task(self.process_tasks())
            # tg.create_task(self.handle_tasks())
            # tg.create_task(self.handlers_dags())


if __name__ == '__main__':
    with get_db_cm() as db:
        watch_path = 'static/records_tmp'
        asyncio.run(
            AsyncWatcher(
                watch_path,
                db=db,
            ).main(),
        )
