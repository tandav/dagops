import asyncio
import datetime
import json
import sys

import aiofiles.os
import dotenv
from extraredis._async import ExtraRedisAsync

from dagops.state import State
from dagops.task import ShellTask
from dagops.task import TaskStatus

dotenv.load_dotenv()


class AsyncWatcher:
    def __init__(self, watch_path: str):
        self.watch_path = watch_path
        self.pending_queue = asyncio.Queue()
        # self.processes = {}
        self.tasks = {}  # task_id: Task
        self.running_tasks = {}
        # self.logs_handlers = {}
        self.extraredis = ExtraRedisAsync(decode_responses=True)
        self.state = State(self.extraredis)

    async def run_subprocess(self, cmd, env, logs_fh):
        p = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=logs_fh,
            stderr=asyncio.subprocess.STDOUT,
        )
        await p.communicate()
        return p

    async def process_handlers(self):
        while True:
            if not self.running_tasks:
                await asyncio.sleep(1)
                continue
            print(self.running_tasks)
            task_to_task_id = {t: k for k, t in self.running_tasks.items()}
            done, running = await asyncio.wait(task_to_task_id, return_when=asyncio.FIRST_COMPLETED)
            # assert len(done) == 1
            for aio_task in done:
                p = aio_task.result()
                # await p.communicate()
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
                del self.running_tasks[task_id]
                del self.tasks[task_id]
            await asyncio.sleep(1)

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

    async def start_task(self, task_id: str):
        task = self.tasks[task_id]
        task.started_at = datetime.datetime.now()
        await self.extraredis.hset_fields(
            self.state.TASK_PREFIX, task_id, {
                'status': TaskStatus.RUNNING,
                'command': json.dumps(task.command),
                'env': json.dumps(task.env),
                'started_at': str(task.started_at),
            },
        )
        # await self.state.set_shell_task(task, TaskStatus.RUNNING)
        self.running_tasks[task_id] = asyncio.create_task(task.run())

        # self.running_tasks[task_id] = asyncio.create_task(task.run())
        # task = self.tasks[task_id]
        # self.running_tasks[task_id] = asyncio.create_task(task.run())

    async def stop_task(self):
        pass

    # async def update_files_tasks(self, files: Iterable[str]) -> None:

    async def update_files_tasks(self) -> None:
        """create tasks for new files"""
        while True:
            files = await self.state.get_files()
            files_tasks = await self.state.files_tasks(files)
            to_update = {}
            for file, task_id in files_tasks.items():
                if task_id is None:
                    task = ShellTask(command=[sys.executable, '-u', 'write_to_mongo.py'], env={'TASK_NAME': file})
                    self.tasks[task.id] = task
                    to_update[file] = task.id
            if len(to_update) == 0:
                await asyncio.sleep(1)
                continue
            await self.extraredis.mset(self.state.FILE_TASK_PREFIX, to_update)
            await self.extraredis.redis.sadd(self.state.TASK_SET, *to_update.values())
            # await self.extraredis.mhset_field(self.state.TASK_PREFIX, 'status', dict.fromkeys(to_update.values(), TaskStatus.PENDING))
            now = datetime.datetime.now()
            await self.extraredis.mhset_fields(
                self.state.TASK_PREFIX,
                {k: {'created_at': str(now), 'status': TaskStatus.PENDING} for k in to_update.values()},
            )
            for task in to_update.values():
                await self.pending_queue.put(task)
            await asyncio.sleep(1)

    async def watch_directory(self):
        while True:
            files = set(await aiofiles.os.listdir(self.watch_path))
            await self.state.update_files(files)
            # await self.update_files_tasks()
            await asyncio.sleep(1)

    async def cancel_orphaned_tasks(self):
        # tasks = await self.state.get_tasks()
        # tasks_statuses = await self.extraredis.mhget_field(self.TASK_PREFIX, 'status', tasks)
        tasks_statuses = await self.state.tasks_statuses()
        tasks_to_cancel = {}
        for task_id, status in tasks_statuses.items():
            if status in {TaskStatus.PENDING, TaskStatus.RUNNING}:
                tasks_to_cancel[task_id] = TaskStatus.CANCELED
        await self.extraredis.mhset_field(self.state.TASK_PREFIX, 'status', tasks_to_cancel)

    async def process_tasks(self):
        await self.cancel_orphaned_tasks()

        while True:
            print('watching', self.watch_path)
            task = await self.pending_queue.get()
            await self.start_task(task)
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
            tg.create_task(self.process_tasks())
            tg.create_task(self.process_handlers())
            tg.create_task(self.update_files_tasks())


if __name__ == '__main__':
    # asyncio.run(main())
    asyncio.run(AsyncWatcher('static/records_tmp').main())
