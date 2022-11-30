import asyncio
import aiofiles.os
from redis import Redis
import sys
import random
import contextlib
from dagops.task import TaskStatus
from dagops import config
from dagops.state import State


class AsyncWatcher:
    def __init__(self, watch_path: str):
        self.watch_path = watch_path
        self.queue = asyncio.Queue()
        self.processes = {}
        self.logs_handlers = {}
        self.state  = State()


    async def run_subprocess(self, key, logs_fh):
        p = await asyncio.create_subprocess_exec(
            sys.executable, '-u', 'write_to_mongo.py',
            env={'TASK_NAME': key},
            stdout=logs_fh,
            stderr=asyncio.subprocess.STDOUT,
        )
        await p.communicate()
        return p
        

    async def process_handlers(self):
        while True:
            if not self.processes:
                await asyncio.sleep(1)
                continue
            print(self.processes)
            task_to_task_id = {t: k for k, t in self.processes.items()}
            done, running = await asyncio.wait(task_to_task_id, return_when=asyncio.FIRST_COMPLETED)
            # assert len(done) == 1
            print('done', done)
            for task in done:
                p = task.result()
                # await p.communicate()
                assert p.returncode is not None
                task_id = task_to_task_id[task]
                print(task_id, p, p.returncode)
                status = TaskStatus.SUCCESS if p.returncode == 0 else TaskStatus.FAILED
                await self.state.set_task_status(task_id, status)
                self.logs_handlers[task_id].close()
                del self.logs_handlers[task_id]
                del self.processes[task_id]
            await asyncio.sleep(1)

    async def add_process(self, task_id):
   
        # while True:
        # key = await self.queue.get()
        await self.state.set_task_status(task_id, TaskStatus.RUNNING)
        logs_fh = open(f'static/logs/{task_id}.txt', 'w')
        self.logs_handlers[task_id] = logs_fh
        task = asyncio.create_task(self.run_subprocess(task_id, logs_fh))
        self.processes[task_id] = task
            # await asyncio.sleep(1)
            # status = random.choice([TaskStatus.SUCCESS, TaskStatus.FAILED])
            # await redis.set(key, status)
            # print(await redis.get(key))
            # self.queue.task_done()

    async def watch_directory(self):
        # reinit state
        # print(1)
        # keys = await aiofiles.os.listdir(self.watch_path)
        # statuses = await redis.mget(keys)
        statuses = await self.state.get_task_statuses()
        for task_id, status in statuses.items():
            if status in {TaskStatus.PENDING, TaskStatus.RUNNING}:
                statuses[task_id] = TaskStatus.CANCELED
        # await self.state.set_task_status(task_id, TaskStatus.CANCELED)
        await self.state.mset_task_status(statuses)
        # print(2)
        # state = dict(zip(keys, statuses))
        while True:
            print('watching', self.watch_path)
            async with asyncio.TaskGroup() as tg:
                statuses = tg.create_task(self.state.get_task_statuses())
                dir_task_ids = tg.create_task(aiofiles.os.listdir(self.watch_path))
            statuses = statuses.result()
            dir_task_ids = dir_task_ids.result()

            new_task_ids = set(dir_task_ids) - set(statuses)
            new_pending = dict.fromkeys(new_task_ids, TaskStatus.PENDING)
            print('new_pending', new_pending)
            await self.state.mset_task_status(new_pending)

            # statuses, dir_tasks = await self.state.get_task_statuses()
            # new_task_ids = await aiofiles.os.listdir(self.watch_path)
            # self.state.mset_task_status({k: TaskStatus.PENDING for k in new_keys})
            statuses = await self.state.get_task_statuses()
            for task_id, status in statuses.items():
                print(task_id, status)
                # if status is None:
                    # await redis.set(key, TaskStatus.PENDING)
                if status == TaskStatus.PENDING:
                    await self.add_process(task_id)
                    # self.queue.put_nowait(key)
            # for file in await aiofiles.os.listdir(path):

            await asyncio.sleep(2)
    
    async def main(self):
        await asyncio.gather(
            self.watch_directory(),
            self.process_handlers(),
        )


if __name__ == '__main__':
    # asyncio.run(main())
    asyncio.run(AsyncWatcher('static/records_tmp').main())
