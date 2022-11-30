import asyncio
import aiofiles.os
from redis import Redis
import dotenv
import os
import sys
import random
import contextlib
from redis.asyncio import Redis
from dagops.task import TaskStatus

dotenv.load_dotenv()
redis = Redis(
    host=os.environ['REDIS_HOST'],
    port=os.environ['REDIS_PORT'],
    password=os.environ['REDIS_PASSWORD'],
)


class AsyncWatcher:
    def __init__(self, watch_path: str):
        self.watch_path = watch_path
        self.queue = asyncio.Queue()
        self.processes = {}
        self.logs_handlers = {}


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
            task_to_key = {t: k for k, t in self.processes.items()}
            done, running = await asyncio.wait(task_to_key, return_when=asyncio.FIRST_COMPLETED)
            print('done', done)
            for task in done:
                p = task.result()
                # await p.communicate()
                assert p.returncode is not None
                key = task_to_key[task]
                print(key, p, p.returncode)
                status = TaskStatus.SUCCESS if p.returncode == 0 else TaskStatus.FAILED
                await redis.set(key, status)
                self.logs_handlers[key].close()
                del self.logs_handlers[key]
                del self.processes[key]
            await asyncio.sleep(1)

    async def add_process(self, key):
   
        # while True:
        # key = await self.queue.get()
        await redis.set(key, TaskStatus.RUNNING)
        logs_fh = open(f'static/logs/{key}.txt', 'w')
        self.logs_handlers[key] = logs_fh
        task = asyncio.create_task(self.run_subprocess(key, logs_fh))
        self.processes[key] = task
            # await asyncio.sleep(1)
            # status = random.choice([TaskStatus.SUCCESS, TaskStatus.FAILED])
            # await redis.set(key, status)
            # print(await redis.get(key))
            # self.queue.task_done()
    
    async def watch_directory(self):
        # reinit state
        print(1)
        keys = await aiofiles.os.listdir(self.watch_path)
        statuses = await redis.mget(keys)
        for key, status in zip(keys, statuses):
            if status in {TaskStatus.PENDING, TaskStatus.RUNNING}:
                await redis.set(key, TaskStatus.CANCELED)
        print(2)

        # state = dict(zip(keys, statuses))
        while True:
            print('watching', self.watch_path)
            keys = await aiofiles.os.listdir(self.watch_path)
            statuses = await redis.mget(keys)
            for key, status in zip(keys, statuses):
                print(key, status)
                if status is None:
                    await redis.set(key, TaskStatus.PENDING)
                elif status == TaskStatus.PENDING:
                    await self.add_process(key)
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
