import os
import dotenv

from redis.asyncio import Redis


class State:
    def __init__(self):
        dotenv.load_dotenv()
        self.redis = Redis(
            host=os.environ['REDIS_HOST'],
            port=os.environ['REDIS_PORT'],
            password=os.environ['REDIS_PASSWORD'],
        )
        self.prefix_tasks = 'tasks'

    # @staticmethod
    # def remove_key_level(keys: list[str], level: int) -> list[str]:
    #     return [key.split(':')[level] for key in keys]
    #     return [key.split(':')[1] for key in keys]

    @staticmethod
    def remove_table_prefix(keys: list[str]) -> list[str]:
        return [key.split(':')[1] for key in keys]

    async def get_task_statuses(self, task_ids: list[str] | None = None) -> dict[str, bytes]:
        if task_ids is None:
            keys = await self.redis.keys(f'{self.prefix_tasks}:*')
        pipe = self.redis.pipeline()
        for key in keys:
            pipe.hget(key, 'status')
        statuses = await pipe.execute()
        keys = [k.decode() for k in keys]
        task_ids = self.remove_table_prefix(keys)
        return dict(zip(task_ids, statuses))
        # values = await self.redis.mget(keys)
        # return dict(zip(keys, values))

    async def set_task_status(self, task_id: str, status: str):
        await self.redis.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)
    
    async def get_task_status(self, task_id: str) -> str:
        return await self.redis.hget(f'{self.prefix_tasks}:{task_id}', 'status')

    async def mset_task_status(self, statuses: dict[str, str]):
        if not statuses:
            return
        pipe = self.redis.pipeline()
        for task_id, status in statuses.items():
            pipe.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)
        await pipe.execute()
        # statuses = {f'{self.prefix_tasks}:{k}': v for k, v in statuses.items()}
        # await self.redis.mset(statuses)
