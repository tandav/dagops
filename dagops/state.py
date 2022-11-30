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
        values = await self.redis.mget(keys)
        keys = [k.decode() for k in keys]
        keys = self.remove_table_prefix(keys)
        return dict(zip(keys, values))

    async def set_task_status(self, task_id: str, status: str):
        await self.redis.set(f'{self.prefix_tasks}:{task_id}', status)
    
    async def get_task_status(self, task_id: str) -> str:
        return await self.redis.get(f'{self.prefix_tasks}:{task_id}')

    async def mset_task_status(self, statuses: dict[str, str]):
        if not statuses:
            return
        statuses = {f'{self.prefix_tasks}:{k}': v for k, v in statuses.items()}
        await self.redis.mset(statuses)
