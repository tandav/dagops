import datetime
from collections.abc import Iterable

from extraredis._async import ExtraRedisAsync


class State:
    def __init__(self, extraredis: ExtraRedisAsync | None = None):
        self.extraredis = extraredis or ExtraRedisAsync(decode_responses=True)
        self.redis = self.extraredis.redis
        self.TASK_PREFIX = 'task'
        self.FILE_TASK_PREFIX = 'file_task'
        self.FILE_SET = 'file_set'
        self.STALE_FILESET = 'stale_file_set'
        self.TASK_SET = 'task_set'

    async def update_files(self, files: Iterable[str]) -> None:
        pipe = self.redis.pipeline()
        pipe.delete(self.FILE_SET)
        pipe.sadd(self.FILE_SET, *files)
        await pipe.execute()

    async def get_tasks(self) -> set[str]:
        return await self.redis.smembers(self.TASK_SET)

    async def get_files(self) -> set[str]:
        return await self.redis.smembers(self.FILE_SET)

    async def add_files(self, *files: str):
        await self.redis.sadd(self.FILE_SET, *files)

    async def files_tasks(self, files: Iterable[str] | None = None) -> dict[str, str]:
        if files is None:
            files = await self.get_files()
        return await self.extraredis.mget(self.FILE_TASK_PREFIX, files)

    async def tasks_statuses(self, tasks: Iterable[str] | None = None) -> dict[str, str]:
        if tasks is None:
            tasks = await self.get_tasks()
        return await self.extraredis.mhget_field(self.TASK_PREFIX, 'status', tasks)

    async def set_task_status(self, task_id: str, status: str):
        await self.extraredis.hset_field(self.TASK_PREFIX, task_id, 'status', status)

    def format_task_info(self, task_info: dict) -> str:
        """inplace"""
        task_info['start_time'] = datetime.datetime.fromisoformat(task_info['start_time'])
        task_info['end_time'] = datetime.datetime.fromisoformat(task_info['end_time'])
        task_info['duration'] = int(task_info['duration'])

    async def get_task_info(self, task_id: str) -> str:
        kv = await self.extraredis.hget_fields(self.TASK_PREFIX, task_id)
        kv['id'] = task_id
        self.format_task_info(kv)
        return kv

    async def get_tasks_info(self, tasks: Iterable[str] | None = None) -> dict[str, dict]:
        if tasks is None:
            tasks = await self.get_tasks()
        kv = await self.extraredis.mhget_fields(self.TASK_PREFIX, tasks)
        for task_id, task_info in kv.items():
            task_info['id'] = task_id
            self.format_task_info(task_info)
        return kv

#     def __init__(self, redis: Redis | None = None):
#         dotenv.load_dotenv()
#         self.redis = redis or Redis(
#             host=os.environ['REDIS_HOST'],
#             port=os.environ['REDIS_PORT'],
#             password=os.environ['REDIS_PASSWORD'],
#         )
#         self.prefix_tasks = 'tasks'

#     # @staticmethod
#     # def remove_key_level(keys: list[str], level: int) -> list[str]:
#     #     return [key.split(':')[level] for key in keys]
#     #     return [key.split(':')[1] for key in keys]

#     @staticmethod
#     def remove_table_prefix(keys: list[bytes]) -> list[bytes]:
#         return [key.split(b':')[1] for key in keys]

#     async def prefix_mget(self, prefix: bytes, keys: list[str] | None = None) -> dict[str, bytes]:
#         if keys is None:
#             keys = await self.redis.keys(prefix + b':*')
#         values = await self.redis.mget(keys)
#         keys = self.remove_table_prefix(keys)
#         return dict(zip(keys, values))

#     async def prefix_mhgetall(self, prefix: str, keys: list[str] | None = None) -> dict[str, bytes]:
#         keys = await self.redis.keys(f'{prefix}:*')
#         pipe = self.redis.pipeline()
#         for key in keys:
#             pipe.hgetall(key)
#         values = await pipe.execute()
#         keys = [k.decode() for k in keys]
#         return dict(zip(keys, values))

#     async def prefix_mhset(self, prefix: str, nv: dict[str, str], fields: list[str] | None = None):
#         if not nv:
#             return
#         pipe = self.redis.pipeline()
#         for (n, v), f in zip(nv.items(), fields):
#             pipe.hset(f'{prefix}:{n}', f, v)
#         await pipe.execute()

#     async def prefix_mhget(self, prefix: str, fields: list[str], keys: list[str] | None = None) -> dict[str, bytes]:
#         if keys is None:
#             keys = await self.redis.keys(prefix + b':*')
#         pipe = self.redis.pipeline()
#         for f, k in zip(fields, keys):
#             pipe.hget(k, f)
#         values = await pipe.execute()
#         print('************')
#         print(keys)
#         print(values)
#         # keys = [k.decode() for k in keys]
#         keys = self.remove_table_prefix(keys)
#         return dict(zip(keys, values))

#         for key in keys:
#             pipe.hmget(key, fields)
#         values = await pipe.execute()
#         keys = [k.decode() for k in keys]
#         keys = self.remove_table_prefix(keys)
#         return dict(zip(keys, values))

#     async def get_task_statuses(self, task_ids: list[str] | None = None) -> dict[str, bytes]:
#         if task_ids is None:
#             keys = await self.redis.keys(f'{self.prefix_tasks}:*')
#         pipe = self.redis.pipeline()
#         for key in keys:
#             pipe.hget(key, 'status')
#         statuses = await pipe.execute()
#         keys = [k.decode() for k in keys]
#         task_ids = self.remove_table_prefix(keys)
#         return dict(zip(task_ids, statuses))
#         # values = await self.redis.mget(keys)
#         # return dict(zip(keys, values))

#     async def set_task_status(self, task_id: str, status: str):
#         await self.redis.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)

#     async def get_task_status(self, task_id: str) -> str:
#         return await self.redis.hget(f'{self.prefix_tasks}:{task_id}', 'status')

#     async def mset_task_status(self, statuses: dict[str, str]):
#         if not statuses:
#             return
#         pipe = self.redis.pipeline()
#         for task_id, status in statuses.items():
#             pipe.hset(f'{self.prefix_tasks}:{task_id}', 'status', status)
#         await pipe.execute()
#         # statuses = {f'{self.prefix_tasks}:{k}': v for k, v in statuses.items()}
#         # await self.redis.mset(statuses)
