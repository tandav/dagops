import datetime
from collections.abc import Iterable

from extraredis._async import ExtraRedisAsync


class State:
    def __init__(self, extraredis: ExtraRedisAsync | None = None):
        self.extraredis = extraredis or ExtraRedisAsync(decode_responses=True)
        self.redis = self.extraredis.redis
        self.TASK_PREFIX = 'task'
        self.DAG_PREFIX = 'dags'
        self.FILE_SET = 'file_set'
        self.UP_TO_DATE_FILE_SET = 'up_to_date_file_set'
        self.DELETED_FILE_SET = 'deleted_file_set'
        self.FILE_DAG_PREFIX = 'file_dag'
        self.TASK_SET = 'task_set'
        self.DAG_SET = 'dag_set'
        self.DAG_TASKS = 'dag_tasks'

    def add_dag_tasks(self, dag: str, *tasks: str):
        self.redis.sadd(self.addprefix(self.DAG_TASKS, dag), *tasks)

    async def update_files(self, files: Iterable[str]) -> None:
        pipe = self.redis.pipeline()
        pipe.delete(self.UP_TO_DATE_FILE_SET)
        pipe.sadd(self.UP_TO_DATE_FILE_SET, *files)
        pipe.sdiffstore(self.DELETED_FILE_SET, self.FILE_SET, self.UP_TO_DATE_FILE_SET)
        pipe.delete(self.FILE_SET)
        if files:
            pipe.sadd(self.FILE_SET, *files)
        await pipe.execute()

    async def get_tasks(self) -> set[str]:
        return await self.redis.smembers(self.TASK_SET)

    async def get_files(self) -> set[str]:
        return await self.redis.smembers(self.FILE_SET)

    async def add_files(self, *files: str):
        await self.redis.sadd(self.FILE_SET, *files)

    async def files_dags(self, files: Iterable[str] | None = None) -> dict[str, str]:
        # delete orphaned dags for deleted files
        del_files = await self.redis.smembers(self.DELETED_FILE_SET)
        if del_files:
            del_files = await self.extraredis.maddprefix(self.FILE_DAG_PREFIX, del_files)
            await self.extraredis.delete(*del_files)
        if files is None:
            files = await self.get_files()
        return await self.extraredis.mget(self.FILE_DAG_PREFIX, files)

    async def tasks_statuses(self, tasks: Iterable[str] | None = None) -> dict[str, str]:
        if tasks is None:
            tasks = await self.get_tasks()
        return await self.extraredis.mhget_field(self.TASK_PREFIX, 'status', tasks)

    async def set_task_status(self, task_id: str, status: str):
        await self.extraredis.hset_field(self.TASK_PREFIX, task_id, 'status', status)

    def format_info(self, info: dict) -> str:
        """inplace"""
        if 'created_at' in info:
            info['created_at'] = datetime.datetime.fromisoformat(info['created_at'])
        if 'started_at' in info:
            info['started_at'] = datetime.datetime.fromisoformat(info['started_at'])
        if 'stopped_at' in info:
            info['stopped_at'] = datetime.datetime.fromisoformat(info['stopped_at'])
        if 'duration' in info:
            info['duration'] = int(info['duration'])

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
            self.format_info(task_info)
        return kv

    async def get_dag_info(self, dag_id: str) -> str:
        kv = await self.extraredis.hget_fields(self.DAG_PREFIX, dag_id)
        kv['id'] = dag_id
        for task_id, task_info in kv['tasks'].items():
            task_info['id'] = task_id
            self.format_info(task_info)
        self.format_info(kv)
        return kv

    async def get_dags_info(self, dags: Iterable[str] | None = None) -> dict[str, dict]:
        if dags is None:
            dags = await self.redis.smembers(self.DAG_SET)
        kv = await self.extraredis.mhget_fields(self.DAG_PREFIX, dags)
        for dag_id, dag_info in kv.items():
            dag_info['id'] = dag_id
            self.format_info(dag_info)
        return kv
