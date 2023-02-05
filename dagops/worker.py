import asyncio

from redis.asyncio import Redis
from sqlalchemy.orm import Session

from dagops import constant
from dagops.dependencies import get_db_cm
from dagops.state import schemas
from dagops.state.crud.worker import worker_crud
from dagops.task_status import TaskStatus


class Worker:
    def __init__(
        self,
        name: str,
        maxtasks: int,
        redis: Redis,
    ):
        self.name = name
        self.maxtasks = maxtasks
        self.aiotask_to_task_id = {}
        self.aio_tasks_channel = f'{constant.CHANNEL_AIO_TASKS}:{self.name}'
        self.redis = redis

    def __repr__(self):
        return f'Worker({self.name}, {self.maxtasks})'

    async def run_task(self, task: schemas.TaskMessage) -> asyncio.subprocess.Process:
        p = await asyncio.create_subprocess_exec(
            *task.input_data.command,
            env=task.input_data.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        async for line in p.stdout:
            await self.redis.rpush(f'{constant.LIST_LOGS}:{task.id}', line)
        await p.communicate()
        return p

    async def run_tasks_from_queue(self):
        while True:
            if len(self.aiotask_to_task_id) >= self.maxtasks:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            print('run_tasks_from_queue', len(self.aiotask_to_task_id))
            _, message = await self.redis.brpop(constant.CHANNEL_TASK_QUEUE)
            task = schemas.TaskMessage.parse_raw(message)
            print('run_tasks_from_queue', task)
            self.aiotask_to_task_id[asyncio.create_task(self.run_task(task))] = task.id
            pipeline = self.redis.pipeline()
            pipeline.lpush(self.aio_tasks_channel, task.id)
            pipeline.lpush(
                constant.CHANNEL_TASK_STATUS, schemas.TaskStatusMessage(
                    id=task.id,
                    status=TaskStatus.RUNNING,
                ).json(),
            )
            await pipeline.execute()

    async def handle_aio_tasks(self):
        while True:
            _, message = await self.redis.brpop(self.aio_tasks_channel)
            print('handle_aio_tasks', message)
            if not self.aiotask_to_task_id:  # todo: try remove
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            done, running = await asyncio.wait(self.aiotask_to_task_id, return_when=asyncio.FIRST_COMPLETED)
            for aiotask in done:
                p = aiotask.result()
                assert p.returncode is not None
                status_message = schemas.TaskStatusMessage(
                    id=self.aiotask_to_task_id[aiotask],
                    status=TaskStatus.SUCCESS if p.returncode == 0 else TaskStatus.FAILED,
                    output_data={'returncode': p.returncode},
                )
                await self.redis.lpush(constant.CHANNEL_TASK_STATUS, status_message.json())
                del self.aiotask_to_task_id[aiotask]
                print('EXITING TASK', status_message)

    async def __call__(self):
        await self.redis.delete(self.aio_tasks_channel)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.run_tasks_from_queue())
            tg.create_task(self.handle_aio_tasks())


async def prepare_workers(
    db: Session,
    redis: Redis,
    workers: dict[str, int] = constant.workers,
) -> list[Worker]:
    dag_worker = worker_crud.read_by_field(db, 'name', 'dag')
    if len(dag_worker) == 0:
        worker_crud.create(db, schemas.WorkerCreate(name='dag'))  # dag worker for dags, todo try to remove

    workers_objs = []
    for worker_name, maxtasks in workers.items():
        workers_objs.append(Worker(worker_name, maxtasks, redis))
        db_worker = worker_crud.read_by_field(db, 'name', worker_name)
        if len(db_worker) == 0:
            worker_crud.create(db, schemas.WorkerCreate(name=worker_name, maxtasks=maxtasks))
            continue
        db_worker, = db_worker
        if db_worker.maxtasks != maxtasks:
            worker_crud.update_by_id(db, db_worker.id, schemas.WorkerUpdate(maxtasks=maxtasks))
        await redis.delete(f'{constant.CHANNEL_AIO_TASKS}:{worker_name}')
    return workers_objs


async def run_workers(workers: list[Worker]):
    async with asyncio.TaskGroup() as tg:
        for worker in workers:
            tg.create_task(worker())

if __name__ == '__main__':
    with get_db_cm() as db:
        prepare_workers(db)
