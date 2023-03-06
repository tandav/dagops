import asyncio

from redis.asyncio import Redis
from sqlalchemy.orm import Session

from dagops import constant
from dagops.dependencies import get_db_cm
from dagops.fsm import WorkerTask
from dagops.state import schemas
from dagops.state.crud.worker import worker_crud


class Worker:
    def __init__(
        self,
        name: str,
        maxtasks: int,
        redis: Redis,
    ) -> None:
        self.name = name
        self.maxtasks = maxtasks
        self.aiotask_to_task_id = {}
        self.aio_tasks_channel = f'{constant.CHANNEL_AIO_TASKS}:{self.name}'
        self.redis = redis
        self.fsm_tasks = {}

    def __repr__(self):
        return f'Worker({self.name}, {self.maxtasks})'

    async def run_subprocess(
        self,
        log_id: str,
        command: list[str],
        env: dict[str, str] | None = None,
    ) -> asyncio.subprocess.Process:
        p = await asyncio.create_subprocess_exec(
            *command,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        log_key = f'{constant.LIST_LOGS}:{log_id}'
        await self.redis.rpush(log_key, '')  # create empty line to set ttl before logs are written
        await self.redis.expire(log_key, constant.LOGS_TTL)
        async for line in p.stdout:
            await self.redis.rpush(log_key, line)
        await p.communicate()
        return p

    async def run_task(self, task: schemas.TaskMessage) -> schemas.TaskRunResult:
        """MVP: without sending status updates to daemon
        TODO: Cache check should be on daemon side. #49
        delete this method, use only run_subprocess
        """
        # exists_returncode = None
        # if task.input_data.exists_command is not None:
        #     exists_p = await self.run_subprocess(
        #         f'{task.id}:exists',
        #         task.input_data.exists_command,
        #         task.input_data.exists_env,
        #     )
        #     if exists_p.returncode == 0:
        #         return schemas.TaskRunResult(exists_returncode=exists_p.returncode)
        #     elif exists_p.returncode == constant.CACHE_NOT_EXISTS_RETURNCODE:
        #         exists_returncode = exists_p.returncode
        #     else:
        #         return schemas.TaskRunResult(exists_returncode=exists_p.returncode)
        # print('-----> run_task', task, 'exists_returncode=', exists_returncode)
        p = await self.run_subprocess(
            task.id,
            task.input_data.command,
            task.input_data.env,
        )
        return schemas.TaskRunResult(
            # exists_returncode=exists_returncode,
            returncode=p.returncode,
        )

    async def run_tasks_from_queue(self):
        while True:
            if len(self.aiotask_to_task_id) >= self.maxtasks:
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            print('run_tasks_from_queue', len(self.aiotask_to_task_id))
            _, message = await self.redis.brpop(f'{constant.QUEUE_TASK}:{self.name}')
            task = schemas.TaskMessage.parse_raw(message)
            fsm_task = WorkerTask(task, self, self.redis)
            self.fsm_tasks[task.id] = fsm_task
            await fsm_task.run()
            print(fsm_task.state)

    async def handle_aio_tasks(self):
        while True:
            _, message = await self.redis.brpop(self.aio_tasks_channel)
            print('handle_aio_tasks', message)
            if not self.aiotask_to_task_id:  # todo: try remove
                await asyncio.sleep(constant.SLEEP_TIME)
                continue
            done, running = await asyncio.wait(self.aiotask_to_task_id, return_when=asyncio.FIRST_COMPLETED)
            for aiotask in done:
                result = aiotask.result()
                task_id = self.aiotask_to_task_id[aiotask]
                fsm_task = self.fsm_tasks[task_id]

                if result.returncode == 0:
                    await fsm_task.succeed(returncode=0)
                else:
                    await fsm_task.fail(returncode=result.returncode)

                # if result.exists_returncode == 0 or result.returncode == 0:
                #     await fsm_task.succeed(returncode=0)
                # else:
                #     await fsm_task.fail(returncode=result.returncode or result.exists_returncode)

                del self.fsm_tasks[task_id]
                del self.aiotask_to_task_id[aiotask]

    async def __call__(self):
        await self.redis.delete(self.aio_tasks_channel)
        await asyncio.gather(
            self.run_tasks_from_queue(),
            self.handle_aio_tasks(),
        )


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
    await asyncio.gather(*[worker() for worker in workers])

if __name__ == '__main__':
    with get_db_cm() as db:
        prepare_workers(db)
