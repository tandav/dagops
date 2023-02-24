import asyncio
import os
import sys

from dagops.daemon import Daemon
from dagops.dependencies import get_db_cm
from dagops.dependencies import get_redis_cm
from dagops.exists import exists
from dagops.state.schemas import InputDataDag
from dagops.state.schemas import TaskInfo
from dagops.worker import prepare_workers
from dagops.worker import run_workers


def create_dag(path: str) -> InputDataDag:
    counter_key = os.environ['COUNTER_KEY']
    counter_cmd = [sys.executable, '-u', 'examples/commands/counter.py', counter_key]
    counter_task = TaskInfo(
        command=counter_cmd,
        exists_command=exists.redis_key(counter_key),
        worker_name='cpu',
    )
    graph = {
        counter_task: [],
    }
    return graph


async def main():
    with (
        get_db_cm() as db,
        get_redis_cm() as redis,
    ):
        daemon = Daemon(
            watch_directory=os.environ['WATCH_DIRECTORY'],
            db=db,
            redis=redis,
            create_dag_func=create_dag,
            storage=os.environ['STORAGE'],
        )
        workers = await prepare_workers(db, redis)
        await asyncio.gather(
            run_workers(workers),
            daemon(),
        )


if __name__ == '__main__':
    asyncio.run(main())
