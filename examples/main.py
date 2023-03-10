import asyncio
import os
import sys

from dagops.daemon import Daemon
from dagops.dag import Dag
from dagops.dependencies import get_db_cm
from dagops.dependencies import get_redis_cm
from dagops.state.schemas import TaskInfo
from dagops.worker import prepare_workers
from dagops.worker import run_workers


def create_dag(file: str) -> Dag:
    command = [sys.executable, '-u', 'examples/commands/long_command.py']
    common_env = {'TASK_NAME': file}
    if N_ITERATIONS := os.environ.get('N_ITERATIONS'):
        common_env['N_ITERATIONS'] = N_ITERATIONS
    a = TaskInfo(command=command, env=common_env | {'SUBTASK': 'a'}, worker_name='cpu')
    b = TaskInfo(command=command, env=common_env | {'SUBTASK': 'b'}, worker_name='cpu')
    c = TaskInfo(command=command, env=common_env | {'SUBTASK': 'c'}, worker_name='cpu')
    d = TaskInfo(command=command, env=common_env | {'SUBTASK': 'd'}, worker_name='cpu')
    e = TaskInfo(command=['ls'], worker_name='cpu')
    graph = {
        a: [],
        b: [],
        c: [a, b],
        d: [],
        e: [c, d],
    }
    return Dag(graph)


def create_batch_dag(files: list[str]) -> Dag:
    command = [sys.executable, 'examples/commands/batch_task.py', '--file', *files]
    a = TaskInfo(command=command, env={'SUBTASK': 'a'}, worker_name='cpu')
    b = TaskInfo(command=command, env={'SUBTASK': 'b'}, worker_name='cpu')
    graph = {
        a: [],
        b: [a],
    }
    return Dag(graph)


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
            storage=os.environ.get('STORAGE', 'filesystem'),
        )

        daemon2 = Daemon(
            watch_directory=os.environ['WATCH_DIRECTORY_BATCH'],
            db=db,
            redis=redis,
            create_dag_func=create_batch_dag,
            batch=True,
            storage=os.environ.get('STORAGE', 'filesystem'),
        )

        workers = await prepare_workers(db, redis)
        await asyncio.gather(
            run_workers(workers),
            daemon(),
            daemon2(),
        )


if __name__ == '__main__':
    asyncio.run(main())
