import asyncio
import os
import sys

from dagops.daemon import Daemon
from dagops.dependencies import get_db_cm
from dagops.state.schemas import InputDataDag
from dagops.state.schemas import TaskInfo


def create_dag(file: str) -> InputDataDag:
    command = [sys.executable, '-u', 'examples/long_command.py']
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
    return graph


def create_batch_dag(files: list[str]) -> InputDataDag:
    command = [sys.executable, 'examples/batch_task.py', '--file', *files]
    a = TaskInfo(command=command, env={'SUBTASK': 'a'}, worker_name='cpu')
    b = TaskInfo(command=command, env={'SUBTASK': 'b'}, worker_name='cpu')
    graph = {
        a: [],
        b: [a],
    }
    return graph


async def main():
    with get_db_cm() as db:
        daemon = Daemon(
            watch_directory='examples/records_tmp',
            db=db,
            create_dag_func=create_dag,
        )

        daemon2 = Daemon(
            watch_directory='examples/records_tmp2',
            db=db,
            create_dag_func=create_batch_dag,
            batch=True,
        )

        async with asyncio.TaskGroup() as tg:
            tg.create_task(daemon())
            tg.create_task(daemon2())


if __name__ == '__main__':
    asyncio.run(main())
