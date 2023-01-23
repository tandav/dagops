import asyncio
import sys

from dagops.daemon import Daemon
from dagops.dependencies import get_db_cm
from dagops.state.schemas import InputDataDag
from dagops.state.schemas import ShellTaskInputData


def create_dag(file: str) -> InputDataDag:
    command = [sys.executable, '-u', 'examples/write_to_mongo.py']
    a = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'a'}, worker_name='cpu')
    b = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'b'}, worker_name='cpu')
    c = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'c'}, worker_name='cpu')
    d = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'd'}, worker_name='cpu')
    e = ShellTaskInputData(command=['ls'], worker_name='cpu')
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
    a = ShellTaskInputData(command=command, env={'SUBTASK': 'a'}, worker_name='cpu')
    b = ShellTaskInputData(command=command, env={'SUBTASK': 'b'}, worker_name='cpu')
    graph = {
        a: [],
        b: [a],
    }
    return graph


async def main():
    with get_db_cm() as db:
        daemon = Daemon(
            watch_directory='records_tmp',
            db=db,
            create_dag_func=create_dag,
        )

        daemon2 = Daemon(
            watch_directory='records_tmp2',
            db=db,
            create_dag_func=create_batch_dag,
            batch=True,
        )

        async with asyncio.TaskGroup() as tg:
            tg.create_task(daemon())
            tg.create_task(daemon2())


if __name__ == '__main__':
    asyncio.run(main())
