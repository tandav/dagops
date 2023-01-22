import asyncio
import sys

from dagops.daemon import Daemon
from dagops.dependencies import get_db_cm
from dagops.state.schemas import InputDataDag
from dagops.state.schemas import ShellTaskInputData


def create_dag(
    file: str,
) -> InputDataDag:
    command = sys.executable, '-u', 'examples/write_to_mongo.py'
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


if __name__ == '__main__':
    with get_db_cm() as db:
        daemon = Daemon(
            watch_path='records_tmp',
            db=db,
            create_dag_func=create_dag,
        )
        asyncio.run(daemon())
