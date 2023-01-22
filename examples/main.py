import asyncio
import sys

from sqlalchemy.orm import Session

from dagops.daemon import Daemon
from dagops.dag import Dag
from dagops.dependencies import get_db_cm
from dagops.state.schemas import InputDataDag
from dagops.state.schemas import ShellTaskInputData
from dagops.task import ShellTask
from dagops.task import Task


def create_dag(
    file: str,
) -> InputDataDag:
    command = sys.executable, '-u', 'write_to_mongo.py'
    a = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'a'})
    b = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'b'})
    c = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'c'})
    d = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'd'})
    e = ShellTaskInputData(command=command, env={'TASK_NAME': file, 'SUBTASK': 'e'})
    graph = {
        a: [],
        b: [],
        c: [a, b],
        d: [],
        e: [c, d],
    }
    return graph
    # dag = Dag(db, graph)
    # return dag


if __name__ == '__main__':
    with get_db_cm() as db:
        daemon = Daemon(
            watch_path='records_tmp',
            db=db,
            create_dag_func=create_dag,
        )
        asyncio.run(daemon())
