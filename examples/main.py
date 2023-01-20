import asyncio
import sys

from sqlalchemy.orm import Session

from dagops.daemon import Daemon
from dagops.dag import Dag
from dagops.dependencies import get_db_cm
from dagops.task import ShellTask


def create_dag(
    file: str,
    db: Session,
) -> Dag:
    print('dag for file', file, 'start creating...')
    command = sys.executable, '-u', 'write_to_mongo.py'
    a = ShellTask(db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'a'})
    b = ShellTask(db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'b'})
    c = ShellTask(db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'c'})
    d = ShellTask(db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'd'})
    e = ShellTask(db, command=command, env={'TASK_NAME': file, 'SUBTASK': 'e'})
    graph = {
        a: [],
        b: [],
        c: [a, b],
        d: [],
        e: [c, d],
    }
    dag = Dag(db, graph)
    print('dag for file', file, 'created')
    return dag


if __name__ == '__main__':
    with get_db_cm() as db:
        daemon = Daemon(
            watch_path='records_tmp',
            db=db,
            create_dag_func=create_dag,
        )
        asyncio.run(daemon())
