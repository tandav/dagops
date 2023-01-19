import abc
import asyncio
import datetime
import os

from sqlalchemy.orm import Session

from dagops.state import schemas
from dagops.state.crud.task import task_crud
from dagops.task_status import TaskStatus


class Task:

    @abc.abstractmethod
    async def run(self):
        ...

    # @property
    # @abc.abstractmethod
    # def status(self):
    #     ...

    # @abc.abstractmethod
    # def handle_running(self):
    #     ...


class ShellTask(Task):
    def __init__(
        self,
        db: Session,
        command: list[str],
        env: dict[str, str] | None = None,
    ):
        super().__init__()
        self.db = db
        self.command = command
        self.env = env or {}
        self.created_at = None
        self.started_at = None
        self.stopped_at = None
        db_task = task_crud.create(self.db, schemas.TaskCreate(command=command, env=self.env))
        self.id = db_task.id
        self.logs_fh = open(f'{os.environ["LOGS_DIRECTORY"]}/{self.id}.txt', 'w')
        self.dag = None

    @property
    def db_task(self):
        return task_crud.read_by_id(self.db, self.id)

    @property
    def status(self) -> schemas.TaskStatus:
        return self.db_task.status

    @property
    def dag_id(self) -> int:
        return self.db_task.dag_id

    async def run(self):
        self.started_at = datetime.datetime.now()
        task_crud.update_by_id(
            self.db, self.id, schemas.TaskUpdate(
                started_at=self.started_at,
                status=TaskStatus.RUNNING,
            ),
        )
        p = await asyncio.create_subprocess_exec(
            *self.command,
            env=self.env,
            stdout=self.logs_fh,
            stderr=asyncio.subprocess.STDOUT,
        )
        await p.communicate()
        self.logs_fh.close()
        return p

    def __hash__(self) -> int:
        return hash(self.id)
