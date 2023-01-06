import abc
import asyncio
from dagops.state import schemas
from dagops.state.crud.task import task_crud
from sqlalchemy.orm import Session


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
        self.status = None
        self.db_task = task_crud.create(self.db, schemas.TaskCreate(command=command, env=env))
        self.id = self.db_task.id
        self.logs_fh = open(f'static/logs/{self.id}.txt', 'w')

    async def run(self):
        p = await asyncio.create_subprocess_exec(
            *self.command,
            env=self.env,
            stdout=self.logs_fh,
            stderr=asyncio.subprocess.STDOUT,
        )
        await p.communicate()
        self.logs_fh.close()
        return p

# class Task:
#     def __init__(self, deps: list[str], **kwargs):
#         self.deps = deps
#         self.data = kwargs
#         self.status = TaskStatus.PENDING
#         # self._output = None

#     def _run(self):
#         self.status = TaskStatus.RUNNING
#         try:
#             # output = self.run()
#             self.run()
#         except Exception as e:
#             self.status = TaskStatus.FAILED
#         else:
#             self.status = TaskStatus.SUCCESS
#             # self._output = output

#     # @property
#     # def output(self):
#     #     if self.status != TaskStatus.SUCCESS:
#     #         raise RuntimeError(f'Task is not finished successfully, current status: {self.status}')
#     #     return self._output

#     @abc.abstractmethod
#     def run(self):
#         ...


# class ShellTask(Task):
#     def __init__(
#         self,
#         # **kwargs,
#         command: list[str],
#         env: dict[str, str] | None = None,
#         name: str | None = None,
#     ):
#         # super().__init__(deps, **kwargs)
#         # self.deps = deps
#         self.command = command
#         self.env = env
#         self.p = None
#         self.lines = []
#         self.name = name or str(uuid.uuid4())
#         self.fh = open(f'static/logs/{self.name}.txt', 'w')
#         # fh = logging.FileHandler(f'logs/{self.name}.txt')
#         # self.logger = logging.getLogger(self.name)
#         # self.logger.addHandler(fh)

#     @classmethod
#     def from_path(cls, path: str, env: dict[str, str] | None = None):
#         return cls(command=['sh', path], env=env)

#     def run(self):
#         print(2, self.env)
#         self.p = subprocess.Popen(
#             self.command,
#             env=self.env,
#             stdout=self.fh,
#             # stdout=self.logger,
#             # stdout=subprocess.PIPE,
#             stderr=subprocess.STDOUT,  # redirect stderr to stdout
#             text=True,
#         )
#         print(3, self.env)

#     @property
#     def status(self):
#         # print(7, self.env)
#         if self.p is None:
#             # print('7.1', self.env)
#             return TaskStatus.PENDING
#         elif self.p.poll() is None:
#             # print('7.2', self.env)
#             return TaskStatus.RUNNING
#         elif self.p.returncode == 0:
#             # print('7.3', self.env)
#             return TaskStatus.SUCCESS
#         else:
#             # print('7.4', self.env)
#             return TaskStatus.FAILED

#     def handle_running(self):
#         pass
#         # print(8, self.env)
#         # if self.status != TaskStatus.RUNNING:
#         #     raise RuntimeError(f'Task is not running, current status: {self.status}')

#         # if self.p is None:
#         #     raise RuntimeError('p is None')

#         # for line in self.p.stdout: # this is blocking till complete, bad, need use something else
#         # #     # logging.info(line.rstrip())
#         #     print(line.rstrip())
#         #     self.lines.append(line)
