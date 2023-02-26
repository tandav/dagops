import datetime
import uuid
from uuid import UUID

from pydantic import BaseModel
from pydantic import Field
from pydantic import root_validator

from dagops import constant
from dagops.state.status import TaskStatus
from dagops.state.status import WorkerTaskStatus

# =============================================================================


class WithDuration(BaseModel):
    created_at: datetime.datetime

    duration_seconds: float | None

    @root_validator
    def validate_duration(cls, values):
        started_at = values.get('started_at')
        stopped_at = values.get('stopped_at')
        if started_at is not None:
            if stopped_at is not None:
                values['duration_seconds'] = (stopped_at - started_at).total_seconds()
            else:
                values['duration_seconds'] = (datetime.datetime.now(tz=datetime.timezone.utc) - started_at).total_seconds()
        return values

# =============================================================================


class WithWorkerName(BaseModel):
    worker_name: str


class ShellTaskInputData(BaseModel):
    command: list[str]
    env: dict[str, str] | None = None
    exists_command: list[str] | None = None
    exists_env: dict[str, str] | None = None

    def __hash__(self):
        command = tuple(self.command)
        env = frozenset(self.env.items()) if self.env is not None else None
        exists_command = tuple(self.exists_command) if self.exists_command is not None else None
        exists_env = frozenset(self.exists_env.items()) if self.exists_env is not None else None
        return hash((command, env, exists_command, exists_env))


class TaskMessage(BaseModel):
    id: str
    input_data: ShellTaskInputData
    daemon_id: str


class WorkerTaskStatusMessage(BaseModel):
    id: str
    status: WorkerTaskStatus
    output_data: dict | None = None


class TaskInfo(ShellTaskInputData, WithWorkerName):
    pass


InputDataDag = dict[TaskInfo, list[TaskInfo]]

type_TO_INPUT_DATA_SCHEMA = {
    'shell': ShellTaskInputData,
    'dag': None,
}


class TaskCreate(WithWorkerName):
    id: UUID = Field(default_factory=uuid.uuid4)
    dag_id: UUID | None = None
    upstream: list[UUID] = []
    type: str | None = None
    input_data: dict | None = None
    worker_id: UUID | None = None

    @root_validator(pre=True)
    def validate_type_and_input_data(cls, values):
        type = values['type']
        if type not in type_TO_INPUT_DATA_SCHEMA:
            raise ValueError(f'unsupported task type {type}')
        input_data_schema = type_TO_INPUT_DATA_SCHEMA[type]

        if input_data_schema is None:
            return values

        if 'input_data' not in values:
            raise ValueError('input_data must be set for type {type}')

        input_data_schema.validate(values['input_data'])
        return values


class Task(TaskCreate, WithDuration):
    dag_id: UUID | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus
    downstream: list[UUID] = []
    output_data: dict | None = None

    class Config:
        orm_mode = True


class TaskUpdate(BaseModel):
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus | None
    output_data: dict | None = None
    worker_id: UUID | None = None
    running_worker_id: UUID | None = None


class TaskRunResult(BaseModel):
    exists_returncode: int | None = None
    returncode: int | None = None

    @root_validator
    def validate_returncodes(cls, values):
        if values['exists_returncode'] is None and values['returncode'] is None:
            raise ValueError('at least one of returncodes should be set')
        if values['exists_returncode'] is None:
            return values
        if values['exists_returncode'] != constant.NOT_EXISTS_RETURNCODE and values['returncode'] is not None:
            raise ValueError(f'task should be run only if exists check not passed {values}')
        return values

# =============================================================================


class DagCreate(BaseModel):
    type: str | None = None
    tasks_input_data: list[dict]
    graph: dict[int, list[int]]
    daemon_id: UUID

    @root_validator
    def validate_graph(cls, values):
        graph = values['graph']
        n_tasks = len(values['tasks_input_data'])
        for node, childs in graph.items():
            if not (0 <= node < n_tasks):
                raise ValueError(f'node={node} not in graph, n_tasks={n_tasks}')
            for child in childs:
                if not (0 <= child < n_tasks):
                    raise ValueError(f'child={child} of node={node} not in graph, n_tasks={n_tasks}')
                if child not in graph:
                    raise ValueError(f'child={child} of node={node } not in graph')
        return values


# =============================================================================


class FileCreate(BaseModel):
    storage: str
    directory: str
    file: str


class FileUpdate(BaseModel):
    dag_id: UUID | None


class File(FileCreate, FileUpdate):
    id: UUID

    class Config:
        orm_mode = True

# =============================================================================


class WorkerCreate(BaseModel):
    name: str
    maxtasks: int | None = None


class WorkerUpdate(BaseModel):
    name: str | None = None
    maxtasks: int | None = None


class Worker(WorkerCreate):
    id: UUID
    tasks: list[UUID]
    running_tasks: list[UUID]

    class Config:
        orm_mode = True
