import datetime
import uuid

from pydantic import BaseModel
from pydantic import root_validator
from pydantic import validator

from dagops.task_status import TaskStatus

# =============================================================================


class WithDuration(BaseModel):
    created_at: datetime.datetime

    duration_seconds: float | None

    @root_validator
    def validate_duration(cls, values):
        started_at = values.get('started_at')
        stopped_at = values.get('stopped_at')
        if started_at is not None and stopped_at is not None:
            values['duration_seconds'] = (stopped_at - started_at).total_seconds()
        return values

# =============================================================================


class ShellTaskInputData(BaseModel):
    command: list[str]
    env: dict[str, str]

    def __hash__(self):
        return hash((tuple(self.command), frozenset(self.env.items())))


InputDataDag = dict[ShellTaskInputData, list[ShellTaskInputData]]

TASK_TYPE_TO_INPUT_DATA_SCHEMA = {
    'shell': ShellTaskInputData,
    'dag': None,
}


class TaskCreate(BaseModel):
    id: str | None = None
    dag_id: str | None = None
    upstream: list[str] = []
    # dag_tasks: list[str] | None = None
    # is_dag_head: bool = False
    task_type: str | None = None
    input_data: dict | None = None

    @root_validator(pre=True)
    def validate_task_type_and_input_data(cls, values):
        if 'task_type' not in values or values['task_type'] is None:
            return values
            # raise ValueError('task_type must be set')
        task_type = values['task_type']
        input_data_schema = TASK_TYPE_TO_INPUT_DATA_SCHEMA[task_type]
        if input_data_schema is None:
            return values
        if 'input_data' not in values:
            raise ValueError('input_data must be set for task_type {task_type}')
        if task_type not in TASK_TYPE_TO_INPUT_DATA_SCHEMA:
            raise ValueError(f'unsupported task type {task_type}')
        input_data_schema.validate(values['input_data'])
        return values

    @root_validator()
    def add_id(cls, values):
        if values['id'] is None:
            values['id'] = uuid.uuid4().hex
        return values

    # @root_validator
    # def validate_dag_tasks(cls, values):
    #     if values.get('is_dag_head') and values.get('dag_tasks') is None:
    #         raise ValueError('dag_tasks must be set for dag head')
    #     return values


class Task(TaskCreate, WithDuration):
    dag_id: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus
    downstream: list[str] = []
    output_data: dict | None = None

    class Config:
        orm_mode = True


class TaskUpdate(BaseModel):
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    updated_at: datetime.datetime
    status: TaskStatus | None
    output_data: dict | None = None


# =============================================================================


class DagCreate(BaseModel):
    task_type: str | None = None
    tasks_input_data: list[dict]
    graph: dict[int, list[int]]

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


# class Dag(WithDuration):
#     id: str
#     created_at: datetime.datetime
#     updated_at: datetime.datetime
#     started_at: datetime.datetime | None
#     stopped_at: datetime.datetime | None
#     status: TaskStatus
#     graph: dict[str, list[str]]
#     tasks: list[str]

#     class Config:
#         orm_mode = True


# class DagUpdate(BaseModel):
#     started_at: datetime.datetime | None
#     stopped_at: datetime.datetime | None
#     status: TaskStatus | None


# =============================================================================


class FileCreate(BaseModel):
    path: str


class FileUpdate(BaseModel):
    dag_id: str | None


class File(FileCreate, FileUpdate):
    id: str

    class Config:
        orm_mode = True
