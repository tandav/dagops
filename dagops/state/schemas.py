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


class ShellTaskPayload(BaseModel):
    command: list[str]
    env: dict[str, str]

    def __hash__(self):
        return hash((tuple(self.command), frozenset(self.env.items())))


PayloadDag = dict[ShellTaskPayload, list[ShellTaskPayload]]

TASK_TYPE_TO_PAYLOAD_SCHEMA = {
    'shell': ShellTaskPayload,
    'dag': None,
}


class TaskCreate(BaseModel):
    id: str | None = None
    dag_id: str | None = None
    upstream: list[str] = []
    # dag_tasks: list[str] | None = None
    # is_dag_head: bool = False
    task_type: str | None = None
    payload: dict | None = None

    @root_validator(pre=True)
    def validate_task_type_and_payload(cls, values):
        if 'task_type' not in values or values['task_type'] is None:
            return values
            # raise ValueError('task_type must be set')
        task_type = values['task_type']
        payload_schema = TASK_TYPE_TO_PAYLOAD_SCHEMA[task_type]
        if payload_schema is None:
            return values
        if 'payload' not in values:
            raise ValueError('payload must be set for task_type {task_type}')
        if task_type not in TASK_TYPE_TO_PAYLOAD_SCHEMA:
            raise ValueError(f'unsupported task type {task_type}')
        payload_schema.validate(values['payload'])
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

    class Config:
        orm_mode = True


class TaskUpdate(BaseModel):
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    updated_at: datetime.datetime
    status: TaskStatus | None


# =============================================================================


class DagCreate(BaseModel):
    task_type: str | None = None
    task_payloads: list[dict]
    graph: dict[int, list[int]]

    @root_validator
    def validate_graph(cls, values):
        graph = values['graph']
        n_task_payloads = len(values['task_payloads'])
        for node, childs in graph.items():
            if not (0 <= node < n_task_payloads):
                raise ValueError(f'node={node} not in graph, n_task_payloads={n_task_payloads}')
            for child in childs:
                if not (0 <= child < n_task_payloads):
                    raise ValueError(f'child={child} of node={node} not in graph, n_task_payloads={n_task_payloads}')
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
