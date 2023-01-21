import datetime
import random
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
        # return hash((tuple(self.command), frozenset(self.env.items())))
        return hash(random.random())


TaskPayload = ShellTaskPayload


class TaskCreate(BaseModel):
    id: str | None = None
    dag_id: str | None = None
    upstream: list[str] = []
    # dag_tasks: list[str] | None = None
    # is_dag_head: bool = False
    payload: TaskPayload | None = None

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
    status: TaskStatus | None


# =============================================================================

class DagCreate(BaseModel):
    graph: dict[TaskPayload, list[TaskPayload]]

    @validator('graph')
    def validate_graph(cls, v):
        for node, childs in v.items():
            for child in childs:
                if child not in v:
                    raise ValueError(f'child={child} of node={node } not in graph')
        return v


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
