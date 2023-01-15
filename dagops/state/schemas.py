import datetime

from pydantic import BaseModel
from pydantic import root_validator
from pydantic import validator

from dagops.task_status import TaskStatus

# =============================================================================


class WithDuration(BaseModel):
    duration_seconds: float | None

    @root_validator
    def validate_duration(cls, values):
        started_at = values.get('started_at')
        stopped_at = values.get('stopped_at')
        if started_at is not None and stopped_at is not None:
            values['duration_seconds'] = (stopped_at - started_at).total_seconds()
        return values

# =============================================================================


class TaskCreate(BaseModel):
    command: list[str]
    env: dict[str, str]


class Task(TaskCreate, WithDuration):
    id: str
    dag_id: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus

    class Config:
        orm_mode = True


class TaskUpdate(BaseModel):
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus | None


# =============================================================================

class DagCreate(BaseModel):
    graph: dict[str, list[str]]

    @validator('graph')
    def validate_graph(cls, v):
        for node, childs in v.items():
            for child in childs:
                if child not in v:
                    raise ValueError(f'child={child} of node={node } not in graph')
        return v


class Dag(WithDuration):
    id: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus
    graph: dict[str, list[str]]
    tasks: list[str]

    class Config:
        orm_mode = True


class DagUpdate(BaseModel):
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus | None


# =============================================================================


class FileCreate(BaseModel):
    path: str


class FileUpdate(BaseModel):
    dag_id: str | None


class File(FileCreate, FileUpdate):
    id: str

    class Config:
        orm_mode = True
