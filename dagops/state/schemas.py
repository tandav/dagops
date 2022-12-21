import datetime

from pydantic import BaseModel
from pydantic import validator

from dagops.task import TaskStatus


# =============================================================================


class TaskCreate(BaseModel):
    command: list[str]
    env: dict[str, str]


class Task(TaskCreate):
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


class Dag(BaseModel):
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
