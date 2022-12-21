import datetime

from pydantic import BaseModel
from pydantic import Json

from dagops.task import TaskStatus


class TaskCreate(BaseModel):
    dag_id: int
    command: list[str]
    env: dict[str, str]


class Task(TaskCreate):
    id: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    started_at: datetime.datetime | None
    stopped_at: datetime.datetime | None
    status: TaskStatus

    class Config:
        orm_mode = True


class Dag(BaseModel):
    id: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    started_at: datetime.datetime
    stopped_at: datetime.datetime
    status: TaskStatus
    graph: dict[str, list[str]]
    tasks: list[str]

    class Config:
        orm_mode = True
