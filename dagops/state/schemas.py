from pydantic import BaseModel
from pydantic import Json
import datetime
from dagops.task import TaskStatus


class Task(BaseModel):
    id: int
    dag_id: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    started_at: datetime.datetime
    stopped_at: datetime.datetime
    status: TaskStatus
    command: Json
    env: Json
