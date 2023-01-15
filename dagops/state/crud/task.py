import datetime

from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.crud.base import CRUD
from dagops.state.schemas import TaskCreate
from dagops.task_status import TaskStatus


class TaskCRUD(CRUD):
    def create(
        self,
        db: Session,
        task: TaskCreate,
    ) -> models.Task:
        db_task = models.Task(
            **task.dict(),
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
            status=TaskStatus.PENDING,
        )
        db.add(db_task)
        db.commit()
        db.refresh(db_task)
        return db_task


task_crud = TaskCRUD(models.Task)
