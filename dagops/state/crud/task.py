import datetime

from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.crud.base import CRUD
from dagops.state.schemas import TaskCreate
from dagops.task import TaskStatus


class TaskCRUD(CRUD):

    # def read_by_id(db: Session, id: int) -> models.Task:
    #     return db.query(models.Task).filter(models.Task.id == id).first()

    # def read_many(
    #     db: Session,
    #     skip: int = 0,
    #     limit: int = 100,
    # ) -> list[models.Task]:
    #     query = db.query(models.Task)
    #     query = query.offset(skip).limit(limit)
    #     return query.all()

    def create(
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
