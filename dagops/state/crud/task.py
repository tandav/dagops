from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.crud.base import CRUD
from dagops.state.crud.worker import worker_crud
from dagops.state.schemas import TaskCreate
from dagops.task_status import TaskStatus


class TaskCRUD(CRUD):
    def create(
        self,
        db: Session,
        task: TaskCreate,
    ) -> models.Task:
        task_dict = task.dict()
        upstream = self.read_by_field_isin(db, 'id', task_dict['upstream'], not_found_error=True)
        task_dict['upstream'] = upstream
        worker, = worker_crud.read_by_field(db, 'name', task_dict.pop('worker_name'))
        task_dict['worker'] = worker
        db_task = models.Task(
            **task_dict,
            status=TaskStatus.PENDING,
        )
        db.add(db_task)
        db.commit()
        db.refresh(db_task)
        return db_task

    def all_success(self, db: Session) -> bool:
        return not db.query(models.Task).filter(models.Task.status != TaskStatus.SUCCESS).first()

    def n_success(self, db: Session) -> int:
        return db.query(models.Task).filter(models.Task.status == TaskStatus.SUCCESS).count()


task_crud = TaskCRUD(models.Task)
