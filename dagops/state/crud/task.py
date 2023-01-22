import datetime

from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.crud import exceptions
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
        now = datetime.datetime.now()
        task_dict = task.dict()
        upstream = self.read_by_field_isin(db, 'id', task_dict['upstream'], not_found_ok=False)
        task_dict['upstream'] = upstream
        worker, = worker_crud.read_by_field(db, 'name', task_dict.pop('worker_name'))
        task_dict['worker'] = worker
        # print(task_dict)
        db_task = models.Task(
            **task_dict,
            created_at=now,
            updated_at=now,
            status=TaskStatus.PENDING,
        )

        db.add(db_task)
        db.commit()
        db.refresh(db_task)
        return db_task

    def update_by_id(self, db: Session, id: int, obj):
        db_obj = self.read_by_id(db, id)
        if db_obj is None:
            raise exceptions.HttpNotFound(f'No {self.model.__name__} with id {id} found')

        # SPECIAL_HANDLING = {'running_worker_id'}
        obj_dict = obj.dict(exclude_unset=True)
        for key, value in obj_dict.items():
            # if key in SPECIAL_HANDLING:
            # continue
            setattr(db_obj, key, value)

        # if obj.worker_id is None:
        # if 'running_worker_id' in obj_dict:
            # if obj_dict['running_worker_id'] is not None:
            # raise NotImplementedError('Changing running_worker_id is not implemented yet')
            # print('Setting running_worker_id to None')
            # db_obj.running_worker = None
            # db_obj.running_worker_id = None

        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj


task_crud = TaskCRUD(models.Task)
