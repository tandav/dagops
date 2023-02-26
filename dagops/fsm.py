from sqlalchemy.orm import Session
from transitions import Machine

from dagops.state import models
from dagops.state import schemas
from dagops.state.crud.task import task_crud
from dagops.status import TaskStatus


class Task:
    def __init__(self, db: Session, db_task: models.Task):
        self.db = db
        db.refresh(db_task)
        self.db_task = db_task
        self.machine = Machine(
            model=self,
            states=TaskStatus,
            initial=TaskStatus.PENDING,
        )

        # MVP: no cache check
        # self.machine.add_transition('wait_cache_path_release', TaskStatus.PENDING, TaskStatus.WAIT_CACHE_PATH_RELEASE)

        self.machine.add_transition('wait_upstream', TaskStatus.PENDING, TaskStatus.WAIT_UPSTREAM, after='update_db')
        self.machine.add_transition('queue_run', TaskStatus.WAIT_UPSTREAM, TaskStatus.QUEUED_RUN, after='update_db')
        self.machine.add_transition('run', TaskStatus.QUEUED_RUN, TaskStatus.RUNNING, after='update_db')
        self.machine.add_transition('succeed', TaskStatus.RUNNING, TaskStatus.SUCCESS, after='update_db')
        self.machine.add_transition('succeed', TaskStatus.WAIT_UPSTREAM, TaskStatus.SUCCESS, after='update_db', conditions=['is_dag'])
        self.machine.add_transition('fail', TaskStatus.RUNNING, TaskStatus.FAILED, after='update_db')
        self.machine.add_transition('cancel', '*', TaskStatus.CANCELED)

    def is_dag(self, **kwargs):
        return self.db_task.type == 'dag'

    def update_db(self, **kwargs):
        self.db_task = task_crud.update_by_id(
            self.db,
            self.db_task.id,
            schemas.TaskUpdate(
                status=self.state,
                **kwargs,
            ),
        )
