import datetime

from sqlalchemy.orm import Session
from transitions import Machine

from dagops.state import models
from dagops.state import schemas
from dagops.status import TaskStatus


class Task:
    def __init__(self, db: Session, db_obj: models.Task):
        self.db = db
        db.refresh(db_obj)
        self.db_obj = db_obj
        self.machine = Machine(
            model=self,
            states=TaskStatus,
            initial=TaskStatus.PENDING,
        )

        # MVP: no cache check
        # self.machine.add_transition('wait_cache_path_release', TaskStatus.PENDING, TaskStatus.WAIT_CACHE_PATH_RELEASE)

        self.machine.add_transition('wait_upstream', TaskStatus.PENDING, TaskStatus.WAIT_UPSTREAM, conditions=['is_dag'], after=['update_started_at', 'update_db'])
        self.machine.add_transition('wait_upstream', TaskStatus.PENDING, TaskStatus.WAIT_UPSTREAM, after='update_db')

        self.machine.add_transition('check_upstream', TaskStatus.WAIT_UPSTREAM, TaskStatus.SUCCESS, conditions=['all_upstream_success', 'is_dag'], after='update_db')
        self.machine.add_transition('check_upstream', TaskStatus.WAIT_UPSTREAM, TaskStatus.QUEUED_RUN, conditions=['all_upstream_success'], after='update_db')
        self.machine.add_transition('check_upstream', TaskStatus.WAIT_UPSTREAM, TaskStatus.FAILED, conditions=['any_upstream_failed'], after='update_db')

        self.machine.add_transition('run', TaskStatus.QUEUED_RUN, TaskStatus.RUNNING, unless=['is_dag'], after=['update_started_at', 'update_db'])
        self.machine.add_transition('run', TaskStatus.QUEUED_RUN, TaskStatus.RUNNING, after='update_db')

        self.machine.add_transition('succeed', TaskStatus.RUNNING, TaskStatus.SUCCESS, after='update_db')
        self.machine.add_transition('fail', TaskStatus.RUNNING, TaskStatus.FAILED, after='update_db')
        self.machine.add_transition('cancel', '*', TaskStatus.CANCELED)

        for method in ('delete_running_worker', 'update_stopped_at'):
            self.machine.on_enter_FAILED(method)
            self.machine.on_enter_SUCCESS(method)

    def all_upstream_success(self, upstream: list[models.Task]):
        return all(u.status == TaskStatus.SUCCESS for u in upstream)

    def any_upstream_failed(self, upstream: list[models.Task]):
        return any(u.status == TaskStatus.FAILED for u in upstream)

    def is_dag(self, **kwargs):
        return self.db_obj.type == 'dag'

    def update_db(self, **kwargs):
        obj = schemas.TaskUpdate(
            status=self.state,
            **kwargs,
        )
        for key, value in obj.dict(exclude_unset=True).items():
            setattr(self.db_obj, key, value)
        self.db.commit()
        self.db.refresh(self.db_obj)

    def delete_running_worker(self, **kwargs):
        self.db_obj.running_worker_id = None

    def update_started_at(self, **kwargs):
        self.db_obj.started_at = datetime.datetime.now(tz=datetime.timezone.utc)

    def update_stopped_at(self, **kwargs):
        self.db_obj.stopped_at = datetime.datetime.now(tz=datetime.timezone.utc)
