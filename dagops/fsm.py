import datetime
from transitions import Machine
from transitions.extensions import GraphMachine
from sqlalchemy.orm import Session

from dagops.status import TaskStatus
from dagops.state import models
from dagops.state import schemas
from uuid import UUID
from dagops.state.crud.task import task_crud


class Task:
    def __init__(self, db: Session, task_id: UUID):
        self.db = db
        self.task_id = task_id
        # self.machine = GraphMachine(
        self.machine = Machine(
            model=self,
            states=TaskStatus,
            initial=TaskStatus.PENDING,
            # transitions=[
            #     {'trigger': 'start', 'source': 'created', 'dest': 'running'},
            #     {'trigger': 'success', 'source': 'running', 'dest': 'success'},
            #     {'trigger': 'failure', 'source': 'running', 'dest': 'failure'},
            # ],
        )

        # MVP: no cache check
        # MVP: no wait upstream (as in current dagops code)
        
        # self.machine.add_transition('wait_cache_path_release', TaskStatus.PENDING, TaskStatus.WAIT_CACHE_PATH_RELEASE)
        # self.machine.add_transition('wait_upstream', TaskStatus.PENDING, TaskStatus.WAIT_UPSTREAM)
        # self.machine.add_transition('queue_run', TaskStatus.WAIT_UPSTREAM, TaskStatus.QUEUED_RUN)
        
        self.machine.add_transition('queue_run', TaskStatus.PENDING, TaskStatus.QUEUED_RUN, after='update_db')
        self.machine.add_transition('run', TaskStatus.QUEUED_RUN, TaskStatus.RUNNING, after='update_db')
        self.machine.add_transition('succeed', TaskStatus.RUNNING, TaskStatus.SUCCESS, after='update_db')
        self.machine.add_transition('fail', TaskStatus.RUNNING, TaskStatus.FAILED, after='update_db')
        self.machine.add_transition('cancel', '*', TaskStatus.CANCELED)


    def update_db(self, **kwargs):
        task = task_crud.read_by_id(self.db, self.task_id)
        task_crud.update_by_id(
            self.db,
            task.id,
            schemas.TaskUpdate(
                status=self.state,
                **kwargs,
            ),
        )
