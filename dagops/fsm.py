from transitions import Machine
from transitions.extensions import GraphMachine

from dagops.status import TaskStatus


class Task:
    def __init__(self):
        self.machine = GraphMachine(
            # self.machine = Machine(
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
        # self.machine.add_transition('wait_cache_path_release', TaskStatus.PENDING, TaskStatus.WAIT_CACHE_PATH_RELEASE)
        self.machine.add_transition('queue_run', TaskStatus.PENDING, TaskStatus.QUEUED_RUN)
        self.machine.add_transition('run', TaskStatus.QUEUED_RUN, TaskStatus.RUNNING)
        self.machine.add_transition('success', TaskStatus.RUNNING, TaskStatus.SUCCESS)
        self.machine.add_transition('failure', TaskStatus.RUNNING, TaskStatus.FAILED)
        self.machine.add_transition('cancel', '*', TaskStatus.CANCELED)
