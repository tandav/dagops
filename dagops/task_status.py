import enum


class TaskStatus(enum.Enum):
    PENDING = 'PENDING'
    QUEUED = 'QUEUED'  # waiting worker to pick up
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    CANCELED = 'CANCELED'
