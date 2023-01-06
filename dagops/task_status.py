import enum

class TaskStatus(enum.Enum):
# class TaskStatus:
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    CANCELED = 'CANCELED'
