SLEEP_TIME = 0.1
PREFIX = 'dagops'
QUEUE_TASK = f'{PREFIX}:queue:task'
QUEUE_TASK_STATUS = f'{PREFIX}:queue:task_status'
CHANNEL_AIO_TASKS = f'{PREFIX}:aio_tasks'
CHANNEL_FILES = f'{PREFIX}:files'
LIST_LOGS = f'{PREFIX}:logs'
LIST_ERROR = f'{PREFIX}:errors'
LOCKS = f'{PREFIX}:locks'

workers = {'cpu': 32, 'gpu': 1}
NOT_EXISTS_RETURNCODE = 87

default_files_exclude = frozenset({'.DS_Store'})
LOGS_TTL = 60 * 60 * 24 * 7  # 7 days
