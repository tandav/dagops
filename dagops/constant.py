SLEEP_TIME = 0.1
PREFIX = 'dagops'
QUEUE_TASK = f'{PREFIX}:queue:task'
QUEUE_TASK_STATUS = f'{PREFIX}:queue:task_status'
CHANNEL_AIO_TASKS = f'{PREFIX}:aio_tasks'
CHANNEL_FILES = f'{PREFIX}:files'
LIST_LOGS = f'{PREFIX}:logs'
LOCKS = f'{PREFIX}:locks'
TEST_LOGS_KEY = f'{PREFIX}:test-logs'
ALL_DAEMONS_DONE_KEY = f'{PREFIX}:all-daemons-done'

workers = {'cpu': 32, 'gpu': 1}
CACHE_NOT_EXISTS_RETURNCODE = 87

default_files_exclude = frozenset({'.DS_Store'})
LOGS_TTL = 60 * 60 * 24 * 7  # 7 days
STOP_WORKER_MESSAGE = 'STOP_WORKER'
