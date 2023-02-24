SLEEP_TIME = 0.1
QUEUE_TASK = 'dagops:queue:task'
QUEUE_TASK_STATUS = 'dagops:queue:task_status'
CHANNEL_AIO_TASKS = 'dagops:aio_tasks'
CHANNEL_FILES = 'dagops:files'
LIST_LOGS = 'dagops:logs'
LIST_ERROR = 'dagops:errors'

workers = {'cpu': 32, 'gpu': 1}
NOT_EXISTS_RETURNCODE = 87
