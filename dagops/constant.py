SLEEP_TIME = 0.1
CHANNEL_TASK_QUEUE = 'dagops:task_queue'
CHANNEL_TASK_STATUS = 'dagops:task_status'
CHANNEL_AIO_TASKS = 'dagops:aio_tasks'
CHANNEL_FILES = 'dagops:files'
LIST_LOGS = 'dagops:logs'
LIST_ERROR = 'dagops:errors'

workers = {'cpu': 32, 'gpu': 1}
