import os
import random
import time


for i in range(10):
    task_name = os.environ['TASK_NAME']
    n = int(task_name[-1])
    print(time.time())
    time.sleep(random.random())
