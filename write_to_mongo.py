import os
import random
import time
import logging

# logger = logging.getLogger()
# logger.addHandler(logging.StreamHandler())
# fh = logging.FileHandler('log.txt')
# logger.addHandler()
# import requests
# url = 'http://127.0.0.1:5006/api'
# track_id = '637e518db6e4c5e0959fd74f'

for i in range(100):
    # print(time.time(), os.environ['TASK_NAME'])
    # logging.info(time.time(), os.environ['TASK_NAME'], '***************8')
    task_name = os.environ['TASK_NAME']
    n = int(task_name[-1])
    # print(i, task_name, '***' * n)
    print(time.time())
    # print(time.time())
    time.sleep(random.random())
