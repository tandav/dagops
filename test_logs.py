from dagops.manager import DagManager
from dagops.task import ShellTask
import sys
import subprocess


def main():

    dag_manager = DagManager()

    command = [sys.executable, '-u', 'write_to_mongo.py']
    task1 = ShellTask(command=command, env={'TASK_NAME': 'task1'})
    task2 = ShellTask(command=command, env={'TASK_NAME': 'task2'})
    task3 = ShellTask(command=command, env={'TASK_NAME': 'task3'})
    # # dag_manager.task_queue.put(task1, block=False)
    # # dag_manager.task_queue.put(task2, block=False)
    # # dag_manager.task_queue.put(task3, block=False)
    dag_manager.task_queue.add(task1)
    dag_manager.task_queue.add(task2)
    dag_manager.task_queue.add(task3)
    dag_manager.run()

    # p = subprocess.Popen(
    #         command, 
    #         env={'TASK_NAME': 'task1'},
    #         stdout=subprocess.PIPE, 
    #         stderr=subprocess.STDOUT,  # redirect stderr to stdout
    #         text=True,
    #         # universal_newlines=True,
    #     )

    # for line in p.stdout:
    # # for line in iter(p.stdout.readline, ""):
    #     print(line.rstrip())


if __name__ == '__main__':
    main()



# Example
# command = [sys.executable, '-u', 'write_to_mongo.py']

# with subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=1, universal_newlines=True) as p:
# with subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=1, text=True) as p:
#     for line in p.stdout:
#         print(line) # process line here

