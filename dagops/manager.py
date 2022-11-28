import requests
import graphlib
import queue
import time
from dagops.task import TaskStatus


class DagManager:
    # def __init__(self, mongo_uri: str):
    def __init__(self):
        # self.mongo_uri = mongo_uri
        # self.tasks = set()
        self.dags = set()
        # self.task_queue = queue.Queue()
        # self.done_tasks_queue = queue.Queue()
        self.task_queue = set()
        self.done_tasks_queue = set()
        self.running = set()

    # def add_task(self, task: Task):
        # self.tasks.add(task)

    def add_dag(self, dag: graphlib.TopologicalSorter):
        self.dags.add(dag)

    def run(self):
        while True:
            # while not self.task_queue.empty():
            # while self.task_queue:
            print(0)
            time.sleep(0.5)
            for task in self.task_queue:
                print(1, task)
                # task = self.task_queue.get(block=False)
                task.run()
                self.running.add(task)
            self.task_queue.clear()
            for task in frozenset(self.running):
                print(4, self.running)
                if task.status in (TaskStatus.SUCCESS, TaskStatus.FAILED):
                    print(5)
                    # self.done_tasks_queue.put(task)
                    self.done_tasks_queue.add(task)
                    self.running.remove(task)
                else:
                    print(6)
                    task.handle_running()
            
            # for dag in self.dags:
            #     for task in dag.ready():
            #         task.run()
            #         dag.done(task)


            # tasks = requests.get(f'{self.mongo_uri}/test_db/tasks/').json()
            # for task in tasks:
            #     if task['status'] == 'pending':
            #         requests.patch(
            #             f'{self.mongo_uri}/test_db/tasks/{task["_id"]}',
            #             json={'status': 'running'},
            #         )
            #         # task.run()
            #         requests.patch(
            #             f'{self.mongo_uri}/test_db/tasks/{task["_id"]}',
            #             json={'status': 'done'},
            #         )



# def main():
#     while True:
#         try:
#             task = Task.from_input()
#         except ValueError as e:
#             print(e)
#             continue
#         break

# if __name__ == '__main__':
#     main()
