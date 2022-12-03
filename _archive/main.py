import graphlib
import sys

from dagops.manager import DagManager
from dagops.task import ShellTask
from dagops.task import Task


class Dag:
    def __init__(self, graph: dict[Task, set[Task]] | None = None):
        # self.graph = graph
        self.dag = graphlib.TopologicalSorter(graph or {})
        # self.pool = concurrent.futures.ProcessPoolExecutor()

    def add_task(self, task: Task):
        if task in self.task_to_deps:
            return
        self.task_to_deps[task] = set()

    def add_edge(self, task: Task, dep: Task):
        if task not in self.task_to_deps:
            raise ValueError('task not in dag')
        if dep not in self.task_to_deps:
            raise ValueError('dep not in dag')
        self.task_to_deps[task].add(dep)

    def run(self):
        # task_to_deps = self.task_to_deps
        # for task in graphlib.TopologicalSorter(task_to_deps):
        #     task.run()

        self.dag.prepare()
        while self.dag.is_active():
            for task in self.dag.ready():
                task.run()
                self.dag.done(task)


def main():
    track_id = '637e518db6e4c5e0959fd74f'
    command = [sys.executable, 'write_to_mongo.py']
    audio = ShellTask(command=command, env={'TRACK_ID': track_id, 'TASK_NAME': 'audio'})
    pitch = ShellTask(command=command, env={'TRACK_ID': track_id, 'TASK_NAME': 'pitch'})
    image = ShellTask(command=command, env={'TRACK_ID': track_id, 'TASK_NAME': 'image'})
    video = ShellTask(command=command, env={'TRACK_ID': track_id, 'TASK_NAME': 'video'})

    dag = Dag()
    dag.add_task(audio)
    dag.add_task(pitch)
    dag.add_task(image)
    dag.add_task(video)

    dag.add_edge(audio, pitch)
    dag.add_edge(pitch, image)
    dag.add_edge(audio, video)
    dag.add_edge(image, video)

    dag_manager = DagManager(mongo_uri='http://127.0.0.1:5006/api')
    dag_manager.run()


if __name__ == '__main__':
    main()
