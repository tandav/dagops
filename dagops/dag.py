from dagops.task import Task
import graphlib
import asyncio


class Dag:
    def __init__(self, graph: dict[Task, set[Task]]) -> None:
        self.graph = graph

    async def run(self) -> None:
        for task in self.graph:
            await task.run()
