from dagops.task import Task
import graphlib
import asyncio


class Dag:
    def __init__(
        self,
        graph: dict[Task, set[Task]],
        queue: asyncio.Queue[Task],
    ) -> None:
        self.graph = graphlib.TopologicalSorter(graph)
        self.graph.prepare()
        self.queue = queue

    async def run(self) -> None:
        while self.graph.is_active():
            for task in self.graph.ready():
                await self.queue.put(task)
            task = self.queue.get()
            self.graph.done(task)
