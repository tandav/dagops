import asyncio
import graphlib
import uuid

from dagops.task import Task


class Dag:
    def __init__(
        self,
        graph: dict[Task, set[Task]],
        pending_queue: asyncio.Queue[Task],
        done_queue: asyncio.Queue[Task],
    ) -> None:
        self.id = str(uuid.uuid4())
        self.tasks, self.id_graph = self.extract_tasks_and_id_graph(graph)
        self.graph = graphlib.TopologicalSorter(graph)
        self.graph.prepare()
        self.pending_queue = pending_queue
        self.done_queue = done_queue
        self.created_at = None
        self.started_at = None
        self.stopped_at = None

    def extract_tasks_and_id_graph(self, graph: dict[Task, set[Task]]) -> tuple[set[Task], dict[Task, list[Task]]]:
        tasks = set()
        id_graph = {}
        for node, predecessors in graph.items():
            tasks |= {node, *predecessors}
            id_graph[node.id] = [p.id for p in predecessors]
        return tasks, id_graph

    async def run(self) -> None:
        while self.graph.is_active():
            for task in self.graph.get_ready():
                await self.pending_queue.put(task)
            task = await self.done_queue.get()
            self.graph.done(task)
