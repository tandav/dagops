import asyncio
import datetime
import graphlib

from sqlalchemy.orm import Session

from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.task import Task


class Dag:
    def __init__(
        self,
        db: Session,
        graph: dict[Task, set[Task]],
        pending_queue: asyncio.Queue[Task],
        done_queue: asyncio.Queue[Task],
        # running_tasks: set[Task],
    ) -> None:
        self.db = db
        self.tasks, self.id_graph = self.extract_tasks_and_id_graph(graph)
        self.graph = graphlib.TopologicalSorter(graph)
        self.graph.prepare()
        self.pending_queue = pending_queue
        self.done_queue = done_queue
        self.created_at = None
        self.started_at = None
        self.stopped_at = None
        db_dag = dag_crud.create(db, schemas.DagCreate(graph=self.id_graph))
        self.id = db_dag.id
        # self.running_tasks = running_tasks

    @property
    def db_task(self):
        return dag_crud.read_by_id(self.db, self.id)

    def extract_tasks_and_id_graph(self, graph: dict[Task, set[Task]]) -> tuple[set[Task], dict[Task, list[Task]]]:
        tasks = set()
        id_graph = {}
        for node, predecessors in graph.items():
            tasks |= {node, *predecessors}
            id_graph[node.id] = [p.id for p in predecessors]
        return tasks, id_graph

    async def run(self) -> None:
        self.started_at = datetime.datetime.now()
        while self.graph.is_active():
            for task in self.graph.get_ready():
                await self.pending_queue.put(task)  # todo: try put in shared queue that is shared for all dags
            task = await self.done_queue.get()
            self.graph.done(task)

    def __hash__(self) -> int:
        return hash(self.id)
