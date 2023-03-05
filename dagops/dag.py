from dagops.state import schemas


class Dag:
    def __init__(self, graph: dict):
        self.validate_dag(graph)
        self.graph = graph
        self.input_data, self.id_graph = self.prepare_dag(graph)

    def prepare_dag(self, graph: schemas.InputDataDag) -> schemas.DagCreate:
        input_data = [None] * len(graph)
        task_to_id = {}
        for i, task in enumerate(graph):
            task_to_id[task] = i
            input_data[i] = task
        id_graph = {}
        for task, deps in graph.items():
            id_graph[task_to_id[task]] = [task_to_id[dep] for dep in deps]
        return input_data, id_graph

    @staticmethod
    def validate_dag(dag: dict[schemas.ShellTaskInputData, list[schemas.ShellTaskInputData]]):
        for task, deps in dag.items():
            for dep in deps:
                if dep not in dag:
                    raise ValueError(f'dependency {dep} of task {task} not in dag')

    def __len__(self):
        return len(self.graph)
