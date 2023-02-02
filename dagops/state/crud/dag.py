import functools
import graphlib
import uuid

from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.schemas import DagCreate
from dagops.task_status import TaskStatus


@functools.cache
def read_worker(db: Session, worker_name: str) -> models.Worker:
    worker = db.query(models.Worker).filter(models.Worker.name == worker_name).first()
    if not worker:
        raise ValueError(f'worker {worker_name} not found')
    return worker


# class DagCRUD(CRUD):
class DagCRUD:
    def create(
        self,
        db: Session,
        dag: DagCreate,
    ) -> models.Task:
        head_task = models.Task(
            id=uuid.uuid4(),
            task_type='dag',
            worker=read_worker(db, 'dag'),
            status=TaskStatus.PENDING,
        )
        db.add(head_task)

        task_input_data_id_to_db_task = {}
        for task_input_data_id in graphlib.TopologicalSorter(dag.graph).static_order():
            input_data = dag.tasks_input_data[task_input_data_id]
            db_task = models.Task(
                task_type=dag.task_type,
                input_data=input_data,
                worker=read_worker(db, input_data.pop('worker_name')),
                upstream=[task_input_data_id_to_db_task[td] for td in dag.graph[task_input_data_id]],
                dag_id=head_task.id,
                status=TaskStatus.PENDING,
            )
            db.add(db_task)
            task_input_data_id_to_db_task[task_input_data_id] = db_task

        head_task.upstream = list(task_input_data_id_to_db_task.values())
        db.commit()
        db.refresh(head_task)
        return head_task


dag_crud = DagCRUD()
