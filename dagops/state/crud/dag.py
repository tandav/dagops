import datetime

from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.crud.base import CRUD
from dagops.state.crud.task import task_crud
from dagops.state.schemas import DagCreate
from dagops.task_status import TaskStatus


class DagCRUD(CRUD):
    def create(
        self,
        db: Session,
        dag: DagCreate,
    ) -> models.Dag:

        tasks = dag.graph.keys()
        tasks = task_crud.read_by_field_isin(db, 'id', tasks)

        if {task.id for task in tasks} != dag.graph.keys():
            raise ValueError('Tasks in graph do not match tasks in database')

        if any(task.status != TaskStatus.PENDING for task in tasks):
            raise ValueError('Tasks in graph are not all pending')

        if any(task.dag_id is not None for task in tasks):
            raise ValueError('Tasks in graph must all have dag_id=None')

        db_dag = models.Dag(
            **dag.dict(),
            tasks=tasks,
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
            status=TaskStatus.PENDING,
        )
        db.add(db_dag)
        db.commit()
        db.refresh(db_dag)

        for task in tasks:
            task.dag_id = db_dag.id
        db.commit()

        return db_dag


dag_crud = DagCRUD(models.Dag)
