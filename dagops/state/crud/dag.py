import datetime

from sqlalchemy.orm import Session

from dagops.state import models
from dagops.task import TaskStatus
from dagops.state.crud.base import CRUD
from dagops.state.crud.task import task_crud
from dagops.state.schemas import DagCreate


class DagCRUD(CRUD):
    def create(
        self,
        db: Session,
        dag: DagCreate,
    ) -> models.Dag:

        tasks = task_crud.read_by_field(db, "dag_id", dag.id)

        if set(task.id for task in tasks) != dag['graph'].keys():
            raise ValueError("Tasks in graph do not match tasks in database")

        if any(task.status != TaskStatus.PENDING for task in tasks):
            raise ValueError("Tasks in graph are not all pending")

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
        return db_dag

dag_crud = DagCRUD(models.Dag)
