import datetime
import graphlib
import uuid

from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state import schemas
from dagops.state.crud.task import task_crud
from dagops.state.schemas import DagCreate
from dagops.task_status import TaskStatus


# class DagCRUD(CRUD):
class DagCRUD:
    def create(
        self,
        db: Session,
        dag: DagCreate,
    ) -> models.Task:

        now = datetime.datetime.now()

        # create dag head task
        head_task_id = uuid.uuid4().hex

        head_task_create = schemas.TaskCreate(
            task_type='dag',
            id=head_task_id,
            worker_name='dummy',
            # upstream=[task.id for task in task_input_data_id_to_db_task.values()],
        )
        head_task = task_crud.create(db, head_task_create)
        task_input_data_id_to_db_task = {}
        for task_input_data_id in graphlib.TopologicalSorter(dag.graph).static_order():
            input_data = dag.tasks_input_data[task_input_data_id]

            worker = db.query(models.Worker).filter(models.Worker.name == input_data.pop('worker_name')).first()
            print(worker.id, worker.name)
            db_task = models.Task(
                id=uuid.uuid4().hex,
                task_type=dag.task_type,
                input_data=input_data,
                worker=worker,
                upstream=[task_input_data_id_to_db_task[td] for td in dag.graph[task_input_data_id]],
                dag_id=head_task.id,
                created_at=now,
                updated_at=now,
                status=TaskStatus.PENDING,
            )

            # task_create = schemas.TaskCreate(
            #     task_type=dag.task_type,
            #     input_data=input_data,
            #     worker_name=input_data['worker_name'],
            #     upstream=[task_input_data_id_to_db_task[td].id for td in dag.graph[task_input_data_id]],
            #     # dag_id=head_task_id,
            #     dag_id=head_task.id,
            # )
            # db_task = task_crud.create(db, task_create, commit=False)
            db.add(db_task)
            task_input_data_id_to_db_task[task_input_data_id] = db_task
        # db.commit()
        # db.refresh(head_task)
        # head_task.upstream = [task.id for task in task_input_data_id_to_db_task.values()]
        head_task.upstream = list(task_input_data_id_to_db_task.values())
        # head_task = task_crud.update_by_id(db, head_task.id, schemas.TaskUpdate(
        #     upstream=[task.id for task in task_input_data_id_to_db_task.values()],
        # ))

        db.commit()
        db.refresh(head_task)
        return head_task

        # head_task_create = schemas.TaskCreate(
        #     task_type='dag',
        #     id=head_task_id,
        #     worker_name='dummy',
        #     upstream=[task.id for task in task_input_data_id_to_db_task.values()],
        # )
        # head_task = task_crud.create(db, head_task_create)
        # db.add(head_task)
        # db.commit()
        # db.refresh(head_task)

        # tasks = [task_crud.read_by_id(db, task_id) for task_id in graph_sorted]

        # tasks = dag.graph.keys()
        # tasks = task_crud.read_by_field_isin(db, 'id', tasks)

        # if {task.id for task in tasks} != dag.graph.keys():
        #     raise ValueError('Tasks in graph do not match tasks in database')

        # if any(task.status != TaskStatus.PENDING for task in tasks):
        #     raise ValueError('Tasks in graph are not all pending')

        # if any(task.dag_id is not None for task in tasks):
        #     raise ValueError('Tasks in graph must all have dag_id=None')

        # db_dag = models.Dag(
        #     **dag.dict(),
        #     tasks=tasks,
        #     created_at=datetime.datetime.now(),
        #     updated_at=datetime.datetime.now(),
        #     status=TaskStatus.PENDING,
        # )
        # db.add(db_dag)
        # db.commit()
        # db.refresh(db_dag)

        # for task in tasks:
        #     task.dag_id = db_dag.id
        # db.commit()

        # return db_dag
# dag_crud = DagCRUD(models.Dag)


dag_crud = DagCRUD()
