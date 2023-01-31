from sqlalchemy.orm import Session

from dagops import constant
from dagops.dependencies import get_db_cm
from dagops.state import schemas
from dagops.state.crud.worker import worker_crud


def prepare_workers(db: Session, workers: dict[str, int] | None = None):
    dag_worker = worker_crud.read_by_field(db, 'name', 'dag')
    if len(dag_worker) == 0:
        worker_crud.create(db, schemas.WorkerCreate(name='dag'))  # dag worker for dags, todo try to remove
    for worker_name, maxtasks in workers.items():
        db_worker = worker_crud.read_by_field(db, 'name', worker_name)
        if len(db_worker) == 0:
            worker_crud.create(db, schemas.WorkerCreate(name=worker_name, maxtasks=maxtasks))
            continue
        db_worker, = db_worker
        if db_worker.maxtasks != maxtasks:
            worker_crud.update_by_id(db, db_worker.id, schemas.WorkerUpdate(maxtasks=maxtasks))


if __name__ == '__main__':
    with get_db_cm() as db:
        prepare_workers(db, constant.workers)
