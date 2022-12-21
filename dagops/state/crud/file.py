from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.schemas import TaskCreate
from dagops.task import TaskStatus


def read_by_id(db: Session, id: int) -> models.File:
    return db.query(models.File).filter(models.File.id == id).first()


def read_many(
    db: Session,
    skip: int = 0,
    limit: int = 100,
) -> list[models.File]:
    query = db.query(models.File)
    query = query.offset(skip).limit(limit)
    return query.all()


# def create(
#     db: Session,
#     task: TaskCreate,
# ) -> models.Task:
#     db_task = models.Task(
#         **task.dict(),
#         created_at=datetime.datetime.now(),
#         updated_at=datetime.datetime.now(),
#         status=TaskStatus.PENDING,
#     )
#     db.add(db_task)
#     db.commit()
#     db.refresh(db_task)
#     return db_task
