from sqlalchemy.orm import Session

from dagops.state import models


def read_by_id(db: Session, id: int) -> models.Dag:
    return db.query(models.Dag).filter(models.Dag.id == id).first()


def read_many(
    db: Session,
    skip: int = 0,
    limit: int = 100,
) -> list[models.Dag]:
    query = db.query(models.Dag)
    query = query.offset(skip).limit(limit)
    return query.all()
