import datetime

from sqlalchemy.orm import Session


from dagops.state import models


def read_by_id(db: Session, id: int) -> models.Task:
    return db.query(models.Task).filter(models.Task.id == id).first()


# def read_many(
#     db: Session,
#     username: str | None = None,
#     authenticated_username: str | None = None,
#     skip: int = 0,
#     limit: int = 100,
# ) -> list[models.Note]:

#     query = db.query(models.Note)

#     if authenticated_username is None:
#         authenticated_username = 'anon'

#     authenticated_user = crud.user.read_by_username(db, authenticated_username, not_found_error=True)
#     query = query.filter((models.Note.user == authenticated_user) | (models.Note.is_private == False))

#     if username is not None:
#         user = crud.user.read_by_username(db, username, not_found_error=True)
#         query = query.filter(models.Note.user == user)

#     query = query.offset(skip).limit(limit)
#     return query.all()
