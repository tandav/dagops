from typing import Type

from sqlalchemy.orm import Session

from dagops.state.crud import exceptions
from dagops.state.models import Base


class CRUD:
    def __init__(self, model: Type[Base]):
        self.model = model

    def read_by_id(self, db: Session, id: int) -> Base:
        return db.query(self.model).filter(self.model.id == id).first()

    def read_many(
        self,
        db: Session,
        skip: int | None = None,
        limit: int | None = None,
    ) -> list[Base]:
        query = db.query(self.model)
        if skip is not None and limit is not None:
            query = query.offset(skip).limit(limit)
        return query.all()

    def read_by_field_isin(
        self,
        db: Session,
        field: str,
        values: list[str],
        not_found_ok: bool = True,
    ) -> list[Base]:
        if len(values) == 0:
            return []
        query = db.query(self.model)
        query = query.filter(getattr(self.model, field).in_(values))
        db_objs = query.all()
        if not_found_ok:
            return db_objs
        if len(db_objs) != len(values):
            raise exceptions.HttpNotFound(
                f'Not all {field}s found in database: {values}',
            )
        return db_objs
        # query = db.query(self.model)
        # query = query.filter(getattr(self.model, field).in_(values))
        # return query.all()

    def read_by_field(
        self,
        db: Session,
        field: str,
        value: str,
    ) -> list[Base]:
        query = db.query(self.model)
        query = query.filter(getattr(self.model, field) == value)
        return query.all()

    def create(self, db: Session, obj: Base) -> Base:
        db_obj = self.model(**obj.dict())
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def create_many(self, db: Session, objs: list[Base]) -> list[Base]:
        if len(objs) == 0:
            return []
        db_objs = [self.model(**obj.dict()) for obj in objs]
        db.add_all(db_objs)
        db.commit()
        for db_obj in db_objs:
            db.refresh(db_obj)
        return db_objs

    def update_by_id(self, db: Session, id: int, obj: Base) -> Base:
        db_obj = self.read_by_id(db, id)
        if db_obj is None:
            raise exceptions.HttpNotFound(f'No {self.model.__name__} with id {id} found')
        for key, value in obj.dict(exclude_unset=True).items():
            setattr(db_obj, key, value)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    def delete_by_id(self, db: Session, id: int) -> Base:
        db_obj = self.read_by_id(db, id)
        if db_obj is None:
            return None
        db.delete(db_obj)
        db.commit()
        return db_obj

    def delete_many_by_ids(self, db: Session, ids: list[int]) -> list[Base]:
        query = db.query(self.model)
        query = query.filter(self.model.id.in_(ids))
        query.delete(synchronize_session=False)
        db.commit()
        return query.all()

    def delete_all(
        self,
        db: Session,
    ) -> list[Base]:
        query = db.query(self.model)
        query.delete()
        db.commit()
        return query.all()
