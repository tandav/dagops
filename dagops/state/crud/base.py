from typing import Type

from sqlalchemy.orm import Session

from dagops.state.models import Base


class CRUD:
    def __init__(self, model: Type[Base]):
        self.model = model

    def read_by_id(self, db: Session, id: int) -> Base:
        return db.query(self.model).filter(self.model.id == id).first()

    def read_many(
        self,
        db: Session,
        skip: int = 0,
        limit: int = 100,
    ) -> list[Base]:
        query = db.query(self.model)
        query = query.offset(skip).limit(limit)
        return query.all()

    def read_by_field_isin(
        self,
        db: Session,
        field: str,
        values: list[str],
    ) -> list[Base]:
        query = db.query(self.model)
        query = query.filter(getattr(self.model, field).in_(values))
        return query.all()

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
            return None
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
