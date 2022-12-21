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
