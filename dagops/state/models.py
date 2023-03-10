import datetime
import uuid

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import relationship

from dagops.state.status import TaskStatus


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = 'file'

    # columns
    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    storage = Column(String, nullable=False)
    directory = Column(String, nullable=False)
    file = Column(String, nullable=False)
    dag_id = Column(UUID, ForeignKey('task.id'), nullable=True)

    # relationships
    dag = relationship('Task')


task_to_upstream_tasks = Table(
    'task_to_upstream_tasks', Base.metadata,
    Column('task_id', UUID, ForeignKey('task.id', ondelete='CASCADE'), primary_key=True),
    Column('upstream_id', UUID, ForeignKey('task.id', ondelete='CASCADE'), primary_key=True),
)


class Task(Base):
    __tablename__ = 'task'

    # columns
    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    type = Column(String, nullable=True)
    dag_id = Column(UUID, ForeignKey('task.id'), nullable=True)
    worker_id = Column(UUID, ForeignKey('worker.id'), nullable=True)
    running_worker_id = Column(UUID, ForeignKey('worker.id'), nullable=True)
    daemon_id = Column(UUID, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)
    status = Column(Enum(TaskStatus), nullable=False)
    input_data = Column(JSONB, nullable=True)
    output_data = Column(JSONB, nullable=True)

    # relationships
    worker = relationship('Worker', back_populates='tasks', foreign_keys=[worker_id])
    running_worker = relationship('Worker', back_populates='running_tasks', foreign_keys=[running_worker_id])
    upstream = relationship(
        'Task',
        secondary=task_to_upstream_tasks,
        primaryjoin=id == task_to_upstream_tasks.c.task_id,
        secondaryjoin=id == task_to_upstream_tasks.c.upstream_id,
        back_populates='downstream',
        cascade='all,delete',
        passive_deletes=True,
    )
    downstream = relationship(
        'Task',
        secondary=task_to_upstream_tasks,
        primaryjoin=id == task_to_upstream_tasks.c.upstream_id,
        secondaryjoin=id == task_to_upstream_tasks.c.task_id,
        back_populates='upstream',
        cascade='all,delete',
        passive_deletes=True,
    )

    def to_dict(self):
        return {
            'id': self.id,
            'type': self.type,
            'dag_id': self.dag_id,
            'worker_id': self.worker_id,
            'worker_name': self.worker.name if self.worker else None,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'started_at': self.started_at,
            'stopped_at': self.stopped_at,
            'status': self.status,
            'input_data': self.input_data,
            'output_data': self.output_data,
            'upstream': [task.id for task in self.upstream],
            'downstream': [task.id for task in self.downstream],
        }


class Worker(Base):
    __tablename__ = 'worker'

    # columns
    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False, unique=True)
    maxtasks = Column(Integer, nullable=True)

    # relationships
    tasks = relationship('Task', back_populates='worker', foreign_keys=[Task.worker_id])
    running_tasks = relationship('Task', back_populates='running_worker', foreign_keys=[Task.running_worker_id])

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'maxtasks': self.maxtasks,
            'tasks': [task.id for task in self.tasks],
            'running_tasks': [task.id for task in self.running_tasks],
        }
