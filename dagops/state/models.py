import uuid

from sqlalchemy import JSON
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import relationship

from dagops.task_status import TaskStatus


def uuid_gen():
    return uuid.uuid4().hex


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = 'file'

    id = Column(String, primary_key=True, default=uuid_gen)
    directory = Column(String, nullable=False)
    file = Column(String, nullable=False)
    dag_id = Column(String, ForeignKey('task.id'), nullable=True)
    dag = relationship('Task')


task_to_upstream_tasks = Table(
    'task_to_upstream_tasks', Base.metadata,
    Column('task_id', String, ForeignKey('task.id'), primary_key=True),
    Column('upstream_id', String, ForeignKey('task.id'), primary_key=True),
)


class Task(Base):
    __tablename__ = 'task'

    id = Column(String, primary_key=True)
    task_type = Column(String, nullable=True)
    dag_id = Column(String, ForeignKey('task.id'), nullable=True)
    worker_id = Column(String, ForeignKey('worker.id'), nullable=True)
    worker = relationship('Worker', back_populates='tasks', foreign_keys=[worker_id])
    running_worker_id = Column(String, ForeignKey('worker.id'), nullable=True)
    running_worker = relationship('Worker', back_populates='running_tasks', foreign_keys=[running_worker_id])

    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)
    status = Column(Enum(TaskStatus), nullable=False)
    input_data = Column(JSON, nullable=True)
    output_data = Column(JSON, nullable=True)
    upstream = relationship(
        'Task',
        secondary=task_to_upstream_tasks,
        primaryjoin=id == task_to_upstream_tasks.c.task_id,
        secondaryjoin=id == task_to_upstream_tasks.c.upstream_id,
        back_populates='downstream',
    )
    downstream = relationship(
        'Task',
        secondary=task_to_upstream_tasks,
        primaryjoin=id == task_to_upstream_tasks.c.upstream_id,
        secondaryjoin=id == task_to_upstream_tasks.c.task_id,
        back_populates='upstream',
    )

    def to_dict(self):
        return {
            'id': self.id,
            'task_type': self.task_type,
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
    id = Column(String, primary_key=True, default=uuid_gen)
    name = Column(String, nullable=False)
    maxtasks = Column(Integer, nullable=True)
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
