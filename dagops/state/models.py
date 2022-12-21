from sqlalchemy import JSON
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from dagops.task import TaskStatus

Base = declarative_base()


class File(Base):
    __tablename__ = 'file'

    id = Column(Integer, primary_key=True)
    path = Column(String, nullable=False)
    dag_id = Column(Integer, ForeignKey('dag.id'), nullable=True)
    dag = relationship('Dag')


class Task(Base):
    __tablename__ = 'task'

    id = Column(Integer, primary_key=True)
    dag_id = Column(Integer, ForeignKey('dag.id'))
    dag = relationship('Dag', back_populates='tasks')
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)
    status = Column(Enum(TaskStatus), nullable=False)
    command = Column(JSON, nullable=False)
    env = Column(JSON, nullable=False)

    # def to_dict(self):
    #     return {
    #         'id': self.id,
    #         'dag_id': self.dag_id,
    #         'created_at': self.created_at,
    #         'updated_at': self.updated_at,
    #         'started_at': self.started_at,
    #         'stopped_at': self.stopped_at,
    #         'status': self.status,
    #         'command': self.command,
    #         'env': self.env,
    #     }


class Dag(Base):
    __tablename__ = 'dag'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)
    status = Column(Enum(TaskStatus), nullable=False)
    graph = Column(JSON, nullable=False)
    tasks = relationship('Task', back_populates='dag')

    def to_dict(self):
        return {
            'id': self.id,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'started_at': self.started_at,
            'stopped_at': self.stopped_at,
            'status': self.status,
            'graph': self.graph,
            'tasks': [task.id for task in self.tasks],
        }
