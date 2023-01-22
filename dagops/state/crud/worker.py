from sqlalchemy.orm import Session

from dagops.state import models
from dagops.state.crud import exceptions
from dagops.state.crud.base import CRUD


class WorkerCRUD(CRUD):
    pass


worker_crud = WorkerCRUD(models.Worker)
