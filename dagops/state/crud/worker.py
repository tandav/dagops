from dagops.state import models
from dagops.state.crud.base import CRUD


class WorkerCRUD(CRUD):
    pass


worker_crud = WorkerCRUD(models.Worker)
