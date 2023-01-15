from dagops.state import models
from dagops.state.crud.base import CRUD


class FileCRUD(CRUD):
    pass


file_crud = FileCRUD(models.File)
