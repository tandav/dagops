from http import HTTPStatus
from pathlib import Path

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from redis import Redis
from sqlalchemy.orm import Session
from starlette.templating import Jinja2Templates

from dagops import constant
from dagops import util
from dagops.dependencies import get_db
from dagops.dependencies import get_redis
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud
from dagops.state.crud.worker import worker_crud

static_folder = Path(__file__).parent / 'static'
app = FastAPI()


app.mount('/static/', StaticFiles(directory=static_folder), name='static')
templates = Jinja2Templates(directory=static_folder / 'templates')
templates.env.filters['format_time'] = util.format_time


# =============================================================================


@app.get('/', response_class=HTMLResponse)
async def root():
    return RedirectResponse('/tasks/')


@app.get('/favicon.ico', response_class=HTMLResponse)
async def favicon():
    return FileResponse(static_folder / 'favicon.ico')

# =============================================================================


@app.get(
    '/api/tasks/',
    response_model=list[schemas.Task],
)
def api_read_tasks(
    db: Session = Depends(get_db),
    status: str | None = None,
    # skip: int = 0,
    # limit: int = 100,
):
    if status is None:
        db_objects = task_crud.read_many(db)
    else:
        db_objects = task_crud.read_by_field(db, 'status', status)
    db_objects = [x.to_dict() for x in db_objects]
    return db_objects


@app.get(
    '/api/tasks/{task_id}',
    response_model=schemas.Task,
)
def api_read_task(
    task_id: str,
    db: Session = Depends(get_db),
):
    db_obj = task_crud.read_by_id(db, task_id)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='task not found')
    return db_obj.to_dict()


@app.post(
    '/api/tasks/',
    response_model=schemas.Task,
)
def api_create_task(
    task: schemas.TaskCreate,
    db: Session = Depends(get_db),
):
    db_obj = task_crud.create(db, task)
    return db_obj.to_dict()


@app.patch(
    '/api/tasks/{task_id}',
)
def api_update_task(
    task_id: str,
    task: schemas.TaskUpdate,
    db: Session = Depends(get_db),
):
    db_obj = task_crud.update_by_id(db, task_id, task)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='task not found')
    return db_obj


@app.delete(
    '/api/tasks/{task_id}',
)
def api_delete_task(
    task_id: str,
    db: Session = Depends(get_db),
):
    n_rows = task_crud.delete_by_field(db, 'id', task_id)
    if n_rows == 0:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='task not found')
    return n_rows


@app.delete(
    '/api/tasks/',
)
def api_delete_all_tasks(
    db: Session = Depends(get_db),
):
    return task_crud.delete_all(db)


# ------------------------------------------------------------------------------


@app.get('/tasks/', response_class=HTMLResponse)
async def read_tasks(
    request: Request,
    db: Session = Depends(get_db),
    status: str | None = None,
    # skip: int = 0,
    # limit: int = 1000,
):
    if status is None:
        db_objects = task_crud.read_many(db)
    else:
        db_objects = task_crud.read_by_field(db, 'status', status)
    db_objects = [db_obj.to_dict() for db_obj in db_objects]
    db_objects = [schemas.Task(**db_obj) for db_obj in db_objects]
    db_objects = sorted(db_objects, key=lambda x: x.created_at, reverse=True)
    return templates.TemplateResponse('tasks.j2', {'request': request, 'tasks': db_objects})


@app.get('/tasks/{task_id}', response_class=HTMLResponse)
async def read_task(
    task_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    task = task_crud.read_by_id(db, task_id)
    task_dict = task.to_dict()
    # task = schemas.Task.from_orm(task)
    task = schemas.Task(**task_dict)
    return templates.TemplateResponse('task.j2', {'request': request, 'task': task})

# =============================================================================


@app.get(
    '/api/dags/',
    response_model=list[schemas.Task],
)
def api_read_dags(
    # skip: int = 0,
    # limit: int = 100,
    db: Session = Depends(get_db),
):
    db_objects = task_crud.read_by_field(db, 'dag_id', None)
    db_objects = [x.to_dict() for x in db_objects]
    return db_objects


# @app.get(
#     '/api/dags/{dag_id}',
#     response_model=schemas.Dag,
# )
# def api_read_dag(
#     dag_id: str,
#     db: Session = Depends(get_db),
# ):
#     db_obj = dag_crud.read_by_id(db, dag_id)
#     if db_obj is None:
#         raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='dag not found')
#     db_obj = db_obj.to_dict()
#     return db_obj


@app.post(
    '/api/dags/',
    response_model=schemas.Task,
)
def api_create_dag(
    dag: schemas.DagCreate,
    db: Session = Depends(get_db),
):
    db_obj = dag_crud.create(db, dag)
    db_obj = db_obj.to_dict()
    return db_obj


# ------------------------------------------------------------------------------


@app.get('/dags/', response_class=HTMLResponse)
async def read_dags(
    request: Request,
    status: str | None = None,
    db: Session = Depends(get_db),
):
    db_objects = task_crud.read_by_field(db, 'dag_id', None)
    if status is not None:
        db_objects = [x for x in db_objects if x.status.value == status]
    db_objects = [x.to_dict() for x in db_objects]
    db_objects = [schemas.Task(**db_obj) for db_obj in db_objects]
    db_objects = sorted(db_objects, key=lambda x: x.created_at, reverse=True)
    return templates.TemplateResponse('tasks.j2', {'request': request, 'tasks': db_objects, 'heading': 'dags'})


# @app.get('/dags/{dag_id}', response_class=HTMLResponse)
# async def dag(
#     dag_id: str,
#     request: Request,
#     db: Session = Depends(get_db),
# ):
#     dag = dag_crud.read_by_id(db, dag_id)
#     if dag is None:
#         raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='dag not found')
#     dag = dag.to_dict()
#     dag = schemas.Dag(**dag)
#     tasks = task_crud.read_by_field_isin(db, 'id', dag.tasks)
#     tasks = [schemas.Task.from_orm(task) for task in tasks]
#     tasks = sorted(tasks, key=lambda x: x.created_at, reverse=True)
#     return templates.TemplateResponse('dag.j2', {'request': request, 'dag': dag, 'tasks': tasks})


# =============================================================================


@app.get(
    '/api/files/',
    response_model=list[schemas.File],
)
def api_read_files(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    db_objects = file_crud.read_many(db, skip, limit)
    return db_objects


@app.post(
    '/api/files/',
    response_model=schemas.File,
)
def api_create_file(
    file: schemas.FileCreate,
    db: Session = Depends(get_db),
):
    db_obj = file_crud.create(db, file)
    return db_obj


@app.post(
    '/api/files/many',
    response_model=list[schemas.File],
)
def api_create_files(
    files: list[schemas.FileCreate],
    db: Session = Depends(get_db),
):
    db_objs = file_crud.create_many(db, files)
    return db_objs


@app.patch(
    '/api/files/{file_id}',
)
def api_update_file(
    file_id: str,
    file: schemas.FileUpdate,
    db: Session = Depends(get_db),
):
    db_obj = file_crud.update_by_id(db, file_id, file)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='file not found')
    return db_obj


@app.delete(
    '/api/files/{file_id}',
)
def api_delete_file(
    file_id: str,
    db: Session = Depends(get_db),
):
    n_rows = file_crud.delete_by_field(db, 'id', file_id)
    if n_rows == 0:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='file not found')
    return n_rows


@app.delete(
    '/api/files/',
)
def api_delete_all_files(
    db: Session = Depends(get_db),
):
    return file_crud.delete_all(db)


# ------------------------------------------------------------------------------


@app.get(
    '/files/',
    response_class=HTMLResponse,
)
def read_files(
    request: Request,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    db_objects = file_crud.read_many(db, skip, limit)
    return templates.TemplateResponse('files.j2', {'request': request, 'files': db_objects})


@app.get(
    '/files/{file_id}',
    response_class=HTMLResponse,
)
def read_file(
    file_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    db_obj = file_crud.read_by_id(db, file_id)
    return templates.TemplateResponse('file.j2', {'request': request, 'file': db_obj})


# =============================================================================

@app.get(
    '/api/workers/',
    response_model=list[schemas.Worker],
)
def api_read_workers(
    db: Session = Depends(get_db),
):
    db_objects = [db_obj.to_dict() for db_obj in worker_crud.read_many(db)]
    return db_objects


@app.get(
    '/api/workers/{worker_id}',
    response_model=schemas.Worker,
)
def api_read_worker(
    worker_id: str,
    db: Session = Depends(get_db),
):
    db_obj = worker_crud.read_by_id(db, worker_id)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='worker not found')
    return db_obj.to_dict()


# ------------------------------------------------------------------------------

@app.get(
    '/workers/',
    response_class=HTMLResponse,
)
def read_workers(
    request: Request,
    db: Session = Depends(get_db),
):
    db_objects = worker_crud.read_many(db)
    return templates.TemplateResponse('workers.j2', {'request': request, 'workers': db_objects})


@app.get('/workers/{worker_id}', response_class=HTMLResponse)
async def read_worker(
    worker_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    db_obj = worker_crud.read_by_id(db, worker_id)
    obj = schemas.Worker(**db_obj.to_dict())
    return templates.TemplateResponse('worker.j2', {'request': request, 'worker': obj})


# =============================================================================


@app.get('/logs/', response_class=HTMLResponse)
async def logs(
    request: Request,
    redis: Redis = Depends(get_redis),
):
    prefix = constant.LIST_LOGS
    logs = await redis.keys(f'{prefix}:*')
    logs = [k.removeprefix(f'{prefix}:') for k in logs]
    return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})


@app.get('/logs/{task_id}.txt', response_class=FileResponse)
async def log(
    task_id: str,
    redis: Redis = Depends(get_redis),
):
    if not await redis.exists(f'{constant.LIST_LOGS}:{task_id}'):
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='log not found')
    log_lines = await redis.lrange(f'{constant.LIST_LOGS}:{task_id}', 0, -1)
    return Response(content=''.join(log_lines), media_type='text/plain')


@app.get('/tasks/{task_id}/{json_attr}.json')
async def task_json_attr(
    task_id: str,
    json_attr: str,
    db: Session = Depends(get_db),
):
    task = task_crud.read_by_id(db, task_id)
    return getattr(task, json_attr)
