import os
from http import HTTPStatus
from pathlib import Path

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from starlette.templating import Jinja2Templates

from dagops import util
from dagops.dependencies import get_db
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.crud.task import task_crud

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
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    db_objects = task_crud.read_many(db, skip, limit)
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
    return db_obj


@app.post(
    '/api/tasks/',
    response_model=schemas.Task,
)
def api_create_task(
    task: schemas.TaskCreate,
    db: Session = Depends(get_db),
):
    db_obj = task_crud.create(db, task)
    return db_obj


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
    db_obj = task_crud.delete_by_id(db, task_id)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='task not found')
    return db_obj


@app.delete(
    '/api/tasks/',
)
def api_delete_all_tasks(
    db: Session = Depends(get_db),
):
    db_objs = task_crud.delete_all(db)
    return db_objs


# ------------------------------------------------------------------------------


@app.get('/tasks/', response_class=HTMLResponse)
async def tasks(
    request: Request,
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
):
    tasks = task_crud.read_many(db, skip, limit)
    tasks = [schemas.Task.from_orm(task) for task in tasks]
    tasks = sorted(tasks, key=lambda x: x.created_at, reverse=True)
    return templates.TemplateResponse('tasks.j2', {'request': request, 'tasks': tasks})


@app.get('/tasks/{task_id}', response_class=HTMLResponse)
async def read_task(
    task_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    task = task_crud.read_by_id(db, task_id)
    task = schemas.Task.from_orm(task)
    return templates.TemplateResponse('task.j2', {'request': request, 'task': task})

# =============================================================================


@app.get(
    '/api/dags/',
    response_model=list[schemas.Dag],
)
def api_read_dags(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    db_objects = dag_crud.read_many(db, skip, limit)
    db_objects = [x.to_dict() for x in db_objects]
    return db_objects


@app.get(
    '/api/dags/{dag_id}',
    response_model=schemas.Dag,
)
def api_read_dag(
    dag_id: str,
    db: Session = Depends(get_db),
):
    db_obj = dag_crud.read_by_id(db, dag_id)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='dag not found')
    db_obj = db_obj.to_dict()
    return db_obj


@app.post(
    '/api/dags/',
    response_model=schemas.Dag,
)
def api_create_dag(
    dag: schemas.DagCreate,
    db: Session = Depends(get_db),
):
    db_obj = dag_crud.create(db, dag)
    db_obj = db_obj.to_dict()
    return db_obj


@app.patch(
    '/api/dags/{dag_id}',
)
def api_update_dag(
    dag_id: str,
    dag: schemas.DagUpdate,
    db: Session = Depends(get_db),
):
    db_obj = dag_crud.update_by_id(db, dag_id, dag)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='dag not found')
    db_obj = db_obj.to_dict()
    return db_obj


@app.delete(
    '/api/dags/{dag_id}',
)
def api_delete_dag(
    dag_id: str,
    db: Session = Depends(get_db),
):
    db_obj = dag_crud.delete_by_id(db, dag_id)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='dag not found')
    db_obj = db_obj.to_dict()
    return db_obj


@app.delete(
    '/api/dags/',
)
def api_delete_all_dags(
    db: Session = Depends(get_db),
):
    db_objs = dag_crud.delete_all(db)
    return db_objs

# ------------------------------------------------------------------------------


@app.get('/dags/', response_class=HTMLResponse)
async def read_dags(
    request: Request,
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
):
    dags = dag_crud.read_many(db, skip, limit)
    dags = [schemas.Dag(**dag.to_dict()) for dag in dags]
    dags = sorted(dags, key=lambda x: x.created_at, reverse=True)
    return templates.TemplateResponse('dags.j2', {'request': request, 'dags': dags})


@app.get('/dags/{dag_id}', response_class=HTMLResponse)
async def dag(
    dag_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    dag = dag_crud.read_by_id(db, dag_id)
    if dag is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='dag not found')
    dag = dag.to_dict()
    dag = schemas.Dag(**dag)
    tasks = task_crud.read_by_field_isin(db, 'id', dag.tasks)
    tasks = [schemas.Task.from_orm(task) for task in tasks]
    tasks = sorted(tasks, key=lambda x: x.created_at, reverse=True)
    return templates.TemplateResponse('dag.j2', {'request': request, 'dag': dag, 'tasks': tasks})


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
    db_obj = file_crud.delete_by_id(db, file_id)
    if db_obj is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='file not found')
    return db_obj


@app.delete(
    '/api/files/',
)
def api_delete_all_files(
    db: Session = Depends(get_db),
):
    db_objs = file_crud.delete_all(db)
    return db_objs


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


@app.get('/logs/', response_class=HTMLResponse)
async def logs(request: Request):
    logs = await util.dirstat(static_folder / 'logs')
    for log in logs:
        log['log_id'] = Path(log['name']).stem
    return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})


@app.get('/logs/{task_id}.txt', response_class=FileResponse)
async def log(task_id: str):
    file = Path(os.environ['LOGS_DIRECTORY']) / f'{task_id}.txt'
    if not file.exists():
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='log not found')
    return FileResponse(file)


@app.get('/tasks/{task_id}/{json_attr}.json')
async def task_json_attr(
    task_id: str,
    json_attr: str,
    db: Session = Depends(get_db),
):
    task = task_crud.read_by_id(db, task_id)
    return getattr(task, json_attr)


@app.get('/dags/{dag_id}/{json_attr}.json')
async def dag_json_attr(
    dag_id: str,
    json_attr: str,
    db: Session = Depends(get_db),
):
    dag = dag_crud.read_by_id(db, dag_id)
    return getattr(dag, json_attr)
