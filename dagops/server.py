from http import HTTPStatus
from pathlib import Path

import dotenv
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from starlette.templating import Jinja2Templates

from dagops import util
from dagops.dependencies import get_db
from dagops.dependencies import get_db_cm
from dagops.state import schemas
from dagops.state.crud.task import task_crud
from dagops.state.crud.dag import dag_crud

dotenv.load_dotenv()

# from dagops.state import State
# import dagops.state.crud.task
# import dagops.state.crud.dag
# from dagops.state import crud


static_folder = Path('static')
app = FastAPI()
# state = State()


app.mount('/static/', StaticFiles(directory=static_folder), name='static')
templates = Jinja2Templates(directory=static_folder / 'templates')
templates.env.filters['format_time'] = util.format_time


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
def api_delete_tasks(
    db: Session = Depends(get_db),
):
    db_objs = task_crud.delete_all(db)
    return db_objs


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
def api_delete_dags(
    db: Session = Depends(get_db),
):
    db_objs = dag_crud.delete_all(db)
    return db_objs


@app.get('/', response_class=HTMLResponse)
async def root():
    return RedirectResponse('/tasks/')


# @app.get('/logs/', response_class=HTMLResponse)
# async def logs(request: Request):
#     logs = await util.dirstat(static_folder / 'logs')
#     for log in logs:
#         log['log_id'] = Path(log['name']).stem
#     return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})


# @app.get('/logs/{task_id}.txt', response_class=FileResponse)
# async def log(task_id: str, request: Request):
#     return FileResponse(static_folder / 'logs' / f'{task_id}.txt')


# @app.get('/tasks/{task_id}/command.json', response_class=JSONResponse)
# async def task_command(task_id: str):
#     task = await state.get_task_info(task_id)
#     return JSONResponse(json.loads(task['command']))


# @app.get('/tasks/{task_id}/env.json', response_class=JSONResponse)
# async def task_env(task_id: str):
#     task = await state.get_task_info(task_id)
#     return JSONResponse(json.loads(task['env']))


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


# @app.get('/tasks/{task_id}', response_class=HTMLResponse)
# async def task(request: Request, task_id: str):
#     task = await state.get_task_info(task_id)
#     return templates.TemplateResponse('task.j2', {'request': request, 'task': task})


# @app.get('/dags/', response_class=HTMLResponse)
# async def dags(request: Request):
#     dags = await state.get_dags_info()
#     dags = sorted(dags.values(), key=lambda x: x['created_at'], reverse=True)
#     print(dags)
#     return templates.TemplateResponse('dags.j2', {'request': request, 'dags': dags})


# @app.get('/dags/{dag_id}', response_class=HTMLResponse)
# async def dag(request: Request):
#     dag = {}
#     return templates.TemplateResponse('dag.j2', {'request': request, 'dag': dag})
