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
    db_notes = task_crud.read_many(db, skip, limit)
    db_notes = [db_note.to_dict() for db_note in db_notes]
    print(db_notes[0])
    return db_notes


@app.get(
    '/api/tasks/{task_id}',
    response_model=schemas.Task,
)
def api_read_task(
    task_id: int,
    db: Session = Depends(get_db),
):
    db_note = task_crud.read_by_id(db, task_id)
    if db_note is None:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='task not found')

    db_note = db_note.to_dict()
    return db_note


@app.post(
    '/api/tasks/',
    response_model=schemas.Task,
)
def api_create_task(
    task: schemas.TaskCreate,
    db: Session = Depends(get_db),
):
    db_note = task_crud.create(db, task)
    # return db_note.to_dict()
    return db_note




# @app.get(
#     '/api/dags/',
#     response_model=list[schemas.Dag],
# )
# def api_read_dags(
#     db: Session = Depends(get_db),
#     skip: int = 0,
#     limit: int = 100,
# ):
#     tasks = [schemas.Dag(**x.to_dict()) for x in crud.dag.read_many(db, skip, limit)]
#     return tasks


# @app.get('/', response_class=HTMLResponse)
# async def root():
#     return RedirectResponse('/tasks/')


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


# @app.get('/tasks/', response_class=HTMLResponse)
# async def tasks(
#     request: Request,
#     db: Session = Depends(get_db),
# ):
#     tasks = await state.get_tasks_info()
#     tasks = sorted(tasks.values(), key=lambda x: x['created_at'], reverse=True)
#     return templates.TemplateResponse('tasks.j2', {'request': request, 'tasks': tasks})


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
