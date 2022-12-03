from pathlib import Path

import dotenv
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from dagops import util
from dagops.state import State

dotenv.load_dotenv()

static_folder = Path('static')
app = FastAPI()
state = State()


app.mount('/static/', StaticFiles(directory=static_folder), name='static')
templates = Jinja2Templates(directory=static_folder / 'templates')
templates.env.filters['format_time'] = util.format_time


@app.get('/', response_class=HTMLResponse)
async def root():
    return RedirectResponse('/tasks/')


@app.get('/logs/', response_class=HTMLResponse)
async def logs(request: Request):
    logs = await util.dirstat(static_folder / 'logs')
    for log in logs:
        log['log_id'] = Path(log['name']).stem
    return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})


@app.get('/logs/{log_name}.txt', response_class=FileResponse)
async def log(log_name: str, request: Request):
    return FileResponse(static_folder / 'logs' / f'{log_name}.txt')


@app.get('/tasks/', response_class=HTMLResponse)
async def tasks(request: Request):
    tasks = await state.get_tasks_info()
    for task in tasks.values():
        print(task)
    tasks = sorted(tasks.values(), key=lambda t: t['created_at'], reverse=True)
    return templates.TemplateResponse('tasks.j2', {'request': request, 'tasks': tasks})


@app.get('/tasks/{task_id}', response_class=HTMLResponse)
async def task(request: Request, task_id: str):
    task = await state.get_task_info(task_id)
    return templates.TemplateResponse('task.j2', {'request': request, 'task': task})


@app.get('/dags/', response_class=HTMLResponse)
async def dags(request: Request):
    dags = []
    return templates.TemplateResponse('dags.j2', {'request': request, 'dags': dags})


@app.get('/dags/{dag_id}', response_class=HTMLResponse)
async def dag(request: Request):
    dag = {}
    return templates.TemplateResponse('dag.j2', {'request': request, 'dag': dag})
