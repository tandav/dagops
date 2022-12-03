import json
from pathlib import Path

import aiofiles.os
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from dagops import util
from dagops.state import State
import dotenv

dotenv.load_dotenv()

static_folder = Path('static')
app = FastAPI()
state = State()


app.mount('/static/', StaticFiles(directory=static_folder), name='static')
templates = Jinja2Templates(directory=static_folder / 'templates')
# templates.env.filters['format_time'] = util.format_time


@app.get('/', response_class=HTMLResponse)
async def root():
    return RedirectResponse('/tasks/')


@app.get('/logs/', response_class=HTMLResponse)
async def logs(request: Request):
    logs = await util.dirstat(static_folder / 'logs')
    for log in logs:
        log['log_id'] = Path(log['name']).stem
    print(logs)
    return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})


@app.get('/logs/{log_name}.txt', response_class=FileResponse)
async def logs(log_name: str, request: Request):
    return FileResponse(static_folder / 'logs' / f'{log_name}.txt')

    # logs = await util.dirstat(static_folder / 'logs')
    # for log in logs:
    #     log['name'] = Path(log['name']).stem
    # print(logs)
    # return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})

@app.get('/tasks/', response_class=HTMLResponse)
async def tasks(request: Request):
    tasks = await state.get_tasks_info()
    print(tasks)
    # tasks_keys = await state.redis.keys(f'tasks:*')
    # # tasks_ids = [k.decode().split(':')[1] for k in tasks_ids]
    # pipe = state.redis.pipeline()
    # for key in tasks_keys:
    #     pipe.hgetall(key)
    # tasks = await pipe.execute()
    # for task_key, task in zip(tasks_keys, tasks):
    #     task['id'] = task_key.decode().split(':')[1]
    #     task['status'] = task[b'status'].decode()
    #     task['cmd'] = task[b'cmd'].decode()
    #     task['env'] = task[b'env'].decode()

    # # tasks = await util.dirstat(static_folder / 'tasks')
    # # for task in tasks:
    #     # task['task_id'] = Path(task['name']).stem
    # print(tasks)
    return templates.TemplateResponse('tasks.j2', {'request': request, 'tasks': tasks})

@app.get('/tasks/{task_id}', response_class=HTMLResponse)
async def task(request: Request, task_id: str):
    task = await state.get_task_info(task_id)
    print(task)
    # task = await state.redis.hgetall(f'tasks:{task_id}')
    # task['id'] = task_id
    # task['status'] = task[b'status'].decode()
    # task['cmd'] = task[b'cmd'].decode()
    # task['env'] = task[b'env'].decode()
    return templates.TemplateResponse('task.j2', {'request': request, 'task': task})
