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


static_folder = Path('static')
app = FastAPI()


app.mount('/static/', StaticFiles(directory=static_folder), name='static')
templates = Jinja2Templates(directory=static_folder / 'templates')
# templates.env.filters['format_time'] = util.format_time


@app.get('/', response_class=HTMLResponse)
async def root():
    return RedirectResponse('/logs/')


@app.get('/logs/', response_class=HTMLResponse)
async def logs(request: Request):
    logs = await util.dirstat(static_folder / 'logs')
    for log in logs:
        log['log_id'] = Path(log['name']).stem
    print(logs)
    return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})


@app.get('/logs/{log_name}', response_class=FileResponse)
async def logs(log_name: str, request: Request):
    return FileResponse(static_folder / 'logs' / f'{log_name}')

    # logs = await util.dirstat(static_folder / 'logs')
    # for log in logs:
    #     log['name'] = Path(log['name']).stem
    # print(logs)
    # return templates.TemplateResponse('logs.j2', {'request': request, 'logs': logs})
