import asyncio
import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import aiofiles.os
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from dagops import constant
from dagops import fsm
from dagops.dag import Dag
from dagops.state import models
from dagops.state import schemas
from dagops.state.crud.dag import dag_crud
from dagops.state.crud.file import file_crud
from dagops.state.status import TaskStatus
from dagops.state.status import WorkerTaskStatus


class Daemon:
    def __init__(
        self,
        watch_directory: str,
        db: Session,
        redis: Redis,
        create_dag_func: Callable[[str | list[str]], Dag],
        batch: bool = False,
        storage: Literal['filesystem', 'redis'] = 'filesystem',
    ) -> None:
        if storage not in {'filesystem', 'redis'}:
            raise ValueError(f'unsupported storage={storage} It must be filesystem or redis')
        self.id = uuid.uuid4()
        self.watch_directory = Path(watch_directory)
        self.db = db
        self.redis = redis
        self.create_dag_func = create_dag_func
        self.batch = batch
        self.storage = storage
        self.files_channel = f'{constant.CHANNEL_FILES}:{self.watch_directory}'
        self.fsm_tasks = {}
        self._test_run = 'TEST_RUN' in os.environ

    @property
    def _test_all_tasks_done(self) -> bool:
        return (
            self._test_run and
            self.fsm_tasks and
            all(fsm_task.state == TaskStatus.SUCCESS for fsm_task in self.fsm_tasks.values())
        )

    @property
    def fsm_tasks_bits_string(self) -> str:
        # bits = ''.join(str(int(fsm_task.state == TaskStatus.SUCCESS)) for fsm_task in self.fsm_tasks.values())
        bits = [str(fsm_task.state) for fsm_task in self.fsm_tasks.values()]
        return f'{len(self.fsm_tasks)} {bits}'

    async def handle_worker_messages(self): # noqa: PLR0912
        while True:
            print(f'daemon.{self.id}.handle_worker_messages {self.fsm_tasks_bits_string}')
            if self._test_all_tasks_done:
                print('daemon.handle_worker_messages test_all_tasks_done')
                await self.redis.set(f'{constant.DAEMONS_DONE_STATUS_KEY}:{self.id}', '1')
                return

            kv = await self.redis.brpop(f'{constant.QUEUE_TASK_STATUS}:{self.id}', timeout=constant.SLEEP_TIME)
            if kv is not None:
                _, message = kv
                print(self.watch_directory, 'handle_tasks', message)
                message = schemas.WorkerTaskStatusMessage.parse_raw(message)
                if message.input_data.is_cache_check:
                    task_id = uuid.UUID(message.input_data.original_task_id)
                    fsm_task = self.fsm_tasks[task_id]
                    if message.status == WorkerTaskStatus.RUNNING:
                        await fsm_task.run_cache_check()
                    elif message.status == WorkerTaskStatus.SUCCESS:
                        await fsm_task.cache_exists(output_data=message.output_data)
                    elif message.status == WorkerTaskStatus.FAILED:
                        if message.output_data['returncode'] == constant.CACHE_NOT_EXISTS_RETURNCODE:
                            await fsm_task.cache_not_exists(output_data=message.output_data)
                        else:
                            await fsm_task.cache_check_failed(output_data=message.output_data)
                else:
                    task_id = uuid.UUID(message.id)
                    fsm_task = self.fsm_tasks[task_id]
                    if message.status == WorkerTaskStatus.RUNNING:
                        await fsm_task.run()

                    elif message.status == WorkerTaskStatus.SUCCESS:
                        await fsm_task.succeed(output_data=message.output_data)
                        if not self._test_run:
                            del self.fsm_tasks[task_id]
                    elif message.status == WorkerTaskStatus.FAILED:
                        await fsm_task.fail(output_data=message.output_data)
                        if not self._test_run:
                            del self.fsm_tasks[task_id]


                # task_id = uuid.UUID(message.id)
                # fsm_task = self.fsm_tasks[task_id]
                # if message.status == WorkerTaskStatus.RUNNING:
                #     if message.input_data.is_cache_check:
                #         await fsm_task.run_cache_check()
                #     else:
                #         await fsm_task.run()

                # if message.status in {WorkerTaskStatus.SUCCESS, WorkerTaskStatus.FAILED}:
                #     if message.status == WorkerTaskStatus.SUCCESS:
                #         if message.input_data.is_cache_check:
                #             await fsm_task.cache_exists(output_data=message.output_data)
                #         else:
                #             await fsm_task.succeed(output_data=message.output_data)
                #     if message.status == WorkerTaskStatus.FAILED:
                #         if message.input_data.is_cache_check:
                #             if message.output_data['returncode'] == constant.CACHE_NOT_EXISTS_RETURNCODE:
                #                 await fsm_task.cache_not_exists(output_data=message.output_data)
                #             else:
                #                 await fsm_task.cache_check_failed(output_data=message.output_data)
                #         else:
                #             await fsm_task.fail(output_data=message.output_data)
                #     del self.fsm_tasks[task_id]

                # if message.status == WorkerTaskStatus.SUCCESS:
                #     await fsm_task.succeed(output_data=message.output_data)
                #     del self.fsm_tasks[task_id]
                # if message.status == WorkerTaskStatus.FAILED:
                #     await fsm_task.fail(output_data=message.output_data)
                #     del self.fsm_tasks[task_id]

    async def handle_tasks(self):
        while True:
            print(f'daemon.{self.id}.handle_tasks {self.fsm_tasks_bits_string}')
            if self._test_all_tasks_done:
                print('handle_tasks', 'all tasks done, exiting')
                await self.redis.set(f'{constant.DAEMONS_DONE_STATUS_KEY}:{self.id}', '1')
                return
            for task in (
                self
                .db
                .query(models.Task)
                .filter(models.Task.daemon_id == self.id)
                # .filter(
                #     models.Task.status.not_in([
                #         TaskStatus.SUCCESS,
                #         TaskStatus.FAILED,
                #         TaskStatus.QUEUED_RUN,
                #         TaskStatus.RUNNING,
                #         TaskStatus.QUEUED_CACHE_CHECK,
                #         TaskStatus.CACHE_CHECK_RUNNING,
                #     ]),
                # )
                .filter(
                    models.Task.status.in_([
                        TaskStatus.PENDING,
                        TaskStatus.WAIT_CACHE_PATH_RELEASE,
                        TaskStatus.WAIT_UPSTREAM,
                        TaskStatus.FAILED,
                    ]),
                )
                .all()
            ):

                await self.redis.rpush(constant.TEST_LOGS_KEY, f'{task.id.hex[:5]} {task.type} {task.status} {task.input_data}')
                fsm_task = self.fsm_tasks[task.id]
                if task.type == 'dag':
                    if task.status == TaskStatus.PENDING:
                        await fsm_task.wait_upstream()
                    elif task.status == TaskStatus.WAIT_UPSTREAM:
                        await self.fsm_tasks[task.id].check_upstream(upstream=task.upstream) # try refresh db_obj, and not pass upstream
                    else:
                        raise ValueError(f'unsupported task status {task.status}')
                else:
                    if task.status == TaskStatus.PENDING:  # noqa: PLR5501
                        if task.input_data.get('exists_command') is not None:
                            # db_task = models.Task(
                            #     type='shell',
                            #     input_data={
                            #         'command': task.input_data['exists_command'],
                            #         'env': task.input_data['exists_env'],
                            #         'is_cache_check': True,
                            #         'original_task_id': str(task.id),
                            #     },
                            #     worker=read_worker(self.db, 'cpu'), # todo: fix this hardcode. cache should have own worker (possibly not equal to task.worker) (you dont need gpu to check redis key)
                            #     dag_id=task.dag_id,
                            #     daemon_id=self.id,
                            #     status=TaskStatus.QUEUED_CACHE_CHECK,
                            # )
                            # self.db.add(db_task)
                            # self.db.commit()
                            # self.db.refresh(db_task)

                            # await self.redis.lpush(
                            #     f'{constant.QUEUE_TASK}:{db_task.worker.name}',
                            #     schemas.TaskMessage(
                            #         id=str(db_task.id),
                            #         input_data=db_task.input_data,
                            #         daemon_id=str(self.id),
                            #     ).json(),
                            # )
                            input_data={
                                'command': task.input_data['exists_command'],
                                'env': task.input_data['exists_env'],
                                'is_cache_check': True,
                                'original_task_id': str(task.id),
                            }
                            await self.redis.lpush(
                                f'{constant.QUEUE_TASK}:cpu', # todo: fix this hardcode. cache should have own worker (possibly not equal to task.worker) (you dont need gpu to check redis key)
                                schemas.TaskMessage(
                                    id=str(uuid.uuid4()),
                                    input_data=input_data,
                                    daemon_id=str(self.id),
                                ).json(),
                            )
                            await fsm_task.queue_cache_check()
                        else:
                            await fsm_task.try_queue_cache_check()
                    elif task.status == TaskStatus.WAIT_UPSTREAM:
                        await self.fsm_tasks[task.id].check_upstream(upstream=task.upstream) # try refresh db_obj, and not pass upstream
                    else:
                        raise ValueError(f'unsupported task status {task.status}')
            await asyncio.sleep(constant.SLEEP_TIME)

    async def create_dag(self, file: str) -> models.Task:
        print('creating dag for file', file, '...')
        dag = self.create_dag_func(file)
        dag_head_task, tasks = dag_crud.create(
            self.db, schemas.DagCreate(
                type='shell',
                tasks_input_data=dag.input_data,
                graph=dag.id_graph,
                daemon_id=self.id,
            ),
        )
        for task in tasks:  # tasks include dag_head_task and all it's deps
            self.fsm_tasks[task.id] = fsm.Task(task, self.db, self.redis)
        print('dag for file', file, 'created')
        return dag_head_task

    async def _update_files_dags_step(self) -> None:
        _, message = await self.redis.brpop(self.files_channel)
        print(message)
        files = file_crud.read_by_field(self.db, 'directory', str(self.watch_directory))
        files = [file for file in files if file.dag_id is None]
        if not self.batch:
            for file in files:
                print(f'creating dag for file {file.directory}/{file.file}...')
                dag_head_task = await self.create_dag(file.file)
                file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag_head_task.id))
        elif files:
            print(f'batch dag for {len(files)} files creating...')
            dag_head_task = await self.create_dag([file.file for file in files])
            for file in files:
                file_crud.update_by_id(self.db, file.id, schemas.FileUpdate(dag_id=dag_head_task.id))

    async def update_files_dags(self) -> None:
        """create dags for new files"""
        while True:
            await self._update_files_dags_step()

    async def read_files(
        self,
        exclude: frozenset[str] = constant.default_files_exclude,
    ) -> set[str]:
        if self.storage == 'filesystem':
            files = set(await aiofiles.os.listdir(self.watch_directory))
        elif self.storage == 'redis':
            files = {k.removeprefix(str(self.watch_directory)) for k in await self.redis.keys(str(self.watch_directory) + '*')}
        else:
            raise ValueError(f'unsupported storage {self.storage}')
        return files - exclude

    async def _do_watch_directory_step(self) -> None:
        await self.redis.rpush(constant.TEST_LOGS_KEY, f'do_watch_directory {self.id}')
        files = await self.read_files()
        stale_files_ids = set()
        up_to_date_files_paths = set()
        for file in file_crud.read_by_field(self.db, 'directory', str(self.watch_directory)):
            if file.file in files:
                up_to_date_files_paths.add(file.file)
            else:
                stale_files_ids.add(file.id)

        await self.redis.rpush(constant.TEST_LOGS_KEY, f'do_watch_directory {files=} {stale_files_ids=} {up_to_date_files_paths=}')

        if stale_files_ids:
            print(f'deleting {len(stale_files_ids)} stale files...')
            file_crud.delete_by_field_isin(self.db, 'id', stale_files_ids)

        await self.redis.rpush(constant.TEST_LOGS_KEY, f'{self.id} do_watch_directory {files=} {up_to_date_files_paths=}')

        new_files = files - up_to_date_files_paths
        if not new_files:
            print('no new files')
            return
        print(f'creating {len(new_files)} new files...')
        file_crud.create_many(
            self.db, [
                schemas.FileCreate(
                    storage=self.storage,
                    directory=str(self.watch_directory),
                    file=file,
                )
                for file in new_files
            ],
        )
        await self.redis.lpush(self.files_channel, str(len(new_files)))

    async def do_watch_directory(self) -> None:
        await self.redis.rpush(constant.TEST_LOGS_KEY, f'do_watch_directory started {self.id}')

        # it = itertools.count()
        # if self._test_run:
        #     it = itertools.islice(it, 1)
        # for _ in it:
        while True:
            await self._do_watch_directory_step()
            await asyncio.sleep(constant.SLEEP_TIME)


    # async def cancel_orphans(self):
    #     await util.delete_keys(self.redis, self.files_channel)
    #     await util.delete_keys(self.redis, f'{constant.QUEUE_TASK}:*')
    #     await util.delete_keys(self.redis, f'{constant.QUEUE_TASK_STATUS}:*')
    #     await util.delete_keys(self.redis, f'{constant.CHANNEL_AIO_TASKS}:*')
    #     await util.delete_keys(self.redis, f'{constant.DAEMONS_DONE_STATUS_KEY}:*')
    #     # await self.redis.set(constant.DAEMONS_STARTED, '1')
    #     orphans = self.db.query(models.Task).filter(models.Task.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING])).all()
    #     if not orphans:
    #         return
    #     print(f'canceling {len(orphans)} orphans tasks...')
    #     for task in orphans:
    #         now = datetime.datetime.utcnow()
    #         task.status = TaskStatus.CANCELED
    #         if task.started_at is not None:
    #             task.stopped_at = now
    #         task.running_worker_id = None
    #     self.db.commit()
    #     print(f'canceling {len(orphans)} orphans tasks... done')

    # async def _check_tasks_success(self):
    #     pass

    # async def check_max_n_success(self):
    #     await self.redis.rpush(constant.TEST_LOGS_KEY, f'check_max_n_success started {self.id}')

    #     while True:
    #         n_success = task_crud.n_success(self.db)
    #         await self.redis.rpush(constant.TEST_LOGS_KEY, f'check_max_n_success {n_success=} / {self.max_n_success=} {self.id}')

    #         print(f'{n_success=} / {self.max_n_success=}')
    #         if n_success == self.max_n_success:
    #             print('MAX_N_SUCCESS reached, exiting')
    #             for task in asyncio.all_tasks():
    #                 task.cancel()
    #             raise SystemExit
    #         elif n_success > self.max_n_success:
    #             raise RuntimeError(f'n_success={n_success} > max_n_success={self.max_n_success}')
    #         await asyncio.sleep(constant.SLEEP_TIME)

    async def __call__(self):
        # await self.cancel_orphans()
        await self.redis.set(f'{constant.DAEMONS_DONE_STATUS_KEY}:{self.id}', '0')
        constant.DAEMONS_STARTED += 1

        aws = [
            self.handle_worker_messages(),
            self.handle_tasks(),
        ]

        if self._test_run:
            await self._do_watch_directory_step()
            await self._update_files_dags_step()
            # aws.append(self._check_tasks_success())
        else:
            aws.append(self.do_watch_directory())
            aws.append(self.update_files_dags())
        # if self.max_n_success is not None:
            # aws.append(self.check_max_n_success())

        await asyncio.gather(*aws)
