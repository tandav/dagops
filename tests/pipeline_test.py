import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest
from redis import Redis

import examples.main
from dagops.state import database

WATCH_DIRECTORY = 'tests/watch_dirs/serial'
WATCH_DIRECTORY_BATCH = 'tests/watch_dirs/batch'


def n_files(
    directory: str,
    exclude: frozenset[str] = frozenset({'.DS_Store'}),
) -> int:
    return sum(1 for p in Path(directory).iterdir() if p.name not in exclude)


@pytest.fixture
def db():
    database.drop_all()
    database.create_all()
    db = database.SessionLocal()
    yield db
    db.close()
    database.drop_all()


def drop_redis_keys(redis: Redis, prefix: str):
    pipeline = redis.pipeline()
    for key in redis.keys(prefix + '*'):
        pipeline.delete(key)
    pipeline.execute()


@pytest.fixture
def init_redis():
    redis = Redis.from_url(os.environ['REDIS_URL'], decode_responses=True)
    drop_redis_keys(redis, 'watch_dirs:')
    for p in Path(WATCH_DIRECTORY).iterdir():
        redis.set(f'watch_dirs:serial:{p.name}', '1')
    for p in Path(WATCH_DIRECTORY_BATCH).iterdir():
        redis.set(f'watch_dirs:batch:{p.name}', '1')
    yield redis
    redis.close()
    drop_redis_keys(redis, 'watch_dirs:')


def test_watch_filesystem(db):
    serial_graph = examples.main.create_dag('dummy_file')
    batch_graph = examples.main.create_batch_dag([])
    MAX_N_SUCCESS = n_files(WATCH_DIRECTORY) * (len(serial_graph) + 1) + len(batch_graph) + 1

    with mock.patch.dict(
        os.environ, {
            'N_ITERATIONS': '1',
            'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
            'WATCH_DIRECTORY': WATCH_DIRECTORY,
            'WATCH_DIRECTORY_BATCH': WATCH_DIRECTORY_BATCH,
            'WATCH_TYPE': 'filesystem',
        },
    ):
        subprocess.check_call([sys.executable, 'examples/main.py'])


def test_watch_redis(db, init_redis):
    serial_graph = examples.main.create_dag('dummy_file')
    batch_graph = examples.main.create_batch_dag([])
    MAX_N_SUCCESS = n_files(WATCH_DIRECTORY) * (len(serial_graph) + 1) + len(batch_graph) + 1

    with mock.patch.dict(
        os.environ, {
            'N_ITERATIONS': '1',
            'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
            'WATCH_DIRECTORY': 'watch_dirs:serial:',
            'WATCH_DIRECTORY_BATCH': 'watch_dirs:batch:',
            'WATCH_TYPE': 'redis',
        },
    ):
        subprocess.check_call([sys.executable, 'examples/main.py'])
