import os
from pathlib import Path

import pytest
from redis import Redis

from dagops.state import database
from dagops.util import drop_redis_keys


@pytest.fixture
def WATCH_DIRECTORY():
    return 'tests/watch_dirs/serial'


@pytest.fixture
def WATCH_DIRECTORY_BATCH():
    return 'tests/watch_dirs/batch'


@pytest.fixture
def db():
    database.drop_all()
    database.create_all()
    db = database.SessionLocal()
    yield db
    db.close()
    database.drop_all()


@pytest.fixture
def redis(WATCH_DIRECTORY, WATCH_DIRECTORY_BATCH):
    redis = Redis.from_url(os.environ['REDIS_URL'], decode_responses=True)
    drop_redis_keys(redis, 'watch_dirs:')
    for p in Path(WATCH_DIRECTORY).iterdir():
        redis.set(f'watch_dirs:serial:{p.name}', '1')
    for p in Path(WATCH_DIRECTORY_BATCH).iterdir():
        redis.set(f'watch_dirs:batch:{p.name}', '1')
    yield redis
    redis.close()
    drop_redis_keys(redis, 'watch_dirs:')
