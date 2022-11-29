import pytest
from redis import Redis
import dotenv
import os

dotenv.load_dotenv()


@pytest.fixture
def redis():
    return Redis(
        host=os.environ['REDIS_HOST'],
        port=os.environ['REDIS_PORT'],
        password=os.environ['REDIS_PASSWORD'],
    )


def test_connection(redis):
    assert redis.ping()
