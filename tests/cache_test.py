import os
import subprocess
import sys
from unittest import mock

import pytest

import examples.counter
from dagops.util import n_files


@pytest.mark.skip
@pytest.mark.asyncio
async def test_cache(db, redis, WATCH_DIRECTORY):
    counter_key = 'counter'
    redis.delete(counter_key)

    with mock.patch.dict(
        os.environ, {
            'COUNTER_KEY': counter_key,
        },
    ):
        graph = examples.counter.create_dag('dummy_file')

    MAX_N_SUCCESS = n_files(WATCH_DIRECTORY) * (len(graph) + 1)
    with mock.patch.dict(
        os.environ, {
            'N_ITERATIONS': '1',
            'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
            'WATCH_DIRECTORY': WATCH_DIRECTORY,
            'STORAGE': 'filesystem',
            'COUNTER_KEY': counter_key,
        },
    ):
        subprocess.check_call([sys.executable, 'examples/counter.py'])

    assert redis.get('counter') == '1'


# def test_lock():
#     pass
