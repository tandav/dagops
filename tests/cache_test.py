import os
import subprocess
import sys
from unittest import mock

import pytest

import examples.counter
from dagops import constant
from dagops.util import delete_keys_sync


# @pytest.mark.skip
# @pytest.mark.asyncio
# async def test_cache(db, redis, WATCH_DIRECTORY):
#     counter_key = 'counter'
#     redis.delete(counter_key)

#     with mock.patch.dict(
#         os.environ, {
#             'COUNTER_KEY': counter_key,
#         },
#     ):
#         graph = examples.counter.create_dag('dummy_file')

#     MAX_N_SUCCESS = n_files(WATCH_DIRECTORY) * (len(graph) + 1)
#     with mock.patch.dict(
#         os.environ, {
#             'N_ITERATIONS': '1',
#             'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
#             'WATCH_DIRECTORY': WATCH_DIRECTORY,
#             'STORAGE': 'filesystem',
#             'COUNTER_KEY': counter_key,
#         },
#     ):
#         subprocess.check_call([sys.executable, 'examples/counter.py'])

#     assert redis.get('counter') == '1'

# @pytest.mark.asyncio
@pytest.mark.parametrize(
    'exists_command, n_iterations', [
        # (None, 1),
        (None, 2),
        # ('redis://counter', 1),
        # ('redis://counter', 2),
    ],
)
def test_cache(exists_command, n_iterations, db, redis):
    WATCH_DIRECTORY = 'some_key'
    # redis.delete(WATCH_DIRECTORY)
    delete_keys_sync(redis, WATCH_DIRECTORY)

    counter_key = 'counter'
    redis.delete(counter_key)
    redis.delete(constant.TEST_LOGS_KEY)


    with mock.patch.dict(
        os.environ, {
            'COUNTER_KEY': counter_key,
        },
    ):
        graph = examples.counter.create_dag('dummy_file')

    MAX_N_SUCCESS = len(graph) + 1
    for i in range(n_iterations):
        redis.set(f'{WATCH_DIRECTORY}:{i}', 'some_value')
        environ_patch = {
            'N_ITERATIONS': '1',
            'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
            'WATCH_DIRECTORY': WATCH_DIRECTORY,
            'STORAGE': 'redis',
            'COUNTER_KEY': counter_key,
        }
        if exists_command is not None:
            environ_patch['EXISTS_COMMAND'] = exists_command

        with mock.patch.dict(os.environ, environ_patch):
            subprocess.check_call([sys.executable, 'examples/counter.py'])

    expected = str(n_iterations) if exists_command is None else '1'
    assert redis.get(counter_key) == expected
    # todo: try test with broken exists command which always returns False (raise SystemExit(CACHE_NOT_EXISTS_RETURNCODE))


# def test_lock():
#     pass
