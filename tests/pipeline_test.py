import os
import subprocess
import sys
from unittest import mock

import examples.main
from dagops.util import n_files


def test_watch_filesystem(db, WATCH_DIRECTORY, WATCH_DIRECTORY_BATCH):
    serial_graph = examples.main.create_dag('dummy_file')
    batch_graph = examples.main.create_batch_dag([])
    MAX_N_SUCCESS = n_files(WATCH_DIRECTORY) * (len(serial_graph) + 1) + len(batch_graph) + 1

    with mock.patch.dict(
        os.environ, {
            'N_ITERATIONS': '1',
            'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
            'WATCH_DIRECTORY': WATCH_DIRECTORY,
            'WATCH_DIRECTORY_BATCH': WATCH_DIRECTORY_BATCH,
            'STORAGE': 'filesystem',
        },
    ):
        subprocess.check_call([sys.executable, 'examples/main.py'])


def test_watch_redis(db, redis, WATCH_DIRECTORY):
    serial_graph = examples.main.create_dag('dummy_file')
    batch_graph = examples.main.create_batch_dag([])
    MAX_N_SUCCESS = n_files(WATCH_DIRECTORY) * (len(serial_graph) + 1) + len(batch_graph) + 1

    with mock.patch.dict(
        os.environ, {
            'N_ITERATIONS': '1',
            'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
            'WATCH_DIRECTORY': 'watch_dirs:serial:',
            'WATCH_DIRECTORY_BATCH': 'watch_dirs:batch:',
            'STORAGE': 'redis',
        },
    ):
        subprocess.check_call([sys.executable, 'examples/main.py'])
