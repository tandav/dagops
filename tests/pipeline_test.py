import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

import examples.main
from dagops.state import database
from dagops.worker import prepare_workers


@pytest.fixture
def db():
    database.drop_all()
    database.create_all()
    db = database.SessionLocal()
    prepare_workers(db)
    yield db
    db.close()
    database.drop_all()


def n_files(
    directory: str,
    exclude: frozenset[str] = frozenset({'.DS_Store'}),
) -> int:
    return sum(1 for p in Path(directory).iterdir() if p.name not in exclude)


def test_pipeline(db, tmpdir):
    WATCH_DIRECTORY = 'tests/watch_dirs/serial'
    WATCH_DIRECTORY_BATCH = 'tests/watch_dirs/batch'
    serial_graph = examples.main.create_dag('dummy_file')
    batch_graph = examples.main.create_batch_dag([])

    MAX_N_SUCCESS = n_files(WATCH_DIRECTORY) * (len(serial_graph) + 1) + n_files(WATCH_DIRECTORY_BATCH) * (len(batch_graph) + 1)

    with mock.patch.dict(
        os.environ, {
            'N_ITERATIONS': '1',
            'LOGS_DIRECTORY': str(tmpdir),
            'MAX_N_SUCCESS': str(MAX_N_SUCCESS),
            'WATCH_DIRECTORY': WATCH_DIRECTORY,
            'WATCH_DIRECTORY_BATCH': WATCH_DIRECTORY_BATCH,
        },
    ):
        subprocess.check_call([sys.executable, 'examples/main.py'])