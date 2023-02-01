import os
import subprocess
import sys
from unittest import mock

import pytest

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


def test_pipeline(db, tmpdir):
    with mock.patch.dict(
        os.environ, {
            'N_ITERATIONS': '1',
            'MAX_N_ALL_DONE': '10',
            'LOGS_DIRECTORY': str(tmpdir),
        },
    ):
        subprocess.check_call([sys.executable, 'examples/main.py'])
