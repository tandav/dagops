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


def test_pipeline(db):
    pass
