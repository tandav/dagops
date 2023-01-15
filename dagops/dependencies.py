import contextlib

from dagops.state.database import SessionLocal


def get_db():
    db = SessionLocal()
    yield db
    db.close()


# at the moment of writiong this app fastapi does not support DI in startup events
# this app has startup events which uses get_db - so following wrapper is used in events instead of get_db
get_db_cm = contextlib.contextmanager(get_db)
