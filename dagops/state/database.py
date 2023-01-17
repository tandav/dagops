import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dagops.state import models

engine = create_engine(os.environ['DB_URL'], connect_args={'check_same_thread': False}, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_all():
    models.Base.metadata.create_all(bind=engine)


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'create':
        create_all()
