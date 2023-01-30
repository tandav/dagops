import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dagops.state import models

# engine = create_engine(os.environ['DB_URL'], connect_args={'check_same_thread': False}, echo=False)
engine = create_engine(os.environ['DB_URL'])
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_all():
    models.Base.metadata.create_all(bind=engine)


def drop_all():
    models.Base.metadata.drop_all(bind=engine)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('Usage: python main.py <command>')
    elif sys.argv[1] == 'create':
        create_all()
    elif sys.argv[1] == 'drop':
        drop_all()
