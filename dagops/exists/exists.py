import sys
from pathlib import Path


def filesystem_path(path):
    return [sys.executable, str(Path(__file__).parent / 'filesystem_path.py'), str(path)]


def redis_key(key):
    return [sys.executable, str(Path(__file__).parent / 'redis_key.py'), key]


def check(
    path: str,
    redis_scheme='redis://',
    file_scheme='file://',
):
    if path.startswith(redis_scheme):
        return redis_key(path.removeprefix(redis_scheme))
    elif path.startswith(file_scheme):
        return filesystem_path(path.removeprefix(file_scheme))
    else:
        raise ValueError(f'Unknown path: {path!r}')
