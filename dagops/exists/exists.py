import sys
from pathlib import Path

# import click


# class Exists:
#     pass


# class FileExists(Exists):
#     def __init__(self, path: Path):
#         self.path = path

#     def __call__(self):
#         return self.path.exists()


# class RedisKeyExists(Exists):
#     pass


# @click.command()
# def main():
#     pass


# if __name__ == '__main__':
#     main()

def filesystem_path(path):
    return [sys.executable, str(Path(__file__).parent / 'filesystem_path.py'), str(path)]


def redis_key(key):
    return [sys.executable, str(Path(__file__).parent / 'redis.py'), key]
