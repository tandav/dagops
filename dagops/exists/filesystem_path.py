import sys
from pathlib import Path

from dagops.constant import CACHE_NOT_EXISTS_RETURNCODE


def main():
    if Path(sys.argv[1]).exists():
        print(f'filesystem path exists: {sys.argv[1]!r}')
        raise SystemExit(0)
    else:
        print(f'filesystem path not exists: {sys.argv[1]!r}')
        raise SystemExit(CACHE_NOT_EXISTS_RETURNCODE)


if __name__ == '__main__':
    main()
