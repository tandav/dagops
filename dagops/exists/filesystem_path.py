import sys
from pathlib import Path

from dagops.constant import NOT_EXISTS_RETURNCODE

if __name__ == '__main__':
    if Path(sys.argv[1]).exists():
        raise SystemExit(0)
    else:
        raise SystemExit(NOT_EXISTS_RETURNCODE)
