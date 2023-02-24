import sys
from pathlib import Path

if __name__ == '__main__':
    if Path(sys.argv[1]).exists():
        raise SystemExit(0)
    else:
        raise SystemExit(255)
