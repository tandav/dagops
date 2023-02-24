import sys

from dagops.constant import NOT_EXISTS_RETURNCODE
from dagops.dependencies import get_redis_cm

if __name__ == '__main__':
    with get_redis_cm() as redis:
        if redis.exists(sys.argv[1]):
            raise SystemExit(0)
        else:
            raise SystemExit(NOT_EXISTS_RETURNCODE)
