import sys

from dagops.dependencies import get_redis_cm

if __name__ == '__main__':
    with get_redis_cm() as redis:
        if redis.exists(sys.argv[1]):
            raise SystemExit(0)
        else:
            raise SystemExit(255)
