import asyncio
import sys

from dagops.constant import NOT_EXISTS_RETURNCODE
from dagops.dependencies import get_redis_cm


async def main():
    with get_redis_cm() as redis:
        if await redis.exists(sys.argv[1]):
            print(f'redis key exists: {sys.argv[1]!r}')
            raise SystemExit(0)
        else:
            print(f'redis key not exists: {sys.argv[1]!r}')
            raise SystemExit(NOT_EXISTS_RETURNCODE)

if __name__ == '__main__':
    asyncio.run(main())
