import asyncio
import sys

from dagops import constant
from dagops.dependencies import get_redis_cm


async def main():
    if len(sys.argv) != 2:  # noqa: PLR2004
        raise SystemExit('pass redis key as first argument')
    key = sys.argv[1]
    with get_redis_cm() as redis:
        is_exists = await redis.exists(key)
        await redis.rpush(constant.TEST_LOGS_KEY, f'exists.redis_key {key} {is_exists}')
        # raise SystemExit(constant.CACHE_NOT_EXISTS_RETURNCODE)
        if await redis.exists(key):
            print(f'redis key exists: {key!r}')
            raise SystemExit(0)
        else:
            print(f'redis key not exists: {key!r}')
            raise SystemExit(constant.CACHE_NOT_EXISTS_RETURNCODE)

if __name__ == '__main__':
    asyncio.run(main())
