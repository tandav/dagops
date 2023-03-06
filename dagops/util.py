import datetime
import operator
from functools import partial
from pathlib import Path

import aiofiles.os
import humanize
from redis import Redis

from dagops import constant


def dirset(
    path: str | Path,
    glob: str = '*',
    stem: bool = False,
    absolute: bool = False,
) -> set[str | Path]:
    it = Path(path).glob(glob)
    if stem and absolute:
        raise ValueError('stem and absolute are mutually exclusive')
    if stem:
        return {p.stem for p in it}
    if absolute:
        return {str(p.absolute()) for p in it}
    return set(it)


async def path_stat(path: str) -> dict[str, float]:
    p = await aiofiles.os.stat(path)
    return {
        'created': datetime.datetime.fromtimestamp(p.st_mtime),
        'size': humanize.naturalsize(p.st_size),
    }


async def dirstat(
    path: str,
    sort_by: str | None = None,
    reverse: bool = False,
) -> list[dict]:
    out = [
        {
            'name': p.name,
            'created_at': datetime.datetime.fromtimestamp(p.stat().st_mtime, tz=datetime.timezone.utc),
            'size': humanize.naturalsize(p.stat().st_size),
        }
        for p in await aiofiles.os.scandir(path)
    ]
    if sort_by is not None:
        if sort_by not in {'name', 'created_at', 'size'}:
            raise ValueError(f'Invalid sort_by: {sort_by}')
        out.sort(key=operator.itemgetter(sort_by), reverse=reverse)

    return out


def to_local_time(dt: datetime.datetime, timezone: datetime.timezone):
    if dt.tzinfo is not None:
        raise ValueError('pass timezone as separate argument')
    dt = dt.replace(tzinfo=timezone)
    return datetime.datetime.fromtimestamp(dt.timestamp())


def format_time(
    t: datetime.datetime,
    absolute: bool = False,
    pad: bool = False,
    timezone: datetime.timezone | None = None,
) -> str:
    if timezone is not None:
        t = to_local_time(t, timezone)
    if absolute or (datetime.datetime.utcnow() - t).days > 30:
        return t.strftime('%Y %b %d %H:%M')
    t = datetime.datetime.fromtimestamp(t.timestamp())
    out = humanize.naturaltime(t)
    if pad:
        out = out.rjust(17)
    return out


format_time_utc = partial(format_time, timezone=datetime.timezone.utc)


def n_files(
    directory: str,
    exclude: frozenset[str] = constant.default_files_exclude,
) -> int:
    return sum(1 for p in Path(directory).iterdir() if p.name not in exclude)


def drop_redis_keys(redis: Redis, prefix: str):
    pipeline = redis.pipeline()
    for key in redis.keys(prefix + '*'):
        pipeline.delete(key)
    pipeline.execute()


async def delete_keys(redis, prefix: str):
    pipeline = redis.pipeline()
    for key in await redis.keys(prefix + '*'):
        pipeline.delete(key)
    await pipeline.execute()


def delete_keys_sync(redis, prefix: str):
    pipeline = redis.pipeline()
    for key in redis.keys(prefix + '*'):
        pipeline.delete(key)
    pipeline.execute()
