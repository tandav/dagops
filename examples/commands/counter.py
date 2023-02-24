import os
import sys

from redis import Redis


def main():
    redis = Redis.from_url(os.environ['REDIS_URL'], decode_responses=True)
    redis.incr(sys.argv[1])


if __name__ == '__main__':
    main()
