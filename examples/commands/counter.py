import os
import sys

from redis import Redis


def main():
    redis = Redis.from_url(os.environ['REDIS_URL'], decode_responses=True)
    value = redis.incr(sys.argv[1])
    print('increment success, value:', value)


if __name__ == '__main__':
    main()
