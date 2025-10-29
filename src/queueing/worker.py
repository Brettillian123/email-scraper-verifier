import logging

from rq import Connection, Worker

from src.queueing.redis_conn import get_redis

logging.basicConfig(level=logging.INFO)


def main():
    with Connection(get_redis()):
        Worker(["verify"]).work(with_scheduler=True)


if __name__ == "__main__":
    main()
