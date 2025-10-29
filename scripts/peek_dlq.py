import json
import os

from redis import Redis


def main():
    redis_url = os.getenv("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
    r = Redis.from_url(redis_url)
    for i, raw in enumerate(r.lrange("dlq:verify", 0, 9)):
        print(f"#{i}:")
        print(json.dumps(json.loads(raw), indent=2))


if __name__ == "__main__":
    main()
