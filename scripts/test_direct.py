import json
import os

from src.queueing.redis_conn import get_redis
from src.queueing.tasks import TemporarySMTPError, verify_email_task


def run(mode, email):
    os.environ["TEST_PROBE"] = mode
    print(f"\n=== MODE={mode} EMAIL={email} ===")
    try:
        res = verify_email_task(email=email)
        print("RESULT:", res)
    except TemporarySMTPError as e:
        print("RAISED TemporarySMTPError:", e)
    except Exception as e:
        print("RAISED Exception:", type(e).__name__, str(e))


if __name__ == "__main__":
    run("success", "ok+1@example.com")
    run("perm", "bad+1@example.com")
    run("temp", "slow+1@example.com")
    run("crash", "boom+1@example.com")

    # Peek at the DLQ
    r = get_redis()
    llen = r.llen("dlq:verify")
    print(f"\nDLQ size: {llen}")
    if llen:
        print("DLQ head:", json.loads(r.lindex("dlq:verify", 0)))
