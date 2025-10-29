from __future__ import annotations

from src.jobs import smoke_job
from src.queue import get_queue

if __name__ == "__main__":
    q = get_queue()
    job = q.enqueue(smoke_job, 2, 3)  # 2 + 3 = 5
    print("Enqueued job:", job.id)
