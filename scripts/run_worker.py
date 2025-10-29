from __future__ import annotations

from src.queue import make_worker

if __name__ == "__main__":
    w, q = make_worker()
    print(f"Starting worker for queue '{q.name}'")
    w.work(with_scheduler=True)
