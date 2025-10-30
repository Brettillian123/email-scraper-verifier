# scripts/run_worker.py
from src.queue import make_worker

if __name__ == "__main__":
    w, q = make_worker()
    print(f"*** Listening on {q.name}...")
    w.work(with_scheduler=True)
