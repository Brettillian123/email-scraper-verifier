from src.config import load_settings

cfg = load_settings()
print("Queue:", cfg.queue)
print("Rate:", cfg.rate)
print("Retry/Timeout:", cfg.retry_timeout)
print("SMTP:", cfg.smtp_identity)
