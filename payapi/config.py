import os

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
DB_PATH = os.environ.get("DB_PATH", "/app/data/payments.db")
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8080"))
PROVIDER_CONTROL_DIR = os.environ.get("PROVIDER_CONTROL_DIR", "")

# How long an in-progress claim on an idempotency key lives, and how long a
# finished response is kept for replay.
CLAIM_TTL_MS = 30_000
RESULT_TTL_S = 86_400
