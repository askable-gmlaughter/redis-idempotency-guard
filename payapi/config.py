import os

DB_PATH = os.environ.get("DB_PATH", "/app/data/payments.db")
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8080"))
PROVIDER_CONTROL_DIR = os.environ.get("PROVIDER_CONTROL_DIR", "")

