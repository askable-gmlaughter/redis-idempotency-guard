import os
import sqlite3
import uuid
from contextlib import closing

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    id TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT NOT NULL,
    account TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


def _connect():
    return sqlite3.connect(config.DB_PATH, timeout=30)


def init():
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    with closing(_connect()) as conn, conn:
        conn.execute(_SCHEMA)


def insert_payment(merchant_id, amount, currency, account):
    payment_id = "pay_" + uuid.uuid4().hex[:16]
    with closing(_connect()) as conn, conn:
        conn.execute(
            "INSERT INTO payments (id, merchant_id, amount, currency, account) VALUES (?, ?, ?, ?, ?)",
            (payment_id, merchant_id, amount, currency, account),
        )
    return payment_id
