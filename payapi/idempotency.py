"""Idempotency guard backed by Redis.

A request first calls begin(). If it gets ("new", claim) it owns the key,
processes the payment, and then calls finish() with its response, or
release() if the key should be usable again. finish() and release() only
act while the request still owns the claim.
"""
import hashlib
import json
import uuid

import redis

from . import config

_client = None

# Replace the claim with the finished response, only if we still own it.
_FINISH = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
  return 1
end
return 0
"""

# Delete the claim, only if we still own it.
_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def client():
    global _client
    if _client is None:
        _client = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    return _client


def fingerprint(body):
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def redis_key(merchant_id, idem_key):
    # Length prefix keeps (merchant, key) pairs unambiguous.
    return f"idem:{len(merchant_id)}:{merchant_id}:{idem_key}"


class Claim:
    def __init__(self, key, fp, value):
        self.key = key
        self.fp = fp
        self.value = value


def begin(merchant_id, idem_key, body):
    """Return ("new", Claim) or ("done", status, body)."""
    r = client()
    key = redis_key(merchant_id, idem_key)
    fp = fingerprint(body)
    value = json.dumps({"state": "in_progress", "fp": fp, "token": uuid.uuid4().hex})

    while True:
        if r.set(key, value, nx=True, px=config.CLAIM_TTL_MS):
            return ("new", Claim(key, fp, value))
        existing = r.get(key)
        if existing is None:
            continue  # released or expired between SET and GET; try again
        record = json.loads(existing)
        if record["fp"] != fp:
            return ("done", 422, {"error": "key_reused"})
        if record["state"] == "in_progress":
            return ("done", 409, {"error": "in_progress"})
        return ("done", record["status"], record["body"])


def finish(claim, status, body):
    record = json.dumps({"state": "done", "fp": claim.fp, "status": status, "body": body})
    client().eval(_FINISH, 1, claim.key, claim.value, record, config.RESULT_TTL_S)


def release(claim):
    client().eval(_RELEASE, 1, claim.key, claim.value)
