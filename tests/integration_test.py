#!/usr/bin/env python3
"""Integration tests for the idempotency guard.

Starts a throwaway redis-server, a reply-gating proxy, and two instances of
payapi sharing one Redis and one database, then drives HTTP scenarios.
Ordering between requests is forced with the proxy (for the race) and with
the sandbox provider's hold/fail files (for in-progress and stale-claim
cases), never with sleeps.

Usage (redis-server must be on PATH):
    python tests/integration_test.py
Exits 0 only if every scenario passes.
"""
import glob
import http.client
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from redis_proxy import Proxy  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK = tempfile.mkdtemp(prefix="payapi-it-")
CONTROL = f"{WORK}/provider"
DB_PATH = f"{WORK}/payments.db"
REDIS_PORT = 6390
PROXY_PORT = 6391
APP_PORTS = (8091, 8092)


# ---------------------------------------------------------------- redis ----
class RawRedis:
    """Tiny RESP client talking straight to Redis (not through the proxy)."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.f = self.sock.makefile("rb")

    def cmd(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)
        return self._read()

    def _read(self):
        line = self.f.readline()[:-2]
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            return None if n == -1 else self.f.read(n + 2)[:-2]
        if kind == b"*":
            n = int(rest)
            return None if n == -1 else [self._read() for _ in range(n)]
        raise RuntimeError(f"unexpected reply {line!r}")


def keys_for(r, idem_key):
    return r.cmd("KEYS", f"*{idem_key}*")


# ----------------------------------------------------------------- http ----
def post(path, body, merchant=None, key=None, port=APP_PORTS[0], timeout=60):
    headers = {"Content-Type": "application/json"}
    if merchant:
        headers["X-Merchant-Id"] = merchant
    if key:
        headers["Idempotency-Key"] = key
    raw = body if isinstance(body, str) else json.dumps(body)
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("POST", path, body=raw, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
    finally:
        conn.close()
    try:
        parsed = json.loads(data)
    except ValueError:
        parsed = None
    return resp.status, parsed


def pay(merchant, key, body, port=APP_PORTS[0]):
    return post("/payments", body, merchant=merchant, key=key, port=port)


class Bg(threading.Thread):
    def __init__(self, fn, *args, **kwargs):
        super().__init__(daemon=True)
        self.fn, self.args, self.kwargs = fn, args, kwargs
        self.result = self.error = None
        self.start()

    def run(self):
        try:
            self.result = self.fn(*self.args, **self.kwargs)
        except Exception as exc:  # surfaced in get()
            self.error = exc

    def get(self, timeout=60):
        self.join(timeout)
        if self.is_alive():
            raise AssertionError("request did not finish")
        if self.error:
            raise self.error
        return self.result


# ------------------------------------------------------ provider control ----
def held_files():
    return set(glob.glob(f"{CONTROL}/held.*"))


def hold_next():
    open(f"{CONTROL}/hold", "w").close()


def wait_new_held(before, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        new = held_files() - before
        if new:
            assert len(new) == 1, f"expected one held call, got {new}"
            return new.pop()
        time.sleep(0.01)
    raise AssertionError("request never reached the provider")


def set_fail(on):
    path = f"{CONTROL}/fail"
    if on:
        open(path, "w").close()
    elif os.path.exists(path):
        os.remove(path)


def payment_count(merchant):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        return conn.execute("SELECT COUNT(*) FROM payments WHERE merchant_id = ?", (merchant,)).fetchone()[0]
    finally:
        conn.close()


def fresh(name):
    return f"m-{name}-{uuid.uuid4().hex[:8]}", f"k-{uuid.uuid4().hex}"


def body(amount=1500, account="acct-1"):
    return {"amount": amount, "currency": "KES", "account": account}


# ------------------------------------------------------------ scenarios ----
def test_replay_same_status_and_body(r, proxy):
    m, k = fresh("replay")
    s1, b1 = pay(m, k, body())
    assert s1 == 201 and b1 and b1.get("payment_id"), f"first request: {s1} {b1}"
    s2, b2 = pay(m, k, body(), port=APP_PORTS[1])
    assert (s2, b2) == (s1, b1), f"replay differs: {s2} {b2} vs {s1} {b1}"
    # Same JSON object, different key order and spacing.
    s3, b3 = pay(m, k, '{"account": "acct-1",  "currency": "KES", "amount": 1500}')
    assert (s3, b3) == (s1, b1), f"reordered body replay differs: {s3} {b3}"
    assert payment_count(m) == 1, "replay created a payment"


def test_key_reused_after_finish(r, proxy):
    m, k = fresh("reused")
    s1, _ = pay(m, k, body())
    assert s1 == 201
    s2, b2 = pay(m, k, body(amount=1600))
    assert (s2, b2) == (422, {"error": "key_reused"}), f"got {s2} {b2}"
    assert payment_count(m) == 1


def test_validation_does_not_use_key(r, proxy):
    m, k = fresh("valid")
    s1, _ = pay(m, k, body(amount=-5))
    assert s1 == 400, f"invalid body got {s1}"
    s2, b2 = pay(m, k, body())
    assert s2 == 201, f"valid request after 400 got {s2} {b2}"


def test_concurrent_requests_create_one_payment(r, proxy):
    m, k = fresh("race")
    proxy.arm(k.encode())
    try:
        a = Bg(pay, m, k, body(), port=APP_PORTS[0])
        b = Bg(pay, m, k, body(), port=APP_PORTS[1])
        ra, rb = a.get(), b.get()
    finally:
        proxy.disarm()
    created = [x for x in (ra, rb) if x[0] == 201]
    assert created, f"no request succeeded: {ra} {rb}"
    first = created[0]
    for res in (ra, rb):
        assert res == first or res == (409, {"error": "in_progress"}), f"unexpected responses {ra} {rb}"
    assert payment_count(m) == 1, f"{payment_count(m)} payments for one key"
    assert pay(m, k, body()) == first, "replay after race differs"


def test_in_progress_conflicts_and_ttls(r, proxy):
    m, k = fresh("inprog")
    before = held_files()
    hold_next()
    a = Bg(pay, m, k, body())
    held = wait_new_held(before)
    try:
        assert pay(m, k, body(), port=APP_PORTS[1]) == (409, {"error": "in_progress"}), "same body while in progress"
        assert pay(m, k, body(amount=99)) == (422, {"error": "key_reused"}), "different body while in progress"
        ttls = [r.cmd("PTTL", key) for key in keys_for(r, k)]
        assert ttls and all(0 < t <= 30_000 for t in ttls), f"claim TTLs {ttls}"
    finally:
        os.remove(held)
    sa, ba = a.get()
    assert sa == 201, f"held request finished with {sa} {ba}"
    ttls = [r.cmd("PTTL", key) for key in keys_for(r, k)]
    assert ttls and 30_000 < max(ttls) <= 86_400_000, f"stored response TTLs {ttls}"
    assert payment_count(m) == 1


def test_failure_releases_key(r, proxy):
    m, k = fresh("fail")
    set_fail(True)
    try:
        s1, b1 = pay(m, k, body())
    finally:
        set_fail(False)
    assert (s1, b1) == (502, {"error": "provider_error"}), f"failing request got {s1} {b1}"
    s2, b2 = pay(m, k, body(), port=APP_PORTS[1])
    assert s2 == 201, f"retry after failure got {s2} {b2}"
    assert pay(m, k, body()) == (s2, b2)
    assert payment_count(m) == 1


def test_keys_are_scoped_by_merchant(r, proxy):
    m1, k = fresh("scope1")
    m2 = f"m-scope2-{uuid.uuid4().hex[:8]}"
    s1, b1 = pay(m1, k, body())
    s2, b2 = pay(m2, k, body())
    assert s1 == 201 and s2 == 201, f"got {s1} {b1} / {s2} {b2}"
    assert b1["payment_id"] != b2["payment_id"], "two merchants got the same payment"
    assert pay(m1, k, body()) == (s1, b1)
    assert payment_count(m1) == 1 and payment_count(m2) == 1


def test_stale_owner_cannot_overwrite(r, proxy):
    m, k = fresh("stale1")
    before = held_files()
    hold_next()
    a = Bg(pay, m, k, body())
    held_a = wait_new_held(before)
    for key in keys_for(r, k):  # the claim expires while A is still working
        r.cmd("DEL", key)
    sb, bb = pay(m, k, body(), port=APP_PORTS[1])
    assert sb == 201, f"request after expiry got {sb} {bb}"
    os.remove(held_a)
    a.get()
    assert pay(m, k, body()) == (sb, bb), "stale request overwrote the new owner's response"


def test_stale_owner_cannot_release(r, proxy):
    m, k = fresh("stale2")
    before = held_files()
    hold_next()
    a = Bg(pay, m, k, body())
    held_a = wait_new_held(before)
    for key in keys_for(r, k):
        r.cmd("DEL", key)
    before_b = held_files()
    hold_next()
    b = Bg(pay, m, k, body(), port=APP_PORTS[1])
    held_b = wait_new_held(before_b)
    set_fail(True)
    try:
        os.remove(held_a)
        a.get()
    finally:
        set_fail(False)
    try:
        sc = pay(m, k, body())
        assert sc == (409, {"error": "in_progress"}), f"claim was released by a stale request: {sc}"
    finally:
        os.remove(held_b)
    sb, bb = b.get()
    assert sb == 201, f"owner finished with {sb} {bb}"
    assert pay(m, k, body()) == (sb, bb)


def test_batch_uses_same_guard(r, proxy):
    m, k1 = fresh("batch")
    k2, k3 = f"k-{uuid.uuid4().hex}", f"k-{uuid.uuid4().hex}"
    items = [dict(body(account="acct-1"), idempotency_key=k1), dict(body(account="acct-2"), idempotency_key=k2)]
    s1, r1 = post("/payments/batch", {"items": items}, merchant=m)
    assert s1 == 200 and [x["status"] for x in r1["results"]] == [201, 201], f"first batch: {s1} {r1}"
    s2, r2 = post("/payments/batch", {"items": items}, merchant=m, port=APP_PORTS[1])
    assert (s2, r2) == (s1, r1), f"batch replay differs: {r2}"
    assert payment_count(m) == 2
    assert pay(m, k1, body(account="acct-1")) == (201, r1["results"][0]["body"]), "/payments did not see batch key"
    _, r3 = post("/payments/batch", {"items": [dict(body(amount=7), idempotency_key=k1)]}, merchant=m)
    assert r3["results"] == [{"status": 422, "body": {"error": "key_reused"}}], f"reused key in batch: {r3}"
    s4, b4 = pay(m, k3, body(account="acct-3"))
    assert s4 == 201
    _, r5 = post("/payments/batch", {"items": [dict(body(account="acct-3"), idempotency_key=k3)]}, merchant=m)
    assert r5["results"] == [{"status": 201, "body": b4}], f"batch did not see /payments key: {r5}"
    assert payment_count(m) == 3


SCENARIOS = [
    test_replay_same_status_and_body,
    test_key_reused_after_finish,
    test_validation_does_not_use_key,
    test_concurrent_requests_create_one_payment,
    test_in_progress_conflicts_and_ttls,
    test_failure_releases_key,
    test_keys_are_scoped_by_merchant,
    test_stale_owner_cannot_overwrite,
    test_stale_owner_cannot_release,
    test_batch_uses_same_guard,
]


# ---------------------------------------------------------------- setup ----
def wait_port(port, proc=None, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"process for port {port} exited with {proc.returncode}")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"nothing listening on {port}")


def main():
    os.makedirs(CONTROL)
    procs = []
    try:
        redis_proc = subprocess.Popen(
            ["redis-server", "--port", str(REDIS_PORT), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(redis_proc)
        wait_port(REDIS_PORT, redis_proc)
        r = RawRedis(REDIS_PORT)
        proxy = Proxy(PROXY_PORT, REDIS_PORT)

        for port in APP_PORTS:
            env = dict(
                os.environ,
                PYTHONDONTWRITEBYTECODE="1",
                REDIS_URL=f"redis://127.0.0.1:{PROXY_PORT}/0",
                DB_PATH=DB_PATH,
                PORT=str(port),
                PROVIDER_CONTROL_DIR=CONTROL,
            )
            log = open(f"{WORK}/app-{port}.log", "w")
            proc = subprocess.Popen(
                [sys.executable, "-m", "payapi"], cwd=REPO_ROOT, env=env,
                stdout=log, stderr=subprocess.STDOUT,
            )
            procs.append(proc)
            wait_port(port, proc)

        failed = 0
        for scenario in SCENARIOS:
            set_fail(False)
            if os.path.exists(f"{CONTROL}/hold"):
                os.remove(f"{CONTROL}/hold")
            try:
                scenario(r, proxy)
                print(f"PASS {scenario.__name__}")
            except Exception:
                failed += 1
                print(f"FAIL {scenario.__name__}")
                traceback.print_exc(limit=3)
        print(f"{len(SCENARIOS) - failed}/{len(SCENARIOS)} scenarios passed")
        return 0 if failed == 0 else 1
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        for f in glob.glob(f"{CONTROL}/held.*"):
            os.remove(f)
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    sys.exit(main())
