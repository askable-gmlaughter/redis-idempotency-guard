"""Sandbox payment provider.

charge() is called once per payment, after the request has claimed its
idempotency key and before the payment row is written. Swap this module for
a real provider client in production.

For fault-injection testing it can be steered through the directory named by
the PROVIDER_CONTROL_DIR environment variable:

  * if <dir>/hold exists, the call takes it by renaming it to
    <dir>/held.<random> and then blocks until that file is removed;
  * if <dir>/fail exists when the call goes ahead, it raises ProviderError.
"""
import os
import time
import uuid

from . import config


class ProviderError(Exception):
    pass


def _wait_if_held(control_dir):
    held = os.path.join(control_dir, f"held.{uuid.uuid4().hex}")
    try:
        os.rename(os.path.join(control_dir, "hold"), held)
    except FileNotFoundError:
        return
    deadline = time.monotonic() + 120
    while os.path.exists(held) and time.monotonic() < deadline:
        time.sleep(0.01)


def charge(merchant_id, amount, currency, account):
    control_dir = config.PROVIDER_CONTROL_DIR
    if control_dir:
        _wait_if_held(control_dir)
        if os.path.exists(os.path.join(control_dir, "fail")):
            raise ProviderError("provider declined the charge")
    return "ok"
