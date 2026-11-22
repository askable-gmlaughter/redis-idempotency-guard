# payapi

A small payment API with Redis-backed idempotency keys. Clients can retry `POST /payments` as many times as they like, and it creates at most one payment per key, even when several app instances get the same request at the same moment.

## Quick start

```bash
docker compose up --build
curl -s localhost:8080/payments \
  -H 'Content-Type: application/json' \
  -H 'X-Merchant-Id: m-1' -H 'Idempotency-Key: order-42' \
  -d '{"amount": 1500, "currency": "KES", "account": "acct-1"}'
```

Running the same command again returns the same `201` and the same `payment_id`, and creates no new payment.

## API

### `POST /payments`

Headers: `X-Merchant-Id` and `Idempotency-Key` (both required).
Body: `{"amount": <int, minor units, > 0>, "currency": "<ISO 4217>", "account": "<string>"}`

| Situation | Response |
|---|---|
| First request for a merchant + key | `201` with the payment |
| Same key and body while the first request is still running | `409 {"error": "in_progress"}` |
| Same key and body after it finished | The original status and body, replayed exactly |
| Same key with a different body | `422 {"error": "key_reused"}` |
| Invalid body | `400`. The key is not used. |
| Provider failure | `502 {"error": "provider_error"}`. The key is released, so a retry is processed as new. |

Keys are scoped per merchant. Two bodies count as the same if they are equal as JSON objects, so key order and whitespace don't matter.

### `POST /payments/batch`

Header: `X-Merchant-Id`. Body: `{"items": [{...payment, "idempotency_key": "..."}]}`.
Each item follows the same rules as `POST /payments` and shares the same key space. The response is `200 {"results": [{"status": ..., "body": ...}]}`, in item order.

## How the guard works

`payapi/idempotency.py`:

1. **Claim:** `SET idem:<merchant>:<key> <claim> NX PX 30000`. The claim value carries the request's fingerprint and a random owner token. `NX` makes the claim atomic, so two concurrent requests can't both win it.
2. **Finish:** a Lua script replaces the claim with `{status, body}` (24-hour TTL), but only if the stored value is still this request's claim.
3. **Release:** a Lua script deletes the key on a 5xx, again only if this request still owns it.

The ownership checks matter when a claim expires mid-request. Without them, the slow request would overwrite or delete the claim of the request that took over.

## Configuration

| Variable | Default |
|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379/0` |
| `DB_PATH` | `/app/data/payments.db` |
| `HOST` | `127.0.0.1` |
| `PORT` | `8080` |
| `PROVIDER_CONTROL_DIR` | unset (sandbox fault injection, see `payapi/provider.py`) |

## Tests

The integration suite starts its own `redis-server`, a TCP proxy that forces requests to overlap, and two app instances:

```bash
pip install -r requirements.txt
python tests/integration_test.py      # needs redis-server on PATH
```

To run the suite without installing Redis locally:

```bash
docker compose run --rm --build test
```
