from flask import Blueprint, jsonify, request

from . import db, idempotency, provider

bp = Blueprint("payments", __name__)


def _validate(body):
    amount = body.get("amount")
    if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
        return "invalid_amount"
    currency = body.get("currency")
    if not (isinstance(currency, str) and len(currency) == 3 and currency.isalpha() and currency.isupper()):
        return "invalid_currency"
    account = body.get("account")
    if not isinstance(account, str) or not account.strip():
        return "invalid_account"
    return None


def _process(merchant_id, body):
    provider.charge(merchant_id, body["amount"], body["currency"], body["account"])
    payment_id = db.insert_payment(merchant_id, body["amount"], body["currency"], body["account"])
    return 201, {
        "payment_id": payment_id,
        "amount": body["amount"],
        "currency": body["currency"],
        "account": body["account"],
        "status": "created",
    }


def handle(merchant_id, idem_key, body):
    error = _validate(body)
    if error:
        return 400, {"error": error}

    outcome = idempotency.begin(merchant_id, idem_key, body)
    if outcome[0] == "done":
        return outcome[1], outcome[2]
    claim = outcome[1]

    try:
        status, response = _process(merchant_id, body)
    except provider.ProviderError:
        idempotency.release(claim)
        return 502, {"error": "provider_error"}
    except Exception:
        idempotency.release(claim)
        raise

    idempotency.finish(claim, status, response)
    return status, response


@bp.post("/payments")
def create_payment():
    merchant_id = request.headers.get("X-Merchant-Id")
    idem_key = request.headers.get("Idempotency-Key")
    if not merchant_id or not idem_key:
        return jsonify({"error": "missing_header"}), 400
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "invalid_body"}), 400
    status, response = handle(merchant_id, idem_key, body)
    return jsonify(response), status

