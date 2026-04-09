import os
import threading
import time

import requests

from number_lookup import normalize_phone_number


RC_SERVER_URL = os.environ.get("RC_SERVER_URL", "https://platform.ringcentral.com").strip().rstrip("/")
RC_CLIENT_ID = os.environ.get("RC_CLIENT_ID", "").strip()
RC_CLIENT_SECRET = os.environ.get("RC_CLIENT_SECRET", "").strip()
RC_JWT = os.environ.get("RC_JWT", "").strip()
RC_EXTENSION_ID = os.environ.get("RC_EXTENSION_ID", "~").strip() or "~"
RC_REQUEST_TIMEOUT_SECONDS = max(5, int(os.environ.get("RC_REQUEST_TIMEOUT_SECONDS", "30")))

_token_lock = threading.Lock()
_token_cache = {
    "access_token": "",
    "expires_at": 0.0,
}


class RingCentralApiError(Exception):
    def __init__(self, message, status_code=None, response_data=None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response_data = response_data or {}


def ringcentral_enabled():
    return bool(RC_CLIENT_ID and RC_CLIENT_SECRET and RC_JWT)


def ringcentral_config_summary():
    missing = []
    if not RC_CLIENT_ID:
        missing.append("RC_CLIENT_ID")
    if not RC_CLIENT_SECRET:
        missing.append("RC_CLIENT_SECRET")
    if not RC_JWT:
        missing.append("RC_JWT")

    return {
        "enabled": not missing,
        "server_url": RC_SERVER_URL,
        "extension_id": RC_EXTENSION_ID,
        "client_id_set": bool(RC_CLIENT_ID),
        "client_secret_set": bool(RC_CLIENT_SECRET),
        "jwt_set": bool(RC_JWT),
        "missing": missing,
    }


def _parse_json(response):
    try:
        return response.json()
    except ValueError:
        return {}


def _extract_error_message(data, fallback):
    if not isinstance(data, dict):
        return fallback

    errors = data.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0] or {}
        return (
            first.get("error_description")
            or first.get("message")
            or first.get("detail")
            or first.get("description")
            or fallback
        )

    return (
        data.get("error_description")
        or data.get("message")
        or data.get("description")
        or data.get("error")
        or fallback
    )


def _fetch_access_token():
    if not ringcentral_enabled():
        summary = ringcentral_config_summary()
        raise RingCentralApiError(
            f"RingCentral is not configured. Missing: {', '.join(summary['missing'])}",
            status_code=400,
            response_data=summary,
        )

    try:
        response = requests.post(
            f"{RC_SERVER_URL}/restapi/oauth/token",
            auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
            headers={"Accept": "application/json"},
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": RC_JWT,
            },
            timeout=RC_REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise RingCentralApiError(f"RingCentral auth request failed: {exc}") from exc
    data = _parse_json(response)
    if response.status_code != 200:
        raise RingCentralApiError(
            _extract_error_message(data, response.text or "Unable to authenticate with RingCentral"),
            status_code=response.status_code,
            response_data=data,
        )

    expires_in = int(data.get("expires_in") or 3600)
    access_token = data.get("access_token", "")
    if not access_token:
        raise RingCentralApiError("RingCentral did not return an access token", status_code=500, response_data=data)

    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = time.time() + max(60, expires_in - 60)
    return access_token


def get_access_token(force_refresh=False):
    with _token_lock:
        if force_refresh or not _token_cache["access_token"] or time.time() >= _token_cache["expires_at"]:
            return _fetch_access_token()
        return _token_cache["access_token"]


def api_request(method, path, params=None, json=None, expected_statuses=(200,), retry_on_401=True):
    token = get_access_token()
    try:
        response = requests.request(
            method.upper(),
            f"{RC_SERVER_URL}{path}",
            params=params,
            json=json,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=RC_REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise RingCentralApiError(f"RingCentral request failed: {exc}") from exc
    data = _parse_json(response)

    if response.status_code == 401 and retry_on_401:
        token = get_access_token(force_refresh=True)
        try:
            response = requests.request(
                method.upper(),
                f"{RC_SERVER_URL}{path}",
                params=params,
                json=json,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=RC_REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            raise RingCentralApiError(f"RingCentral request failed after token refresh: {exc}") from exc
        data = _parse_json(response)

    if response.status_code not in expected_statuses:
        raise RingCentralApiError(
            _extract_error_message(data, response.text or f"RingCentral request failed with status {response.status_code}"),
            status_code=response.status_code,
            response_data=data,
        )

    return data


def _extract_sender_records(records, source):
    senders = []
    for record in records or []:
        features = list(record.get("features") or [])
        senders.append(
            {
                "id": str(record.get("id") or ""),
                "phone_number": record.get("phoneNumber") or record.get("phone_number") or "",
                "usage_type": record.get("usageType") or record.get("usage_type") or "",
                "payment_type": record.get("paymentType") or record.get("payment_type") or "",
                "features": features,
                "source": source,
                "extension_id": str(record.get("extension", {}).get("id") or record.get("extensionId") or ""),
                "has_a2p_sender": "A2PSmsSender" in features,
                "has_sms_sender": "SmsSender" in features,
            }
        )
    return senders


def list_phone_number_inventory():
    inventory = []
    seen = set()

    extension_data = api_request(
        "GET",
        f"/restapi/v1.0/account/~/extension/{RC_EXTENSION_ID}/phone-number",
    )
    for record in _extract_sender_records(extension_data.get("records"), "extension"):
        key = record["phone_number"]
        if key and key not in seen:
            inventory.append(record)
            seen.add(key)

    account_data = api_request("GET", "/restapi/v1.0/account/~/phone-number")
    for record in _extract_sender_records(account_data.get("records"), "account"):
        key = record["phone_number"]
        if key and key not in seen:
            inventory.append(record)
            seen.add(key)

    inventory.sort(key=lambda item: (item["source"], item["phone_number"]))
    return inventory


def list_a2p_senders():
    senders = [
        item for item in list_phone_number_inventory()
        if item["source"] == "extension" and item["has_a2p_sender"]
    ]
    senders.sort(key=lambda item: item["phone_number"])
    return senders


def normalize_recipients(numbers, default_country_code="1"):
    accepted = []
    rejected = []
    seen = set()

    for raw in numbers or []:
        candidate = str(raw or "").strip()
        if not candidate:
            continue
        normalized = normalize_phone_number(candidate, default_country_code)
        if not normalized:
            rejected.append(
                {
                    "input_phone": candidate,
                    "normalized_phone": "",
                    "reason": "Invalid phone number format",
                }
            )
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        accepted.append(normalized)

    return {
        "accepted_numbers": accepted,
        "rejected": rejected,
        "submitted_count": len([item for item in numbers or [] if str(item or "").strip()]),
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
    }


def create_batch(from_number, text, recipients):
    payload = {
        "from": from_number,
        "text": text,
        "messages": [{"to": [recipient]} for recipient in recipients],
    }
    return api_request(
        "POST",
        "/restapi/v1.0/account/~/a2p-sms/batches",
        json=payload,
        expected_statuses=(200, 201),
    )


def get_batch(batch_id):
    return api_request("GET", f"/restapi/v1.0/account/~/a2p-sms/batches/{batch_id}")


def get_batch_statuses(batch_id):
    return api_request(
        "GET",
        "/restapi/v1.0/account/~/a2p-sms/statuses",
        params={"batchId": batch_id},
    )


def list_batch_messages(batch_id, per_page=100):
    data = api_request(
        "GET",
        "/restapi/v1.0/account/~/a2p-sms/messages",
        params={
            "batchId": batch_id,
            "view": "Detailed",
            "perPage": min(max(1, int(per_page)), 1000),
        },
    )
    records = []
    for record in data.get("records") or []:
        records.append(
            {
                "id": str(record.get("id") or ""),
                "from": record.get("from") or "",
                "to": (record.get("to") or [""])[0],
                "text": record.get("text") or "",
                "status": record.get("messageStatus") or record.get("status") or "",
                "cost": record.get("cost"),
                "segment_count": record.get("segmentCount") or 0,
                "error_code": record.get("errorCode") or "",
                "created_at": record.get("createdAt") or record.get("creationTime") or "",
                "updated_at": record.get("lastUpdatedAt") or record.get("lastModifiedTime") or "",
            }
        )
    return records


def create_webhook_subscription(webhook_url, from_number=""):
    event_filters = ["/restapi/v1.0/account/~/a2p-sms/batches"]
    if from_number:
        event_filters.append(f"/restapi/v1.0/account/~/a2p-sms/messages?direction=Outbound&from={from_number}")

    payload = {
        "eventFilters": event_filters,
        "deliveryMode": {
            "transportType": "WebHook",
            "address": webhook_url,
        },
    }
    return api_request(
        "POST",
        "/restapi/v1.0/subscription",
        json=payload,
        expected_statuses=(200, 201),
    )
