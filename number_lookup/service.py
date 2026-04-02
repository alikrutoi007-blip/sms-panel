import re
from urllib.parse import quote

import requests


_DIGITS_RE = re.compile(r"\D+")
_SMS_CAPABLE_TYPES = {
    "wireless",
    "mobile",
    "fixed_or_mobile",
    "fixedormobile",
}
_NON_SMS_TYPES = {
    "wireline",
    "landline",
    "voip",
    "fixed_voip",
    "fixedvoip",
    "non_fixed_voip",
    "nonfixedvoip",
    "pager",
    "satellite",
}


def normalize_phone_number(raw_number, default_country_code="1"):
    raw = str(raw_number or "").strip()
    if not raw:
        return ""

    default_country_code = _DIGITS_RE.sub("", str(default_country_code or ""))
    if raw.startswith("00"):
        raw = f"+{raw[2:]}"

    if raw.startswith("+"):
        digits = _DIGITS_RE.sub("", raw)
        if 8 <= len(digits) <= 15:
            return f"+{digits}"
        return ""

    digits = _DIGITS_RE.sub("", raw)
    if not digits:
        return ""

    if default_country_code and len(digits) == 10:
        digits = f"{default_country_code}{digits}"

    if 8 <= len(digits) <= 15:
        return f"+{digits}"
    return ""


def lookup_phone_number(api_key, phone_number, timeout=15):
    url = f"https://api.telnyx.com/v2/number_lookup/{quote(phone_number, safe='')}?carrier&caller-name"
    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        },
        timeout=timeout,
    )

    try:
        data = response.json()
    except ValueError:
        data = {}

    if response.status_code == 200:
        return True, response.status_code, data, ""

    errors = data.get("errors") or []
    if errors:
        first = errors[0] or {}
        error_text = first.get("detail") or first.get("title") or response.text
    else:
        error_text = response.text
    return False, response.status_code, data, error_text


def classify_lookup_result(raw_number, normalized_phone, payload):
    data = (payload or {}).get("data") or {}
    portability = data.get("portability") or {}
    carrier = data.get("carrier") or {}

    line_type = portability.get("line_type") or carrier.get("type") or ""
    carrier_name = (
        carrier.get("name")
        or portability.get("spid_carrier_name")
        or portability.get("altspid_carrier_name")
        or ""
    )
    country_code = data.get("country_code") or ""

    normalized_type = str(line_type or "").strip().lower().replace("-", "_").replace(" ", "_")
    sms_capable = normalized_type in _SMS_CAPABLE_TYPES
    known_bad = normalized_type in _NON_SMS_TYPES

    if sms_capable:
        decision = "accepted"
        reason = "sms_capable"
    elif known_bad:
        decision = "rejected"
        reason = f"non_sms_capable:{normalized_type}"
    else:
        decision = "unknown"
        reason = "lookup_inconclusive"

    return {
        "input_phone": raw_number,
        "normalized_phone": normalized_phone or data.get("phone_number") or "",
        "display_phone": data.get("phone_number") or normalized_phone or raw_number,
        "line_type": normalized_type,
        "carrier_name": carrier_name,
        "country_code": country_code,
        "sms_capable": sms_capable,
        "decision": decision,
        "reason": reason,
        "payload": payload or {},
    }
