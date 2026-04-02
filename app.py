import hmac
import json
import os
import psycopg2
import psycopg2.extras
import requests
import time
import threading
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, render_template, g, redirect, session, url_for

from number_lookup import classify_lookup_result, lookup_phone_number, normalize_phone_number

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SESSION_SECRET") or os.environ.get("FLASK_SECRET_KEY") or "change-me-in-production"
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

TELNYX_API_KEY = os.environ.get("TELNYX_API_KEY", "YOUR_API_KEY")
MY_NUMBER     = os.environ.get("MY_NUMBER", "+15512300914")
_DATABASE_URL  = os.environ.get("DATABASE_URL", "")
APP_USERNAME   = os.environ.get("APP_USERNAME", "Baroservicellc").strip()
APP_PASSWORD   = os.environ.get("APP_PASSWORD", "Baro20252025")
AUTH_ENABLED   = bool(APP_USERNAME and APP_PASSWORD)
BROADCAST_RATE_LIMIT_PER_MINUTE = max(1, int(os.environ.get("BROADCAST_RATE_LIMIT_PER_MINUTE", "2")))
BROADCAST_MIN_INTERVAL_SECONDS = 60.0 / BROADCAST_RATE_LIMIT_PER_MINUTE
MAX_BROADCAST_RETRIES = max(1, int(os.environ.get("MAX_BROADCAST_RETRIES", "3")))
BROADCAST_STALE_MINUTES = max(5, int(os.environ.get("BROADCAST_STALE_MINUTES", "10")))
BROADCAST_WORKER_LOCK_ID = 2026040201
DEFAULT_COUNTRY_CODE = os.environ.get("DEFAULT_COUNTRY_CODE", "1")

# Railway даёт "postgres://..." — psycopg2 требует "postgresql://..."
DATABASE_URL = _DATABASE_URL.replace("postgres://", "postgresql://", 1) if _DATABASE_URL.startswith("postgres://") else _DATABASE_URL
_broadcast_worker = None
_broadcast_worker_guard = threading.Lock()


def env_flag(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


NUMBER_LOOKUP_ENABLED = env_flag(
    "NUMBER_LOOKUP_ENABLED",
    bool(TELNYX_API_KEY and TELNYX_API_KEY != "YOUR_API_KEY"),
)
NUMBER_LOOKUP_TIMEOUT_SECONDS = max(5, int(os.environ.get("NUMBER_LOOKUP_TIMEOUT_SECONDS", "15")))
NUMBER_LOOKUP_CACHE_DAYS = max(1, int(os.environ.get("NUMBER_LOOKUP_CACHE_DAYS", "30")))
NUMBER_LOOKUP_FAIL_CLOSED = env_flag("NUMBER_LOOKUP_FAIL_CLOSED", True)


def _safe_next_url(value):
    if value and value.startswith("/") and not value.startswith("//"):
        return value
    return url_for("index")


def utcnow_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")


def iso_after(seconds):
    return (datetime.utcnow() + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%f")


def iso_days_after(days):
    return (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%f")


def parse_telnyx_json(resp):
    try:
        return resp.json()
    except ValueError:
        return {}


def extract_telnyx_cost(payload):
    payload = payload or {}
    cost = payload.get("cost") or {}
    breakdown = payload.get("cost_breakdown") or {}
    carrier_fee = breakdown.get("carrier_fee") or {}
    rate = breakdown.get("rate") or {}
    return {
        "cost_amount": cost.get("amount"),
        "cost_currency": cost.get("currency"),
        "cost_carrier_fee": carrier_fee.get("amount"),
        "cost_rate": rate.get("amount"),
        "parts": payload.get("parts") or 0,
    }


def extract_telnyx_error(payload):
    errors = (payload or {}).get("errors") or []
    if not errors:
        return "", ""
    first = errors[0] or {}
    return first.get("code", ""), first.get("detail") or first.get("title") or ""


def extract_telnyx_timing(payload):
    payload = payload or {}
    return {
        "provider_received_at": payload.get("received_at"),
        "provider_sent_at": payload.get("sent_at"),
        "provider_completed_at": payload.get("completed_at"),
        "provider_valid_until": payload.get("valid_until"),
        "provider_wait_seconds": payload.get("wait_seconds"),
    }


def parse_iso_utc(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def seconds_between(start_value, end_value):
    start_dt = parse_iso_utc(start_value)
    end_dt = parse_iso_utc(end_value)
    if not start_dt or not end_dt:
        return None
    return round((end_dt - start_dt).total_seconds(), 3)


def sql_utc_timestamptz(column_name):
    return f"""
        CASE
            WHEN {column_name} IS NULL OR {column_name} = '' THEN NULL
            WHEN {column_name} ~ '(Z|[+-][0-9]{{2}}:[0-9]{{2}})$' THEN {column_name}::timestamptz
            ELSE ({column_name} || '+00:00')::timestamptz
        END
    """


def coerce_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def lookup_reason_display(decision, line_type="", error_text=""):
    if decision == "accepted":
        return f"SMS-capable ({line_type or 'mobile'})"
    if decision == "invalid_format":
        return "Invalid phone format"
    if decision == "rejected":
        return f"Not SMS-capable ({line_type or 'unknown'})"
    if decision == "lookup_error":
        return error_text or "Number lookup failed"
    if decision == "format_only":
        return "Format looks valid, lookup disabled"
    return "Lookup result is inconclusive"


def sanitize_lookup_result(result):
    return {
        "input_phone": result.get("input_phone", ""),
        "normalized_phone": result.get("normalized_phone", ""),
        "accepted": bool(result.get("accepted")),
        "decision": result.get("decision", ""),
        "reason": result.get("reason", ""),
        "reason_display": result.get("reason_display", ""),
        "line_type": result.get("line_type", ""),
        "carrier_name": result.get("carrier_name", ""),
        "country_code": result.get("country_code", ""),
        "cache_hit": bool(result.get("cache_hit")),
        "lookup_performed": bool(result.get("lookup_performed")),
        "source": result.get("source", ""),
        "checked_at": result.get("checked_at"),
        "expires_at": result.get("expires_at"),
        "error_text": result.get("error_text", ""),
    }


def load_cached_lookup(conn, normalized_phone):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM number_lookup_cache
        WHERE normalized_phone = %s
          AND expires_at >= %s
        """,
        (normalized_phone, utcnow_iso())
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return dict(row)


def save_cached_lookup(conn, result, expires_days):
    checked_at = utcnow_iso()
    expires_at = iso_days_after(expires_days)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO number_lookup_cache (
            normalized_phone, accepted, decision, reason, reason_display,
            line_type, carrier_name, country_code, source, error_text,
            raw_payload, checked_at, expires_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (normalized_phone) DO UPDATE SET
            accepted = EXCLUDED.accepted,
            decision = EXCLUDED.decision,
            reason = EXCLUDED.reason,
            reason_display = EXCLUDED.reason_display,
            line_type = EXCLUDED.line_type,
            carrier_name = EXCLUDED.carrier_name,
            country_code = EXCLUDED.country_code,
            source = EXCLUDED.source,
            error_text = EXCLUDED.error_text,
            raw_payload = EXCLUDED.raw_payload,
            checked_at = EXCLUDED.checked_at,
            expires_at = EXCLUDED.expires_at
        """,
        (
            result.get("normalized_phone", ""),
            bool(result.get("accepted")),
            result.get("decision", ""),
            result.get("reason", ""),
            result.get("reason_display", ""),
            result.get("line_type", ""),
            result.get("carrier_name", ""),
            result.get("country_code", ""),
            result.get("source", ""),
            result.get("error_text", ""),
            json.dumps(result.get("raw_payload") or {}, ensure_ascii=True),
            checked_at,
            expires_at,
        )
    )
    cur.close()
    result["checked_at"] = checked_at
    result["expires_at"] = expires_at


def assess_phone_for_sms(conn, raw_number):
    input_phone = str(raw_number or "").strip()
    normalized_phone = normalize_phone_number(input_phone, DEFAULT_COUNTRY_CODE)
    if not normalized_phone:
        return {
            "input_phone": input_phone,
            "normalized_phone": "",
            "accepted": False,
            "decision": "invalid_format",
            "reason": "invalid_format",
            "reason_display": lookup_reason_display("invalid_format"),
            "line_type": "",
            "carrier_name": "",
            "country_code": "",
            "cache_hit": False,
            "lookup_performed": False,
            "source": "format",
            "error_text": "",
            "raw_payload": {},
            "checked_at": utcnow_iso(),
            "expires_at": iso_days_after(NUMBER_LOOKUP_CACHE_DAYS),
        }

    cached = load_cached_lookup(conn, normalized_phone)
    if cached:
        return {
            "input_phone": input_phone,
            "normalized_phone": normalized_phone,
            "accepted": bool(cached.get("accepted")),
            "decision": cached.get("decision", ""),
            "reason": cached.get("reason", ""),
            "reason_display": cached.get("reason_display", ""),
            "line_type": cached.get("line_type", ""),
            "carrier_name": cached.get("carrier_name", ""),
            "country_code": cached.get("country_code", ""),
            "cache_hit": True,
            "lookup_performed": False,
            "source": cached.get("source", "cache"),
            "error_text": cached.get("error_text", ""),
            "raw_payload": {},
            "checked_at": cached.get("checked_at"),
            "expires_at": cached.get("expires_at"),
        }

    if not NUMBER_LOOKUP_ENABLED:
        return {
            "input_phone": input_phone,
            "normalized_phone": normalized_phone,
            "accepted": True,
            "decision": "format_only",
            "reason": "lookup_disabled",
            "reason_display": lookup_reason_display("format_only"),
            "line_type": "",
            "carrier_name": "",
            "country_code": "",
            "cache_hit": False,
            "lookup_performed": False,
            "source": "format",
            "error_text": "",
            "raw_payload": {},
            "checked_at": utcnow_iso(),
            "expires_at": iso_days_after(NUMBER_LOOKUP_CACHE_DAYS),
        }

    if not TELNYX_API_KEY or TELNYX_API_KEY == "YOUR_API_KEY":
        accepted = not NUMBER_LOOKUP_FAIL_CLOSED
        result = {
            "input_phone": input_phone,
            "normalized_phone": normalized_phone,
            "accepted": accepted,
            "decision": "lookup_error",
            "reason": "missing_telnyx_api_key",
            "reason_display": lookup_reason_display("lookup_error", error_text="Number lookup API key is missing"),
            "line_type": "",
            "carrier_name": "",
            "country_code": "",
            "cache_hit": False,
            "lookup_performed": False,
            "source": "lookup",
            "error_text": "Number lookup API key is missing",
            "raw_payload": {},
        }
        save_cached_lookup(conn, result, 1)
        return result

    try:
        ok, status_code, data, error_text = lookup_phone_number(
            TELNYX_API_KEY,
            normalized_phone,
            timeout=NUMBER_LOOKUP_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        ok, status_code, data, error_text = False, None, {}, str(exc)

    if ok:
        classified = classify_lookup_result(input_phone, normalized_phone, data)
        decision = classified["decision"]
        accepted = decision == "accepted"
        if decision == "unknown":
            accepted = not NUMBER_LOOKUP_FAIL_CLOSED
            decision = "accepted" if accepted else "lookup_error"

        result = {
            "input_phone": input_phone,
            "normalized_phone": classified.get("normalized_phone") or normalized_phone,
            "accepted": accepted,
            "decision": decision,
            "reason": classified.get("reason", ""),
            "reason_display": lookup_reason_display(
                "accepted" if accepted and classified.get("decision") == "accepted" else decision,
                line_type=classified.get("line_type", ""),
                error_text="Lookup result is inconclusive",
            ),
            "line_type": classified.get("line_type", ""),
            "carrier_name": classified.get("carrier_name", ""),
            "country_code": classified.get("country_code", ""),
            "cache_hit": False,
            "lookup_performed": True,
            "source": "telnyx",
            "error_text": "" if classified.get("decision") != "unknown" else "Lookup result is inconclusive",
            "raw_payload": data,
        }
        save_cached_lookup(conn, result, NUMBER_LOOKUP_CACHE_DAYS)
        return result

    accepted = not NUMBER_LOOKUP_FAIL_CLOSED
    result = {
        "input_phone": input_phone,
        "normalized_phone": normalized_phone,
        "accepted": accepted,
        "decision": "lookup_error",
        "reason": f"lookup_http_{status_code or 'error'}",
        "reason_display": lookup_reason_display("lookup_error", error_text=error_text),
        "line_type": "",
        "carrier_name": "",
        "country_code": "",
        "cache_hit": False,
        "lookup_performed": False,
        "source": "telnyx",
        "error_text": error_text,
        "raw_payload": data,
    }
    save_cached_lookup(conn, result, 1)
    return result


def evaluate_numbers_for_sms(conn, numbers_raw):
    results = []
    seen = set()
    duplicates_removed = 0

    for raw_number in numbers_raw or []:
        text = str(raw_number or "").strip()
        if not text:
            continue
        normalized = normalize_phone_number(text, DEFAULT_COUNTRY_CODE)
        dedupe_key = normalized or text
        if dedupe_key in seen:
            duplicates_removed += 1
            continue
        seen.add(dedupe_key)
        results.append(assess_phone_for_sms(conn, text))

    accepted = [sanitize_lookup_result(r) for r in results if r.get("accepted")]
    rejected = [sanitize_lookup_result(r) for r in results if not r.get("accepted")]

    return {
        "total_submitted": len([n for n in numbers_raw or [] if str(n or "").strip()]),
        "processed": len(results),
        "duplicates_removed": duplicates_removed,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "cache_hits": sum(1 for r in results if r.get("cache_hit")),
        "lookup_hits": sum(1 for r in results if r.get("lookup_performed")),
        "accepted_numbers": [r["normalized_phone"] for r in accepted if r.get("normalized_phone")],
        "accepted": accepted,
        "rejected": rejected,
    }


def send_telnyx_message(to_number, from_number, text):
    resp = requests.post(
        "https://api.telnyx.com/v2/messages",
        headers={
            "Authorization": f"Bearer {TELNYX_API_KEY}",
            "Content-Type": "application/json"
        },
        json={"from": from_number, "to": to_number, "text": text},
        timeout=20
    )
    data = parse_telnyx_json(resp)
    if resp.status_code in (200, 201):
        return True, resp.status_code, data, ""

    errors = data.get("errors") or []
    if errors:
        first = errors[0] or {}
        error_text = first.get("detail") or first.get("title") or resp.text
    else:
        error_text = resp.text
    return False, resp.status_code, data, error_text


def insert_message_record(conn, direction, contact, my_number, body, status, created_at,
                          provider_message_id="", cost_payload=None, finalized_at=None,
                          error_code="", error_detail="", broadcast_id=None,
                          broadcast_recipient_id=None):
    cost_data = extract_telnyx_cost(cost_payload)
    timing_data = extract_telnyx_timing(cost_payload)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO messages (
            direction, contact, my_number, body, status, created_at,
            provider_message_id, cost_amount, cost_currency, cost_carrier_fee,
            cost_rate, parts, finalized_at, error_code, error_detail,
            broadcast_id, broadcast_recipient_id, provider_received_at,
            provider_sent_at, provider_completed_at, provider_valid_until,
            provider_wait_seconds
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            direction, contact, my_number, body, status, created_at,
            provider_message_id or None,
            cost_data["cost_amount"],
            cost_data["cost_currency"],
            cost_data["cost_carrier_fee"],
            cost_data["cost_rate"],
            cost_data["parts"],
            finalized_at,
            error_code or None,
            error_detail or None,
            broadcast_id,
            broadcast_recipient_id,
            timing_data["provider_received_at"],
            timing_data["provider_sent_at"],
            timing_data["provider_completed_at"],
            timing_data["provider_valid_until"],
            timing_data["provider_wait_seconds"],
        )
    )
    cur.close()


def refresh_broadcast_stats(conn, broadcast_id):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT
            COUNT(*)::int AS total,
            COUNT(*) FILTER (
                WHERE status IN ('queued', 'sending', 'sent', 'delivered', 'delivery_failed', 'delivery_unconfirmed', 'sending_failed')
            )::int AS sent,
            COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
            COUNT(*) FILTER (WHERE status = 'delivered')::int AS delivered,
            COUNT(*) FILTER (WHERE status IN ('delivery_failed', 'sending_failed'))::int AS delivery_failed,
            COUNT(*) FILTER (WHERE status = 'delivery_unconfirmed')::int AS delivery_unconfirmed,
            COUNT(*) FILTER (
                WHERE status IN ('delivered', 'delivery_failed', 'delivery_unconfirmed', 'sending_failed')
            )::int AS finalized,
            COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
            COUNT(*) FILTER (WHERE status = 'sending')::int AS sending,
            COALESCE(SUM(COALESCE(cost_amount, 0)), 0) AS cost_amount,
            MAX(NULLIF(cost_currency, '')) AS cost_currency
        FROM broadcast_recipients
        WHERE broadcast_id = %s
        """,
        (broadcast_id,)
    )
    stats = cur.fetchone()
    cur.close()

    if not stats or stats["total"] == 0:
        return

    confirmed_total = stats["delivered"] + stats["delivery_failed"] + stats["delivery_unconfirmed"]
    delivery_rate = round((stats["delivered"] * 100.0) / confirmed_total, 2) if confirmed_total else None

    if stats["pending"] == stats["total"] and stats["sent"] == 0 and stats["failed"] == 0:
        status = "queued"
    elif stats["pending"] == 0 and stats["sending"] == 0:
        status = "done"
    else:
        status = "running"

    finished_at = utcnow_iso() if status == "done" else None

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE broadcasts
        SET
            total = %s,
            sent = %s,
            failed = %s,
            delivered = %s,
            delivery_failed = %s,
            delivery_unconfirmed = %s,
            finalized = %s,
            cost_amount = %s,
            cost_currency = COALESCE(%s, cost_currency, 'USD'),
            delivery_rate = %s,
            status = %s,
            finished_at = CASE
                WHEN %s IS NOT NULL THEN COALESCE(finished_at, %s)
                ELSE finished_at
            END
        WHERE id = %s
        """,
        (
            stats["total"],
            stats["sent"],
            stats["failed"],
            stats["delivered"],
            stats["delivery_failed"],
            stats["delivery_unconfirmed"],
            stats["finalized"],
            stats["cost_amount"],
            stats["cost_currency"],
            delivery_rate,
            status,
            finished_at,
            finished_at,
            broadcast_id,
        )
    )
    cur.close()


def update_outbound_tracking(conn, payload, event_type):
    payload = payload or {}
    provider_message_id = payload.get("id", "")
    to_num = (payload.get("to") or [{}])[0].get("phone_number", "")
    status = (payload.get("to") or [{}])[0].get("status", "sent")
    finalized_at = utcnow_iso() if event_type == "message.finalized" else None
    cost_data = extract_telnyx_cost(payload)
    timing_data = extract_telnyx_timing(payload)
    error_code, error_detail = extract_telnyx_error(payload)
    updated_broadcasts = set()

    cur = conn.cursor()
    message_updated = False
    if provider_message_id:
        cur.execute(
            """
            UPDATE messages
            SET
                provider_message_id = COALESCE(provider_message_id, %s),
                status = %s,
                cost_amount = COALESCE(%s, cost_amount),
                cost_currency = COALESCE(%s, cost_currency),
                cost_carrier_fee = COALESCE(%s, cost_carrier_fee),
                cost_rate = COALESCE(%s, cost_rate),
                parts = CASE WHEN %s > 0 THEN %s ELSE parts END,
                finalized_at = CASE WHEN %s IS NOT NULL THEN %s ELSE finalized_at END,
                provider_received_at = COALESCE(%s, provider_received_at),
                provider_sent_at = COALESCE(%s, provider_sent_at),
                provider_completed_at = COALESCE(%s, provider_completed_at),
                provider_valid_until = COALESCE(%s, provider_valid_until),
                provider_wait_seconds = COALESCE(%s, provider_wait_seconds),
                error_code = COALESCE(NULLIF(%s, ''), error_code),
                error_detail = COALESCE(NULLIF(%s, ''), error_detail)
            WHERE provider_message_id = %s
            RETURNING broadcast_id
            """,
            (
                provider_message_id,
                status,
                cost_data["cost_amount"],
                cost_data["cost_currency"],
                cost_data["cost_carrier_fee"],
                cost_data["cost_rate"],
                cost_data["parts"],
                cost_data["parts"],
                finalized_at,
                finalized_at,
                timing_data["provider_received_at"],
                timing_data["provider_sent_at"],
                timing_data["provider_completed_at"],
                timing_data["provider_valid_until"],
                timing_data["provider_wait_seconds"],
                error_code,
                error_detail,
                provider_message_id,
            )
        )
        rows = cur.fetchall()
        message_updated = bool(rows)
        updated_broadcasts.update({row[0] for row in rows if row and row[0]})

    if not message_updated and to_num:
        cur.execute(
            """
            UPDATE messages
            SET
                provider_message_id = COALESCE(provider_message_id, %s),
                status = %s,
                cost_amount = COALESCE(%s, cost_amount),
                cost_currency = COALESCE(%s, cost_currency),
                cost_carrier_fee = COALESCE(%s, cost_carrier_fee),
                cost_rate = COALESCE(%s, cost_rate),
                parts = CASE WHEN %s > 0 THEN %s ELSE parts END,
                finalized_at = CASE WHEN %s IS NOT NULL THEN %s ELSE finalized_at END,
                provider_received_at = COALESCE(%s, provider_received_at),
                provider_sent_at = COALESCE(%s, provider_sent_at),
                provider_completed_at = COALESCE(%s, provider_completed_at),
                provider_valid_until = COALESCE(%s, provider_valid_until),
                provider_wait_seconds = COALESCE(%s, provider_wait_seconds),
                error_code = COALESCE(NULLIF(%s, ''), error_code),
                error_detail = COALESCE(NULLIF(%s, ''), error_detail)
            WHERE id = (
                SELECT MAX(id) FROM messages
                WHERE contact = %s AND direction = 'outbound'
            )
            RETURNING broadcast_id
            """,
            (
                provider_message_id or None,
                status,
                cost_data["cost_amount"],
                cost_data["cost_currency"],
                cost_data["cost_carrier_fee"],
                cost_data["cost_rate"],
                cost_data["parts"],
                cost_data["parts"],
                finalized_at,
                finalized_at,
                timing_data["provider_received_at"],
                timing_data["provider_sent_at"],
                timing_data["provider_completed_at"],
                timing_data["provider_valid_until"],
                timing_data["provider_wait_seconds"],
                error_code,
                error_detail,
                to_num,
            )
        )
        rows = cur.fetchall()
        updated_broadcasts.update({row[0] for row in rows if row and row[0]})

    if provider_message_id:
        cur.execute(
            """
            UPDATE broadcast_recipients
            SET
                status = %s,
                cost_amount = COALESCE(%s, cost_amount),
                cost_currency = COALESCE(%s, cost_currency),
                cost_carrier_fee = COALESCE(%s, cost_carrier_fee),
                cost_rate = COALESCE(%s, cost_rate),
                parts = CASE WHEN %s > 0 THEN %s ELSE parts END,
                finalized_at = CASE WHEN %s IS NOT NULL THEN %s ELSE finalized_at END,
                provider_received_at = COALESCE(%s, provider_received_at),
                provider_sent_at = COALESCE(%s, provider_sent_at),
                provider_completed_at = COALESCE(%s, provider_completed_at),
                provider_valid_until = COALESCE(%s, provider_valid_until),
                provider_wait_seconds = COALESCE(%s, provider_wait_seconds),
                last_error_code = COALESCE(NULLIF(%s, ''), last_error_code),
                last_error_detail = COALESCE(NULLIF(%s, ''), last_error_detail)
            WHERE provider_message_id = %s
            RETURNING broadcast_id
            """,
            (
                status,
                cost_data["cost_amount"],
                cost_data["cost_currency"],
                cost_data["cost_carrier_fee"],
                cost_data["cost_rate"],
                cost_data["parts"],
                cost_data["parts"],
                finalized_at,
                finalized_at,
                timing_data["provider_received_at"],
                timing_data["provider_sent_at"],
                timing_data["provider_completed_at"],
                timing_data["provider_valid_until"],
                timing_data["provider_wait_seconds"],
                error_code,
                error_detail,
                provider_message_id,
            )
        )
        rows = cur.fetchall()
        updated_broadcasts.update({row[0] for row in rows if row and row[0]})

    cur.close()

    for broadcast_id in updated_broadcasts:
        refresh_broadcast_stats(conn, broadcast_id)


def get_broadcast_diagnostics(conn, broadcast_id, recent_limit=8):
    queued_ts = sql_utc_timestamptz("queued_at")
    sent_ts = sql_utc_timestamptz("sent_at")
    provider_received_ts = sql_utc_timestamptz("provider_received_at")
    provider_sent_ts = sql_utc_timestamptz("provider_sent_at")
    provider_completed_ts = sql_utc_timestamptz("provider_completed_at")
    finalized_ts = sql_utc_timestamptz("finalized_at")
    provider_stage_end = f"COALESCE({provider_sent_ts}, {provider_completed_ts}, {finalized_ts})"
    overall_end = f"COALESCE({provider_completed_ts}, {finalized_ts}, {provider_sent_ts}, {provider_received_ts}, {sent_ts})"

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"""
        SELECT
            COUNT(*)::int AS total,
            COUNT(*) FILTER (WHERE sent_at IS NOT NULL)::int AS app_sent_count,
            COUNT(*) FILTER (WHERE provider_received_at IS NOT NULL)::int AS provider_received_count,
            COUNT(*) FILTER (WHERE provider_sent_at IS NOT NULL)::int AS provider_sent_count,
            COUNT(*) FILTER (WHERE provider_completed_at IS NOT NULL OR finalized_at IS NOT NULL)::int AS provider_completed_count,
            AVG(EXTRACT(EPOCH FROM ({sent_ts} - {queued_ts}))) FILTER (
                WHERE {sent_ts} IS NOT NULL AND {queued_ts} IS NOT NULL
            ) AS avg_app_queue_seconds,
            MAX(EXTRACT(EPOCH FROM ({sent_ts} - {queued_ts}))) FILTER (
                WHERE {sent_ts} IS NOT NULL AND {queued_ts} IS NOT NULL
            ) AS max_app_queue_seconds,
            AVG(EXTRACT(EPOCH FROM ({provider_received_ts} - {sent_ts}))) FILTER (
                WHERE {provider_received_ts} IS NOT NULL AND {sent_ts} IS NOT NULL
            ) AS avg_provider_accept_seconds,
            AVG(EXTRACT(EPOCH FROM ({provider_stage_end} - COALESCE({provider_received_ts}, {sent_ts})))) FILTER (
                WHERE {provider_stage_end} IS NOT NULL
                  AND COALESCE({provider_received_ts}, {sent_ts}) IS NOT NULL
            ) AS avg_provider_queue_seconds,
            MAX(EXTRACT(EPOCH FROM ({provider_stage_end} - COALESCE({provider_received_ts}, {sent_ts})))) FILTER (
                WHERE {provider_stage_end} IS NOT NULL
                  AND COALESCE({provider_received_ts}, {sent_ts}) IS NOT NULL
            ) AS max_provider_queue_seconds,
            AVG(provider_wait_seconds) FILTER (WHERE provider_wait_seconds IS NOT NULL) AS avg_provider_wait_seconds,
            MAX(provider_wait_seconds) FILTER (WHERE provider_wait_seconds IS NOT NULL) AS max_provider_wait_seconds,
            AVG(EXTRACT(EPOCH FROM ({overall_end} - {queued_ts}))) FILTER (
                WHERE {overall_end} IS NOT NULL AND {queued_ts} IS NOT NULL
            ) AS avg_end_to_end_seconds,
            MAX(EXTRACT(EPOCH FROM ({overall_end} - {queued_ts}))) FILTER (
                WHERE {overall_end} IS NOT NULL AND {queued_ts} IS NOT NULL
            ) AS max_end_to_end_seconds
        FROM broadcast_recipients
        WHERE broadcast_id = %s
        """,
        (broadcast_id,)
    )
    summary = dict(cur.fetchone() or {})

    cur.execute(
        """
        SELECT
            id,
            phone,
            status,
            attempts,
            queued_at,
            last_attempt_at,
            sent_at,
            finalized_at,
            provider_received_at,
            provider_sent_at,
            provider_completed_at,
            provider_valid_until,
            provider_wait_seconds,
            last_error_code,
            last_error_detail
        FROM broadcast_recipients
        WHERE broadcast_id = %s
        ORDER BY COALESCE(
            provider_completed_at,
            finalized_at,
            provider_sent_at,
            provider_received_at,
            sent_at,
            last_attempt_at,
            queued_at
        ) DESC, id DESC
        LIMIT %s
        """,
        (broadcast_id, recent_limit)
    )
    recent_rows = [dict(row) for row in cur.fetchall()]
    cur.close()

    for key in (
        "avg_app_queue_seconds",
        "max_app_queue_seconds",
        "avg_provider_accept_seconds",
        "avg_provider_queue_seconds",
        "max_provider_queue_seconds",
        "avg_provider_wait_seconds",
        "max_provider_wait_seconds",
        "avg_end_to_end_seconds",
        "max_end_to_end_seconds",
    ):
        summary[key] = coerce_float(summary.get(key))

    for row in recent_rows:
        row["provider_wait_seconds"] = coerce_float(row.get("provider_wait_seconds"))
        row["app_queue_seconds"] = seconds_between(
            row.get("queued_at"),
            row.get("sent_at") or row.get("last_attempt_at")
        )
        row["provider_accept_seconds"] = seconds_between(
            row.get("sent_at"),
            row.get("provider_received_at")
        )
        row["provider_queue_seconds"] = seconds_between(
            row.get("provider_received_at") or row.get("sent_at"),
            row.get("provider_sent_at") or row.get("provider_completed_at") or row.get("finalized_at")
        )
        row["end_to_end_seconds"] = seconds_between(
            row.get("queued_at"),
            row.get("provider_completed_at")
            or row.get("finalized_at")
            or row.get("provider_sent_at")
            or row.get("provider_received_at")
            or row.get("sent_at")
        )

    return {
        "summary": summary,
        "recent_recipients": recent_rows,
    }


def recover_stuck_recipients(conn):
    stale_before = (datetime.utcnow() - timedelta(minutes=BROADCAST_STALE_MINUTES)).strftime("%Y-%m-%dT%H:%M:%S.%f")
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE broadcast_recipients
        SET status = 'pending'
        WHERE status = 'sending'
          AND provider_message_id IS NULL
          AND attempts < %s
          AND last_attempt_at IS NOT NULL
          AND last_attempt_at < %s
        """,
        (MAX_BROADCAST_RETRIES, stale_before)
    )
    cur.execute(
        """
        UPDATE broadcast_recipients
        SET
            status = 'failed',
            finalized_at = COALESCE(finalized_at, %s),
            last_error_detail = CASE
                WHEN COALESCE(last_error_detail, '') = '' THEN 'Не удалось подтвердить отправку после перезапуска'
                ELSE last_error_detail
            END
        WHERE status = 'sending'
          AND provider_message_id IS NULL
          AND attempts >= %s
          AND last_attempt_at IS NOT NULL
          AND last_attempt_at < %s
        RETURNING broadcast_id
        """,
        (utcnow_iso(), MAX_BROADCAST_RETRIES, stale_before)
    )
    stuck_rows = cur.fetchall()
    cur.close()

    broadcast_ids = {row[0] for row in stuck_rows if row and row[0]}
    cur = conn.cursor()
    cur.execute("SELECT id FROM broadcasts WHERE status IN ('queued', 'running')")
    broadcast_ids.update({row[0] for row in cur.fetchall()})
    cur.close()

    for broadcast_id in broadcast_ids:
        refresh_broadcast_stats(conn, broadcast_id)


def process_next_broadcast(conn):
    now_iso = utcnow_iso()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT value FROM app_state WHERE key='broadcast_next_send_at' FOR UPDATE")
    state_row = cur.fetchone()
    if state_row and state_row["value"] and state_row["value"] > now_iso:
        cur.close()
        conn.rollback()
        return False

    cur.execute(
        """
        SELECT
            br.id,
            br.broadcast_id,
            br.phone,
            br.attempts,
            b.body,
            b.from_number
        FROM broadcast_recipients br
        JOIN broadcasts b ON b.id = br.broadcast_id
        WHERE br.status = 'pending'
          AND b.status IN ('queued', 'running')
          AND (b.next_send_at IS NULL OR b.next_send_at <= %s)
        ORDER BY COALESCE(b.next_send_at, b.created_at), b.id, br.id
        FOR UPDATE OF br, b SKIP LOCKED
        LIMIT 1
        """,
        (now_iso,)
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.rollback()
        return False

    cur.execute(
        "UPDATE broadcast_recipients SET status='sending', attempts=attempts+1, last_attempt_at=%s WHERE id=%s",
        (now_iso, row["id"])
    )
    cur.execute(
        """
        UPDATE broadcasts
        SET
            status = 'running',
            started_at = COALESCE(started_at, %s),
            next_send_at = %s
        WHERE id = %s
        """,
        (now_iso, iso_after(BROADCAST_MIN_INTERVAL_SECONDS), row["broadcast_id"])
    )
    cur.execute(
        "UPDATE app_state SET value=%s WHERE key='broadcast_next_send_at'",
        (iso_after(BROADCAST_MIN_INTERVAL_SECONDS),)
    )
    cur.close()
    conn.commit()

    try:
        ok, status_code, data, error_text = send_telnyx_message(row["phone"], row["from_number"] or MY_NUMBER, row["body"])
    except requests.RequestException as exc:
        ok, status_code, data, error_text = False, None, {}, str(exc)

    sent_at = utcnow_iso()
    payload = data.get("data") or {}
    cost_data = extract_telnyx_cost(payload)
    timing_data = extract_telnyx_timing(payload)
    provider_message_id = payload.get("id", "")
    provider_status = (payload.get("to") or [{}])[0].get("status", "sent")
    error_code, api_error_detail = extract_telnyx_error(data)
    error_text = api_error_detail or error_text

    cur = conn.cursor()
    if ok:
        insert_message_record(
            conn,
            "outbound",
            row["phone"],
            row["from_number"] or MY_NUMBER,
            row["body"],
            provider_status,
            sent_at,
            provider_message_id=provider_message_id,
            cost_payload=payload,
            broadcast_id=row["broadcast_id"],
            broadcast_recipient_id=row["id"],
        )
        cur.execute(
            """
            UPDATE broadcast_recipients
            SET
                status = %s,
                provider_message_id = %s,
                sent_at = %s,
                cost_amount = COALESCE(%s, cost_amount),
                cost_currency = COALESCE(%s, cost_currency),
                cost_carrier_fee = COALESCE(%s, cost_carrier_fee),
                cost_rate = COALESCE(%s, cost_rate),
                parts = CASE WHEN %s > 0 THEN %s ELSE parts END,
                provider_received_at = COALESCE(%s, provider_received_at),
                provider_sent_at = COALESCE(%s, provider_sent_at),
                provider_completed_at = COALESCE(%s, provider_completed_at),
                provider_valid_until = COALESCE(%s, provider_valid_until),
                provider_wait_seconds = COALESCE(%s, provider_wait_seconds)
            WHERE id = %s
            """,
            (
                provider_status,
                provider_message_id or None,
                sent_at,
                cost_data["cost_amount"],
                cost_data["cost_currency"],
                cost_data["cost_carrier_fee"],
                cost_data["cost_rate"],
                cost_data["parts"],
                cost_data["parts"],
                timing_data["provider_received_at"],
                timing_data["provider_sent_at"],
                timing_data["provider_completed_at"],
                timing_data["provider_valid_until"],
                timing_data["provider_wait_seconds"],
                row["id"],
            )
        )
    else:
        retryable = status_code in (429, 500, 502, 503, 504) or status_code is None
        should_retry = retryable and (row["attempts"] + 1) < MAX_BROADCAST_RETRIES
        recipient_status = "pending" if should_retry else "failed"
        finalized_at = None if should_retry else sent_at
        cur.execute(
            """
            UPDATE broadcast_recipients
            SET
                status = %s,
                finalized_at = COALESCE(%s, finalized_at),
                last_error_code = COALESCE(NULLIF(%s, ''), last_error_code),
                last_error_detail = COALESCE(NULLIF(%s, ''), last_error_detail)
            WHERE id = %s
            """,
            (recipient_status, finalized_at, error_code, error_text, row["id"])
        )

    cur.close()
    refresh_broadcast_stats(conn, row["broadcast_id"])
    conn.commit()
    return True


def broadcast_worker_loop():
    while True:
        conn = None
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor()
            cur.execute("SELECT pg_try_advisory_lock(%s)", (BROADCAST_WORKER_LOCK_ID,))
            is_leader = cur.fetchone()[0]
            cur.close()
            conn.commit()

            if not is_leader:
                conn.close()
                time.sleep(10)
                continue

            while True:
                recover_stuck_recipients(conn)
                conn.commit()
                worked = process_next_broadcast(conn)
                if not worked:
                    time.sleep(2)
        except Exception:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            time.sleep(5)
        finally:
            if conn is not None and not conn.closed:
                conn.close()


def start_broadcast_worker():
    global _broadcast_worker
    if not DATABASE_URL:
        return

    with _broadcast_worker_guard:
        if _broadcast_worker and _broadcast_worker.is_alive():
            return
        _broadcast_worker = threading.Thread(target=broadcast_worker_loop, daemon=True, name="broadcast-worker")
        _broadcast_worker.start()


@app.before_request
def require_login():
    if not AUTH_ENABLED:
        return None

    if request.endpoint in {"login", "logout", "static"}:
        return None

    if request.path == "/webhook/telnyx":
        return None

    if session.get("authenticated"):
        return None

    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "auth_required"}), 401

    return redirect(url_for("login", next=request.path))


# ═══════════════════════════════════════════════════════
#  DB helpers
# ═══════════════════════════════════════════════════════

def get_db():
    db = getattr(g, "_database", None)
    if db is None or db.closed:
        db = g._database = psycopg2.connect(DATABASE_URL)
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None and not db.closed:
        db.close()


def init_db():
    with app.app_context():
        db = get_db()
        cur = db.cursor()

        # Основная таблица сообщений
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id         SERIAL PRIMARY KEY,
                direction  TEXT NOT NULL,
                contact    TEXT NOT NULL,
                my_number  TEXT NOT NULL,
                body       TEXT NOT NULL,
                status     TEXT DEFAULT 'delivered',
                created_at TEXT NOT NULL
            )
        """)

        # Таблица рассылок
        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcasts (
                id         SERIAL PRIMARY KEY,
                name       TEXT NOT NULL,
                body       TEXT NOT NULL,
                total      INTEGER DEFAULT 0,
                sent       INTEGER DEFAULT 0,
                failed     INTEGER DEFAULT 0,
                status     TEXT DEFAULT 'running',
                created_at TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_recipients (
                id                 SERIAL PRIMARY KEY,
                broadcast_id       INTEGER NOT NULL REFERENCES broadcasts(id) ON DELETE CASCADE,
                phone              TEXT NOT NULL,
                status             TEXT DEFAULT 'pending',
                attempts           INTEGER DEFAULT 0,
                provider_message_id TEXT,
                cost_amount        NUMERIC(12, 6),
                cost_currency      TEXT DEFAULT 'USD',
                cost_carrier_fee   NUMERIC(12, 6),
                cost_rate          NUMERIC(12, 6),
                parts              INTEGER DEFAULT 0,
                last_error_code    TEXT DEFAULT '',
                last_error_detail  TEXT DEFAULT '',
                queued_at          TEXT NOT NULL,
                last_attempt_at    TEXT,
                sent_at            TEXT,
                finalized_at       TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS number_lookup_cache (
                normalized_phone TEXT PRIMARY KEY,
                accepted         BOOLEAN NOT NULL,
                decision         TEXT NOT NULL,
                reason           TEXT NOT NULL,
                reason_display   TEXT NOT NULL,
                line_type        TEXT DEFAULT '',
                carrier_name     TEXT DEFAULT '',
                country_code     TEXT DEFAULT '',
                source           TEXT DEFAULT '',
                error_text       TEXT DEFAULT '',
                raw_payload      TEXT DEFAULT '{}',
                checked_at       TEXT NOT NULL,
                expires_at       TEXT NOT NULL
            )
        """)

        # Таблица контактов (имя, компания, теги, заметки)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id         SERIAL PRIMARY KEY,
                phone      TEXT UNIQUE NOT NULL,
                name       TEXT DEFAULT '',
                company    TEXT DEFAULT '',
                tags       TEXT DEFAULT '',
                notes      TEXT DEFAULT '',
                updated_at TEXT NOT NULL
            )
        """)

        # Индекс для быстрого поиска по тексту
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS provider_message_id TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS cost_amount NUMERIC(12, 6)")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS cost_currency TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS cost_carrier_fee NUMERIC(12, 6)")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS cost_rate NUMERIC(12, 6)")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS parts INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS finalized_at TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS error_code TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS error_detail TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS broadcast_id INTEGER")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS broadcast_recipient_id INTEGER")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS provider_received_at TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS provider_sent_at TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS provider_completed_at TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS provider_valid_until TEXT")
        cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS provider_wait_seconds NUMERIC(12, 3)")

        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS from_number TEXT DEFAULT ''")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS delivered INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS delivery_failed INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS delivery_unconfirmed INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS finalized INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS cost_amount NUMERIC(12, 6) DEFAULT 0")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS cost_currency TEXT DEFAULT 'USD'")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS delivery_rate NUMERIC(7, 2)")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS next_send_at TEXT")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS started_at TEXT")
        cur.execute("ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS finished_at TEXT")
        cur.execute("ALTER TABLE broadcast_recipients ADD COLUMN IF NOT EXISTS provider_received_at TEXT")
        cur.execute("ALTER TABLE broadcast_recipients ADD COLUMN IF NOT EXISTS provider_sent_at TEXT")
        cur.execute("ALTER TABLE broadcast_recipients ADD COLUMN IF NOT EXISTS provider_completed_at TEXT")
        cur.execute("ALTER TABLE broadcast_recipients ADD COLUMN IF NOT EXISTS provider_valid_until TEXT")
        cur.execute("ALTER TABLE broadcast_recipients ADD COLUMN IF NOT EXISTS provider_wait_seconds NUMERIC(12, 3)")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS line_type TEXT DEFAULT ''")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS carrier_name TEXT DEFAULT ''")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS country_code TEXT DEFAULT ''")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS source TEXT DEFAULT ''")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS error_text TEXT DEFAULT ''")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS raw_payload TEXT DEFAULT '{}'")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS reason_display TEXT DEFAULT ''")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS checked_at TEXT DEFAULT ''")
        cur.execute("ALTER TABLE number_lookup_cache ADD COLUMN IF NOT EXISTS expires_at TEXT DEFAULT ''")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_body ON messages USING gin(to_tsvector('simple', body))")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_contact ON messages(contact)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_provider_id ON messages(provider_message_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_broadcast_id ON messages(broadcast_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_broadcast_recipients_broadcast_id ON broadcast_recipients(broadcast_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_broadcast_recipients_status ON broadcast_recipients(status)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_broadcast_recipients_provider_id ON broadcast_recipients(provider_message_id)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_broadcast_recipients_phone_per_broadcast ON broadcast_recipients(broadcast_id, phone)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_number_lookup_cache_expires_at ON number_lookup_cache(expires_at)")
        cur.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES ('broadcast_next_send_at', %s)
            ON CONFLICT (key) DO NOTHING
            """,
            (utcnow_iso(),)
        )

        db.commit()
        cur.close()


# ═══════════════════════════════════════════════════════
#  Webhook Telnyx
# ═══════════════════════════════════════════════════════

@app.route("/webhook/telnyx", methods=["POST"])
def telnyx_webhook():
    data = request.get_json(silent=True) or {}
    event_type = data.get("data", {}).get("event_type", "")

    if event_type == "message.received":
        payload = data["data"]["payload"]
        from_num = payload.get("from", {}).get("phone_number", "")
        to_num = payload.get("to", [{}])[0].get("phone_number", MY_NUMBER)
        body = payload.get("text", "")
        db = get_db()
        insert_message_record(
            db,
            "inbound",
            from_num,
            to_num,
            body,
            "received",
            utcnow_iso(),
            provider_message_id=payload.get("id", ""),
            cost_payload=payload,
        )
        db.commit()

    elif event_type in ("message.sent", "message.finalized"):
        payload = data["data"]["payload"]
        db = get_db()
        update_outbound_tracking(db, payload, event_type)
        db.commit()

    return jsonify({"ok": True}), 200


# ═══════════════════════════════════════════════════════
#  API — Contacts list
# ═══════════════════════════════════════════════════════

@app.route("/api/contacts")
def api_contacts():
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            m.contact,
            m.my_number,
            MAX(m.created_at)  AS last_at,
            SUM(CASE WHEN m.direction='inbound'  THEN 1 ELSE 0 END) AS inbound_count,
            SUM(CASE WHEN m.direction='outbound' THEN 1 ELSE 0 END) AS outbound_count,
            SUM(CASE WHEN m.direction='inbound' AND m.status='received' THEN 1 ELSE 0 END) AS unread,
            COALESCE(c.name,    '') AS name,
            COALESCE(c.company, '') AS company,
            COALESCE(c.tags,    '') AS tags
        FROM messages m
        LEFT JOIN contacts c ON c.phone = m.contact
        GROUP BY m.contact, m.my_number, c.name, c.company, c.tags
        ORDER BY last_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/messages/<contact>")
def api_messages(contact):
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT * FROM messages WHERE contact=%s ORDER BY created_at ASC",
        (contact,)
    )
    rows = cur.fetchall()
    cur.close()
    return jsonify([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════
#  API — Contact card (GET/POST)
# ═══════════════════════════════════════════════════════

@app.route("/api/contact/<path:phone>", methods=["GET"])
def api_contact_get(phone):
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM contacts WHERE phone=%s", (phone,))
    row = cur.fetchone()

    cur.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN direction='outbound' THEN 1 ELSE 0 END) AS sent,
            SUM(CASE WHEN direction='inbound'  THEN 1 ELSE 0 END) AS received,
            MIN(created_at) AS first_msg,
            MAX(created_at) AS last_msg
        FROM messages WHERE contact=%s
    """, (phone,))
    stats = cur.fetchone()
    cur.close()

    result = dict(row) if row else {
        "phone": phone, "name": "", "company": "", "tags": "", "notes": ""
    }
    result["stats"] = dict(stats) if stats else {}
    return jsonify(result)


@app.route("/api/contact/<path:phone>", methods=["POST"])
def api_contact_save(phone):
    body    = request.get_json()
    name    = body.get("name",    "")
    company = body.get("company", "")
    tags    = body.get("tags",    "")
    notes   = body.get("notes",   "")

    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO contacts (phone, name, company, tags, notes, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (phone) DO UPDATE SET
            name       = EXCLUDED.name,
            company    = EXCLUDED.company,
            tags       = EXCLUDED.tags,
            notes      = EXCLUDED.notes,
            updated_at = EXCLUDED.updated_at
    """, (phone, name, company, tags, notes, datetime.utcnow().isoformat()))
    db.commit()
    cur.close()
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════
#  API — Full-text search
# ═══════════════════════════════════════════════════════

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        "SELECT * FROM messages WHERE body ILIKE %s ORDER BY created_at DESC LIMIT 40",
        (f"%{q}%",)
    )
    rows = cur.fetchall()
    cur.close()
    return jsonify([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════
#  API — Dashboard stats
# ═══════════════════════════════════════════════════════

@app.route("/api/stats")
def api_stats():
    db  = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    cur.execute("SELECT COUNT(DISTINCT contact) AS v FROM messages")
    total_contacts = cur.fetchone()["v"]

    cur.execute("SELECT COUNT(*) AS v FROM messages WHERE direction='outbound' AND created_at LIKE %s", (f"{today}%",))
    today_sent = cur.fetchone()["v"]

    cur.execute("SELECT COUNT(*) AS v FROM messages WHERE direction='inbound' AND created_at LIKE %s", (f"{today}%",))
    today_recv = cur.fetchone()["v"]

    cur.execute("SELECT COUNT(*) AS v FROM messages WHERE direction='inbound' AND status='received'")
    unread = cur.fetchone()["v"]

    cur.execute("SELECT COUNT(*) AS v FROM messages")
    total_msgs = cur.fetchone()["v"]

    cur.close()
    return jsonify({
        "total_contacts": total_contacts,
        "today_sent":     today_sent,
        "today_recv":     today_recv,
        "unread":         unread,
        "total_msgs":     total_msgs
    })


# ═══════════════════════════════════════════════════════
#  API — Number lookup
# ═══════════════════════════════════════════════════════

@app.route("/api/number-lookup", methods=["POST"])
def api_number_lookup():
    body = request.get_json(silent=True) or {}
    numbers_raw = body.get("numbers", [])
    db = get_db()
    lookup = evaluate_numbers_for_sms(db, numbers_raw)
    db.commit()
    return jsonify({"ok": True, "lookup": lookup})


# ═══════════════════════════════════════════════════════
#  API — Send single message
# ═══════════════════════════════════════════════════════

@app.route("/api/send", methods=["POST"])
def api_send():
    body = request.get_json(silent=True) or {}
    to = body.get("to")
    text = (body.get("text") or "").strip()
    from_num = body.get("from", MY_NUMBER)
    if not text:
        return jsonify({"ok": False, "error": "Message text is required"}), 400

    db = get_db()
    lookup = assess_phone_for_sms(db, to)
    db.commit()
    if not lookup.get("accepted"):
        return jsonify({
            "ok": False,
            "error": lookup.get("reason_display", "Number rejected by lookup"),
            "lookup": sanitize_lookup_result(lookup),
        }), 400

    to_number = lookup.get("normalized_phone") or str(to or "").strip()

    try:
        ok, status_code, data, error_text = send_telnyx_message(to_number, from_num, text)
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    if ok:
        payload = data.get("data") or {}
        provider_status = (payload.get("to") or [{}])[0].get("status", "sending")
        insert_message_record(
            db,
            "outbound",
            to_number,
            from_num,
            text,
            provider_status,
            utcnow_iso(),
            provider_message_id=payload.get("id", ""),
            cost_payload=payload,
        )
        db.commit()
        return jsonify({"ok": True, "lookup": sanitize_lookup_result(lookup)})
    return jsonify({"ok": False, "error": error_text or str(data)}), 400


# ═══════════════════════════════════════════════════════
#  API — Broadcast queue
# ═══════════════════════════════════════════════════════


@app.route("/api/broadcast", methods=["POST"])
def api_broadcast():
    body = request.get_json(silent=True) or {}
    numbers_raw = body.get("numbers", [])
    text = body.get("text", "").strip()
    name = body.get("name", "Рассылка").strip()
    from_num = body.get("from", MY_NUMBER)

    db = get_db()
    lookup = evaluate_numbers_for_sms(db, numbers_raw)
    numbers = lookup["accepted_numbers"]

    if not text:
        db.commit()
        return jsonify({"ok": False, "error": "Message text is required", "lookup": lookup}), 400

    if not numbers:
        db.commit()
        return jsonify({"ok": False, "error": "No SMS-capable numbers after lookup", "lookup": lookup}), 400

    now_iso = utcnow_iso()
    cur = db.cursor()
    cur.execute(
        """
        INSERT INTO broadcasts (
            name, body, total, sent, failed, status, created_at, from_number, next_send_at
        )
        VALUES (%s,%s,%s,0,0,'queued',%s,%s,%s)
        RETURNING id
        """,
        (name, text, len(numbers), now_iso, from_num, now_iso)
    )
    broadcast_id = cur.fetchone()[0]
    cur.executemany(
        """
        INSERT INTO broadcast_recipients (broadcast_id, phone, status, attempts, queued_at)
        VALUES (%s,%s,'pending',0,%s)
        ON CONFLICT (broadcast_id, phone) DO NOTHING
        """,
        [(broadcast_id, number, now_iso) for number in numbers]
    )
    db.commit()
    cur.close()

    return jsonify({
        "ok": True,
        "broadcast_id": broadcast_id,
        "total": len(numbers),
        "lookup": lookup,
    })


@app.route("/api/broadcast/<int:bid>")
def api_broadcast_status(bid):
    db = get_db()
    refresh_broadcast_stats(db, bid)
    db.commit()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM broadcasts WHERE id=%s", (bid,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return jsonify({"ok": False}), 404
    result = dict(row)
    result["diagnostics"] = get_broadcast_diagnostics(db, bid)
    return jsonify(result)


@app.route("/api/broadcasts")
def api_broadcasts():
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT id FROM broadcasts ORDER BY created_at DESC LIMIT 20")
    broadcast_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    for broadcast_id in broadcast_ids:
        refresh_broadcast_stats(db, broadcast_id)
    db.commit()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT 20")
    rows = cur.fetchall()
    cur.close()
    return jsonify([dict(r) for r in rows])


@app.route("/login", methods=["GET", "POST"])
def login():
    if not AUTH_ENABLED:
        return redirect(url_for("index"))

    next_url = _safe_next_url(request.values.get("next"))

    if session.get("authenticated"):
        return redirect(next_url)

    error = ""
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if hmac.compare_digest(username, APP_USERNAME) and hmac.compare_digest(password, APP_PASSWORD):
            session.clear()
            session["authenticated"] = True
            return redirect(next_url)
        error = "Неверный логин или пароль"

    return render_template("login.html", error=error, next_url=next_url)


@app.route("/logout")
def logout():
    session.clear()
    if AUTH_ENABLED:
        return redirect(url_for("login"))
    return redirect(url_for("index"))


# ═══════════════════════════════════════════════════════
#  UI
# ═══════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html", my_number=MY_NUMBER, auth_enabled=AUTH_ENABLED)


# Инициализация при старте (создаёт таблицы если их нет)
init_db()
start_broadcast_worker()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
