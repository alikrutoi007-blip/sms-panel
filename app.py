import os
import psycopg2
import psycopg2.extras
import requests
import time
import threading
from datetime import datetime
from flask import Flask, request, jsonify, render_template, g

app = Flask(__name__)

TELNYX_API_KEY = os.environ.get("TELNYX_API_KEY", "YOUR_API_KEY")
MY_NUMBER     = os.environ.get("MY_NUMBER", "+15512300914")
_DATABASE_URL  = os.environ.get("DATABASE_URL", "")

# Railway даёт "postgres://..." — psycopg2 требует "postgresql://..."
DATABASE_URL = _DATABASE_URL.replace("postgres://", "postgresql://", 1) if _DATABASE_URL.startswith("postgres://") else _DATABASE_URL


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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_body ON messages USING gin(to_tsvector('simple', body))")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_contact ON messages(contact)")

        db.commit()
        cur.close()


# ═══════════════════════════════════════════════════════
#  Webhook Telnyx
# ═══════════════════════════════════════════════════════

@app.route("/webhook/telnyx", methods=["POST"])
def telnyx_webhook():
    data       = request.get_json(silent=True) or {}
    event_type = data.get("data", {}).get("event_type", "")

    if event_type == "message.received":
        payload  = data["data"]["payload"]
        from_num = payload.get("from", {}).get("phone_number", "")
        to_num   = payload.get("to", [{}])[0].get("phone_number", MY_NUMBER)
        body     = payload.get("text", "")
        db = get_db()
        cur = db.cursor()
        cur.execute(
            "INSERT INTO messages (direction, contact, my_number, body, status, created_at)"
            " VALUES (%s,%s,%s,%s,%s,%s)",
            ("inbound", from_num, to_num, body, "received", datetime.utcnow().isoformat())
        )
        db.commit()
        cur.close()

    elif event_type in ("message.sent", "message.finalized"):
        payload  = data["data"]["payload"]
        to_num   = payload.get("to", [{}])[0].get("phone_number", "")
        status   = payload.get("to", [{}])[0].get("status", "sent")
        telnyx_id = payload.get("id", "")
        if to_num and telnyx_id:
            db = get_db()
            cur = db.cursor()
            cur.execute(
                "UPDATE messages SET status=%s"
                " WHERE contact=%s AND direction='outbound' AND status='sending'"
                " AND id=(SELECT MAX(id) FROM messages WHERE contact=%s AND direction='outbound')",
                (status, to_num, to_num)
            )
            db.commit()
            cur.close()

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
#  API — Send single message
# ═══════════════════════════════════════════════════════

@app.route("/api/send", methods=["POST"])
def api_send():
    body     = request.get_json()
    to       = body.get("to")
    text     = body.get("text")
    from_num = body.get("from", MY_NUMBER)

    resp = requests.post(
        "https://api.telnyx.com/v2/messages",
        headers={
            "Authorization": f"Bearer {TELNYX_API_KEY}",
            "Content-Type":  "application/json"
        },
        json={"from": from_num, "to": to, "text": text}
    )

    if resp.status_code in (200, 201):
        db  = get_db()
        cur = db.cursor()
        cur.execute(
            "INSERT INTO messages (direction, contact, my_number, body, status, created_at)"
            " VALUES (%s,%s,%s,%s,%s,%s)",
            ("outbound", to, from_num, text, "sending", datetime.utcnow().isoformat())
        )
        db.commit()
        cur.close()
        return jsonify({"ok": True})
    else:
        return jsonify({"ok": False, "error": resp.text}), 400


# ═══════════════════════════════════════════════════════
#  API — Broadcast (background thread)
# ═══════════════════════════════════════════════════════

def _run_broadcast(broadcast_id, numbers, text, from_num):
    """Runs in a daemon thread — uses its own DB connection."""
    conn  = psycopg2.connect(DATABASE_URL)
    sent  = 0
    failed = 0

    for num in numbers:
        try:
            resp = requests.post(
                "https://api.telnyx.com/v2/messages",
                headers={
                    "Authorization": f"Bearer {TELNYX_API_KEY}",
                    "Content-Type":  "application/json"
                },
                json={"from": from_num, "to": num, "text": text},
                timeout=10
            )
            cur = conn.cursor()
            if resp.status_code in (200, 201):
                sent += 1
                cur.execute(
                    "INSERT INTO messages (direction, contact, my_number, body, status, created_at)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    ("outbound", num, from_num, text, "sending", datetime.utcnow().isoformat())
                )
            else:
                failed += 1
            cur.execute(
                "UPDATE broadcasts SET sent=%s, failed=%s WHERE id=%s",
                (sent, failed, broadcast_id)
            )
            conn.commit()
            cur.close()
        except Exception:
            failed += 1

        time.sleep(0.15)   # ~6 msg/sec — не перегружаем Telnyx

    cur = conn.cursor()
    cur.execute(
        "UPDATE broadcasts SET sent=%s, failed=%s, status='done' WHERE id=%s",
        (sent, failed, broadcast_id)
    )
    conn.commit()
    cur.close()
    conn.close()


@app.route("/api/broadcast", methods=["POST"])
def api_broadcast():
    body       = request.get_json()
    numbers_raw = body.get("numbers", [])
    text       = body.get("text",   "").strip()
    name       = body.get("name",   "Рассылка").strip()
    from_num   = body.get("from",   MY_NUMBER)

    # Дедупликация + валидация формата
    numbers = list({
        n.strip() for n in numbers_raw
        if n.strip() and n.strip().startswith("+")
    })

    if not numbers or not text:
        return jsonify({"ok": False, "error": "Нет номеров или текста"}), 400

    db  = get_db()
    cur = db.cursor()
    cur.execute(
        "INSERT INTO broadcasts (name, body, total, sent, failed, status, created_at)"
        " VALUES (%s,%s,%s,0,0,'running',%s) RETURNING id",
        (name, text, len(numbers), datetime.utcnow().isoformat())
    )
    broadcast_id = cur.fetchone()[0]
    db.commit()
    cur.close()

    t = threading.Thread(target=_run_broadcast, args=(broadcast_id, numbers, text, from_num), daemon=True)
    t.start()

    return jsonify({"ok": True, "broadcast_id": broadcast_id, "total": len(numbers)})


@app.route("/api/broadcast/<int:bid>")
def api_broadcast_status(bid):
    db  = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM broadcasts WHERE id=%s", (bid,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return jsonify({"ok": False}), 404
    return jsonify(dict(row))


@app.route("/api/broadcasts")
def api_broadcasts():
    db  = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT 20")
    rows = cur.fetchall()
    cur.close()
    return jsonify([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════
#  UI
# ═══════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html", my_number=MY_NUMBER)


# Инициализация при старте (создаёт таблицы если их нет)
init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
