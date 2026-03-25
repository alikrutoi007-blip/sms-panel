import os
import sqlite3
import requests
import time
import threading
from datetime import datetime
from flask import Flask, request, jsonify, render_template, g

app = Flask(__name__)

TELNYX_API_KEY = os.environ.get("TELNYX_API_KEY", "YOUR_API_KEY")
MY_NUMBER = os.environ.get("MY_NUMBER", "+15512300914")
DATABASE = "sms.db"

# ---------- DB ----------

def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


def init_db():
    with app.app_context():
        db = get_db()
        db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT NOT NULL,
                contact TEXT NOT NULL,
                my_number TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT DEFAULT 'delivered',
                created_at TEXT NOT NULL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                body TEXT NOT NULL,
                total INTEGER DEFAULT 0,
                sent INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                created_at TEXT NOT NULL
            )
        """)
        db.commit()


# ---------- Webhook ----------

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
        db.execute(
            "INSERT INTO messages (direction, contact, my_number, body, status, created_at) VALUES (?,?,?,?,?,?)",
            ("inbound", from_num, to_num, body, "received", datetime.utcnow().isoformat())
        )
        db.commit()

    elif event_type in ("message.sent", "message.finalized"):
        payload = data["data"]["payload"]
        to_num = payload.get("to", [{}])[0].get("phone_number", "")
        status = payload.get("to", [{}])[0].get("status", "sent")
        telnyx_id = payload.get("id", "")
        if to_num and telnyx_id:
            db = get_db()
            db.execute(
                "UPDATE messages SET status=? WHERE contact=? AND direction='outbound' AND status='sending' "
                "AND id=(SELECT MAX(id) FROM messages WHERE contact=? AND direction='outbound')",
                (status, to_num, to_num)
            )
            db.commit()

    return jsonify({"ok": True}), 200


# ---------- API: Contacts ----------

@app.route("/api/contacts")
def api_contacts():
    db = get_db()
    rows = db.execute("""
        SELECT
            contact,
            my_number,
            MAX(created_at) as last_at,
            SUM(CASE WHEN direction='inbound' THEN 1 ELSE 0 END)  as inbound_count,
            SUM(CASE WHEN direction='outbound' THEN 1 ELSE 0 END) as outbound_count,
            SUM(CASE WHEN direction='inbound' AND status='received' THEN 1 ELSE 0 END) as unread
        FROM messages
        GROUP BY contact
        ORDER BY last_at DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/messages/<contact>")
def api_messages(contact):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM messages WHERE contact=? ORDER BY created_at ASC",
        (contact,)
    ).fetchall()
    return jsonify([dict(r) for r in rows])


# ---------- API: Send ----------

@app.route("/api/send", methods=["POST"])
def api_send():
    body = request.get_json()
    to = body.get("to")
    text = body.get("text")
    from_num = body.get("from", MY_NUMBER)

    resp = requests.post(
        "https://api.telnyx.com/v2/messages",
        headers={
            "Authorization": f"Bearer {TELNYX_API_KEY}",
            "Content-Type": "application/json"
        },
        json={"from": from_num, "to": to, "text": text}
    )

    if resp.status_code in (200, 201):
        db = get_db()
        db.execute(
            "INSERT INTO messages (direction, contact, my_number, body, status, created_at) VALUES (?,?,?,?,?,?)",
            ("outbound", to, from_num, text, "sending", datetime.utcnow().isoformat())
        )
        db.commit()
        return jsonify({"ok": True})
    else:
        return jsonify({"ok": False, "error": resp.text}), 400


# ---------- API: Broadcast ----------

def _run_broadcast(broadcast_id, numbers, text, from_num):
    sent = 0
    failed = 0

    for num in numbers:
        try:
            resp = requests.post(
                "https://api.telnyx.com/v2/messages",
                headers={
                    "Authorization": f"Bearer {TELNYX_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={"from": from_num, "to": num, "text": text},
                timeout=10
            )
            with app.app_context():
                db = get_db()
                if resp.status_code in (200, 201):
                    sent += 1
                    db.execute(
                        "INSERT INTO messages (direction, contact, my_number, body, status, created_at) VALUES (?,?,?,?,?,?)",
                        ("outbound", num, from_num, text, "sending", datetime.utcnow().isoformat())
                    )
                else:
                    failed += 1
                db.execute(
                    "UPDATE broadcasts SET sent=?, failed=? WHERE id=?",
                    (sent, failed, broadcast_id)
                )
                db.commit()
        except Exception:
            failed += 1

        time.sleep(0.15)

    with app.app_context():
        db = get_db()
        db.execute(
            "UPDATE broadcasts SET sent=?, failed=?, status='done' WHERE id=?",
            (sent, failed, broadcast_id)
        )
        db.commit()


@app.route("/api/broadcast", methods=["POST"])
def api_broadcast():
    body = request.get_json()
    numbers_raw = body.get("numbers", [])
    text = body.get("text", "").strip()
    name = body.get("name", "Рассылка").strip()
    from_num = body.get("from", MY_NUMBER)

    numbers = list({
        n.strip() for n in numbers_raw
        if n.strip() and n.strip().startswith("+")
    })

    if not numbers or not text:
        return jsonify({"ok": False, "error": "Нет номеров или текста"}), 400

    db = get_db()
    cur = db.execute(
        "INSERT INTO broadcasts (name, body, total, sent, failed, status, created_at) VALUES (?,?,?,0,0,'running',?)",
        (name, text, len(numbers), datetime.utcnow().isoformat())
    )
    db.commit()
    broadcast_id = cur.lastrowid

    t = threading.Thread(target=_run_broadcast, args=(broadcast_id, numbers, text, from_num), daemon=True)
    t.start()

    return jsonify({"ok": True, "broadcast_id": broadcast_id, "total": len(numbers)})


@app.route("/api/broadcast/<int:bid>")
def api_broadcast_status(bid):
    db = get_db()
    row = db.execute("SELECT * FROM broadcasts WHERE id=?", (bid,)).fetchone()
    if not row:
        return jsonify({"ok": False}), 404
    return jsonify(dict(row))


@app.route("/api/broadcasts")
def api_broadcasts():
    db = get_db()
    rows = db.execute("SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT 20").fetchall()
    return jsonify([dict(r) for r in rows])


# ---------- UI ----------

@app.route("/")
def index():
    return render_template("index.html", my_number=MY_NUMBER)


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
