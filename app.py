import os
import sqlite3
import requests
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
        from_num = payload.get("from", {}).get("phone_number", MY_NUMBER)
        status = payload.get("to", [{}])[0].get("status", "sent")
        telnyx_id = payload.get("id", "")
        if to_num and telnyx_id:
            db = get_db()
            db.execute(
                "UPDATE messages SET status=? WHERE contact=? AND direction='outbound' AND status='sending' AND id=(SELECT MAX(id) FROM messages WHERE contact=? AND direction='outbound')",
                (status, to_num, to_num)
            )
            db.commit()

    return jsonify({"ok": True}), 200


# ---------- API ----------

@app.route("/api/contacts")
def api_contacts():
    db = get_db()
    rows = db.execute("""
        SELECT contact, my_number,
               MAX(created_at) as last_at,
               SUM(CASE WHEN direction='inbound' THEN 1 ELSE 0 END) as unread
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


# ---------- UI ----------

@app.route("/")
def index():
    return render_template("index.html", my_number=MY_NUMBER)


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
