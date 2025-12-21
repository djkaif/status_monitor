import os
import sqlite3
import time
import uuid
from flask import Flask, request, jsonify, abort
from threading import Thread, Lock
from dotenv import load_dotenv

load_dotenv()

# ================== CONFIG ==================

CENTRAL_API_KEY = os.getenv("CENTRAL_API_KEY")
BUFFER_DB = "buffer.db"
ARCHIVE_DB = "archive.db"

BUFFER_ROTATE_SECONDS = 30 * 60  # 30 minutes
ROTATE_CHECK_INTERVAL = 60       # every 60 seconds

# ================== APP ==================

app = Flask(__name__)
db_lock = Lock()
current_batch_id = None

# ================== DB SETUP ==================

def init_dbs():
    with sqlite3.connect(BUFFER_DB) as db:
        db.execute("""
        CREATE TABLE IF NOT EXISTS heartbeats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node TEXT NOT NULL,
            node_type TEXT NOT NULL,
            received_at INTEGER NOT NULL,
            seq TEXT UNIQUE
        )
        """)
        db.commit()

    with sqlite3.connect(ARCHIVE_DB) as db:
        db.execute("""
        CREATE TABLE IF NOT EXISTS heartbeats_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node TEXT NOT NULL,
            node_type TEXT NOT NULL,
            received_at INTEGER NOT NULL,
            seq TEXT UNIQUE
        )
        """)
        db.commit()

    print("[CENTRAL] Databases initialized")

# ================== AUTH ==================

def require_api_key():
    key = request.headers.get("X-API-Key")
    if key != CENTRAL_API_KEY:
        abort(401)

# ================== HEARTBEAT ENDPOINT ==================

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    require_api_key()

    data = request.json or {}
    node = data.get("node")
    node_type = data.get("node_type")

    if not node or not node_type:
        return jsonify({"error": "invalid payload"}), 400

    now = int(time.time())
    seq = f"{node}:{now}"

    with db_lock, sqlite3.connect(BUFFER_DB) as db:
        try:
            db.execute(
                "INSERT INTO heartbeats (node, node_type, received_at, seq) VALUES (?, ?, ?, ?)",
                (node, node_type, now, seq)
            )
            db.commit()
            print(f"[CENTRAL] Heartbeat stored → {node}")
        except sqlite3.IntegrityError:
            print(f"[CENTRAL] Duplicate heartbeat ignored → {node}")

    return jsonify({"ok": True})

# ================== BUFFER ROTATION ==================

def buffer_rotator():
    while True:
        time.sleep(ROTATE_CHECK_INTERVAL)
        try:
            with db_lock, sqlite3.connect(BUFFER_DB) as buf_db, sqlite3.connect(ARCHIVE_DB) as arc_db:
                cur = buf_db.execute("SELECT MIN(received_at) FROM heartbeats")
                row = cur.fetchone()

                if not row or not row[0]:
                    continue

                oldest = row[0]
                if int(time.time()) - oldest < BUFFER_ROTATE_SECONDS:
                    continue

                rows = buf_db.execute(
                    "SELECT node, node_type, received_at, seq FROM heartbeats"
                ).fetchall()

                if not rows:
                    continue

                arc_db.executemany(
                    "INSERT OR IGNORE INTO heartbeats_archive (node, node_type, received_at, seq) VALUES (?, ?, ?, ?)",
                    rows
                )
                arc_db.commit()

                buf_db.execute("DELETE FROM heartbeats")
                buf_db.commit()

                print(f"[CENTRAL] Buffer rotated → {len(rows)} rows archived")

        except Exception as e:
            print("[CENTRAL] Rotation error:", e)

# ================== BOT PULL ==================

@app.route("/archive/pull", methods=["GET"])
def pull_archive():
    require_api_key()
    global current_batch_id

    with db_lock, sqlite3.connect(ARCHIVE_DB) as db:
        rows = db.execute(
            "SELECT node, node_type, received_at, seq FROM heartbeats_archive"
        ).fetchall()

    if not rows:
        return jsonify({"count": 0, "data": []})

    current_batch_id = str(uuid.uuid4())
    print(f"[CENTRAL] Bot connected → sending batch {current_batch_id}")

    return jsonify({
        "batch_id": current_batch_id,
        "count": len(rows),
        "data": [
            {
                "node": r[0],
                "node_type": r[1],
                "received_at": r[2],
                "seq": r[3]
            } for r in rows
        ]
    })

# ================== BOT ACK ==================

@app.route("/archive/ack", methods=["POST"])
def archive_ack():
    require_api_key()
    global current_batch_id

    data = request.json or {}
    batch_id = data.get("batch_id")

    if not batch_id or batch_id != current_batch_id:
        return jsonify({"error": "invalid batch"}), 400

    with db_lock, sqlite3.connect(ARCHIVE_DB) as db:
        db.execute("DELETE FROM heartbeats_archive")
        db.commit()

    print(f"[CENTRAL] Batch {batch_id} acknowledged → archive cleared")
    current_batch_id = None
    return jsonify({"ok": True})

# ================== HEALTH ==================

@app.route("/")
def health():
    return "Central server running", 200

# ================== MAIN ==================

if __name__ == "__main__":
    if not CENTRAL_API_KEY:
        raise RuntimeError("CENTRAL_API_KEY not set")

    init_dbs()
    Thread(target=buffer_rotator, daemon=True).start()
    print("[CENTRAL] Server online – waiting for agents and bot")

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
