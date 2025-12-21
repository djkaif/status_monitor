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

# Settings moved to Central
OFFLINE_THRESHOLD = int(os.getenv("OFFLINE_THRESHOLD", 90))
BUFFER_ROTATE_SECONDS = 30 * 60 
ROTATE_CHECK_INTERVAL = 30

# ================== APP ==================

app = Flask(__name__)
db_lock = Lock()
current_batch_id = None
event_queue = [] # Temporary store for pending alerts

# ================== DB SETUP ==================

def init_dbs():
    # Delete databases on start as requested
    for f in [BUFFER_DB, ARCHIVE_DB]:
        if os.path.exists(f):
            os.remove(f)
            print(f"[CENTRAL] Deleted old {f}")

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
        # Table to track current state for offline detection
        db.execute("""
        CREATE TABLE IF NOT EXISTS node_status (
            node TEXT PRIMARY KEY,
            node_type TEXT,
            last_seen INTEGER,
            status TEXT DEFAULT 'online'
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
        # 1. Archive the raw heartbeat
        try:
            db.execute(
                "INSERT INTO heartbeats (node, node_type, received_at, seq) VALUES (?, ?, ?, ?)",
                (node, node_type, now, seq)
            )
        except sqlite3.IntegrityError:
            pass 

        # 2. Check for Recovery (Offline -> Online)
        row = db.execute("SELECT status FROM node_status WHERE node=?", (node,)).fetchone()
        if row and row[0] == "offline":
            event_queue.append({"node": node, "type": "online", "ts": now})
            print(f"[CENTRAL] Node {node} recovered!")

        # 3. Update Status
        db.execute("""
            INSERT INTO node_status (node, node_type, last_seen, status) 
            VALUES (?, ?, ?, 'online')
            ON CONFLICT(node) DO UPDATE SET last_seen=excluded.last_seen, status='online'
        """, (node, node_type, now))
        db.commit()

    return jsonify({"ok": True})

# ================== BACKGROUND MONITOR ==================

def monitor_nodes():
    """Background thread to detect nodes that stop sending heartbeats"""
    while True:
        time.sleep(10)
        now = int(time.time())
        with db_lock, sqlite3.connect(BUFFER_DB) as db:
            # Find nodes that crossed the threshold and are still marked 'online'
            offline_nodes = db.execute("""
                SELECT node FROM node_status 
                WHERE status = 'online' AND (? - last_seen) > ?
            """, (now, OFFLINE_THRESHOLD)).fetchall()

            for (node,) in offline_nodes:
                db.execute("UPDATE node_status SET status='offline' WHERE node=?", (node,))
                event_queue.append({"node": node, "type": "offline", "ts": now})
                print(f"[CENTRAL] Node {node} timed out")
            db.commit()

def buffer_rotator():
    """Moves data to Archive DB every 30 mins"""
    while True:
        time.sleep(ROTATE_CHECK_INTERVAL)
        with db_lock, sqlite3.connect(BUFFER_DB) as buf_db, sqlite3.connect(ARCHIVE_DB) as arc_db:
            cur = buf_db.execute("SELECT MIN(received_at) FROM heartbeats")
            row = cur.fetchone()
            if not row or not row[0] or (int(time.time()) - row[0] < BUFFER_ROTATE_SECONDS):
                continue

            rows = buf_db.execute("SELECT node, node_type, received_at, seq FROM heartbeats").fetchall()
            arc_db.executemany("INSERT OR IGNORE INTO heartbeats_archive VALUES (NULL,?,?,?,?)", rows)
            arc_db.commit()
            buf_db.execute("DELETE FROM heartbeats")
            buf_db.commit()

# ================== BOT ENDPOINTS ==================

@app.route("/events", methods=["GET"])
def get_events():
    """Bot polls this for instant alerts"""
    require_api_key()
    global event_queue
    response = list(event_queue)
    event_queue = [] # Clear after sending
    return jsonify(response)

@app.route("/archive/pull", methods=["GET"])
def pull_archive():
    require_api_key()
    global current_batch_id
    with db_lock, sqlite3.connect(ARCHIVE_DB) as db:
        rows = db.execute("SELECT node, node_type, received_at, seq FROM heartbeats_archive").fetchall()
    
    if not rows: return jsonify({"count": 0, "data": []})
    
    current_batch_id = str(uuid.uuid4())
    return jsonify({
        "batch_id": current_batch_id,
        "count": len(rows),
        "data": [{"node": r[0], "node_type": r[1], "received_at": r[2], "seq": r[3]} for r in rows]
    })

@app.route("/archive/ack", methods=["POST"])
def archive_ack():
    require_api_key()
    global current_batch_id
    data = request.json or {}
    if data.get("batch_id") != current_batch_id:
        return jsonify({"error": "invalid batch"}), 400

    with db_lock, sqlite3.connect(ARCHIVE_DB) as db:
        db.execute("DELETE FROM heartbeats_archive")
        db.commit()
    current_batch_id = None
    return jsonify({"ok": True})

if __name__ == "__main__":
    init_dbs()
    Thread(target=monitor_nodes, daemon=True).start()
    Thread(target=buffer_rotator, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
