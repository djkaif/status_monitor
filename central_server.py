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

# Settings
OFFLINE_THRESHOLD = int(os.getenv("OFFLINE_THRESHOLD", 90))
BUFFER_ROTATE_SECONDS = 10 * 60  # 30 minutes
ROTATE_CHECK_INTERVAL = 60       # Check rotation every 30s

# ================== APP STATE ==================

app = Flask(__name__)
db_lock = Lock()
current_batch_id = None
event_queue = []  # Stores alerts for the bot

# ================== DB SETUP ==================

def init_dbs():
    # CLEAN START: Delete old databases on startup
    for f in [BUFFER_DB, ARCHIVE_DB]:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"[CENTRAL] Deleted old {f}")
            except Exception as e:
                print(f"[CENTRAL] Failed to delete {f}: {e}")

    with sqlite3.connect(BUFFER_DB) as db:
        # Raw heartbeats (temporary storage)
        db.execute("""
        CREATE TABLE IF NOT EXISTS heartbeats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node TEXT NOT NULL,
            node_type TEXT NOT NULL,
            received_at INTEGER NOT NULL,
            seq TEXT UNIQUE
        )
        """)
        # Current status tracker (for offline detection)
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
        # Long-term storage for batching
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
        # 1. Store raw heartbeat
        try:
            db.execute(
                "INSERT INTO heartbeats (node, node_type, received_at, seq) VALUES (?, ?, ?, ?)",
                (node, node_type, now, seq)
            )
        except sqlite3.IntegrityError:
            pass # Ignore duplicates

        # 2. Check for Recovery (Offline -> Online)
        row = db.execute("SELECT status FROM node_status WHERE node=?", (node,)).fetchone()
        
        # If it was offline (or new), mark it online
        if not row or row[0] == "offline":
            # Add to event queue for the bot
            event_queue.append({
                "node": node, 
                "type": "online", 
                "ts": now,
                "node_type": node_type
            })
            print(f"[CENTRAL] Event: Node {node} came ONLINE")

        # 3. Update Status Table
        db.execute("""
            INSERT INTO node_status (node, node_type, last_seen, status) 
            VALUES (?, ?, ?, 'online')
            ON CONFLICT(node) DO UPDATE SET last_seen=excluded.last_seen, status='online'
        """, (node, node_type, now))
        db.commit()

    return jsonify({"ok": True})

# ================== BACKGROUND MONITOR (WATCHER) ==================

def monitor_nodes():
    """Checks for nodes that have stopped sending heartbeats."""
    while True:
        time.sleep(10) # Check every 10 seconds
        now = int(time.time())
        
        with db_lock, sqlite3.connect(BUFFER_DB) as db:
            # Find nodes marked 'online' but haven't been seen in THRESHOLD
            offline_nodes = db.execute("""
                SELECT node, node_type FROM node_status 
                WHERE status = 'online' AND (? - last_seen) > ?
            """, (now, OFFLINE_THRESHOLD)).fetchall()

            for node, n_type in offline_nodes:
                # Mark as offline in DB
                db.execute("UPDATE node_status SET status='offline' WHERE node=?", (node,))
                
                # Push event to queue
                event_queue.append({
                    "node": node, 
                    "type": "offline", 
                    "ts": now,
                    "node_type": n_type
                })
                print(f"[CENTRAL] Event: Node {node} went OFFLINE")
            
            db.commit()

# ================== ARCHIVE ROTATION ==================

def buffer_rotator():
    """Moves data from Buffer to Archive every 30 minutes."""
    while True:
        time.sleep(ROTATE_CHECK_INTERVAL)
        try:
            with db_lock, sqlite3.connect(BUFFER_DB) as buf_db, sqlite3.connect(ARCHIVE_DB) as arc_db:
                # Check age of oldest heartbeat
                cur = buf_db.execute("SELECT MIN(received_at) FROM heartbeats")
                row = cur.fetchone()
                
                if not row or not row[0]:
                    continue # Buffer empty

                oldest = row[0]
                if int(time.time()) - oldest < BUFFER_ROTATE_SECONDS:
                    continue # Not time yet

                # Move data
                rows = buf_db.execute("SELECT node, node_type, received_at, seq FROM heartbeats").fetchall()
                if not rows: continue

                arc_db.executemany(
                    "INSERT OR IGNORE INTO heartbeats_archive (node, node_type, received_at, seq) VALUES (?, ?, ?, ?)",
                    rows
                )
                arc_db.commit()

                # Clear buffer
                buf_db.execute("DELETE FROM heartbeats")
                buf_db.commit()
                print(f"[CENTRAL] Archived {len(rows)} heartbeats.")

        except Exception as e:
            print(f"[CENTRAL] Rotation Error: {e}")

# ================== BOT ENDPOINTS ==================

@app.route("/events", methods=["GET"])
def get_events():
    """Bot polls this endpoint to get instant alerts."""
    require_api_key()
    global event_queue
    
    # Return events and clear queue
    response = list(event_queue)
    event_queue = [] 
    return jsonify(response)

@app.route("/archive/pull", methods=["GET"])
def pull_archive():
    """Bot calls this every 30m to get history."""
    require_api_key()
    global current_batch_id

    with db_lock, sqlite3.connect(ARCHIVE_DB) as db:
        rows = db.execute(
            "SELECT node, node_type, received_at, seq FROM heartbeats_archive"
        ).fetchall()

    if not rows:
        return jsonify({"count": 0, "data": []})

    current_batch_id = str(uuid.uuid4())
    print(f"[CENTRAL] Sending batch {current_batch_id} to bot")

    return jsonify({
        "batch_id": current_batch_id,
        "count": len(rows),
        "data": [
            {"node": r[0], "node_type": r[1], "received_at": r[2], "seq": r[3]} 
            for r in rows
        ]
    })

@app.route("/archive/ack", methods=["POST"])
def archive_ack():
    """Bot confirms it saved the data, so we can delete it."""
    require_api_key()
    global current_batch_id

    data = request.json or {}
    batch_id = data.get("batch_id")

    if not batch_id or batch_id != current_batch_id:
        return jsonify({"error": "invalid batch"}), 400

    with db_lock, sqlite3.connect(ARCHIVE_DB) as db:
        db.execute("DELETE FROM heartbeats_archive")
        db.commit()

    print(f"[CENTRAL] Batch {batch_id} acknowledged & cleared.")
    current_batch_id = None
    return jsonify({"ok": True})

@app.route("/")
def health():
    return "Central Server Active", 200

# ================== MAIN ==================

if __name__ == "__main__":
    if not CENTRAL_API_KEY:
        raise RuntimeError("CENTRAL_API_KEY not set")

    init_dbs()
    
    # Start background threads
    Thread(target=monitor_nodes, daemon=True).start()
    Thread(target=buffer_rotator, daemon=True).start()
    
    print("[CENTRAL] Server Online on Port 5000")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
