import os
import time
import threading
from flask import Flask, request, jsonify
import mysql.connector
from datetime import datetime

# ================= CONFIG =================

API_KEY = os.getenv("CENTRAL_API_KEY")
OFFLINE_THRESHOLD = int(os.getenv("OFFLINE_THRESHOLD", 60))
RESET_DB = os.getenv("RESET_DATABASE", "false").lower() == "true"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "ssl_disabled": True,
}

# ================= APP =================

app = Flask(__name__)

# ================= DATABASE =================

db = mysql.connector.connect(**DB_CONFIG)
db.autocommit = True
cursor = db.cursor(dictionary=True)

print("[CENTRAL] Database connected successfully")

def init_db():
    if RESET_DB:
        print("[CENTRAL] RESET_DATABASE enabled — wiping tables")
        cursor.execute("DROP TABLE IF EXISTS events")
        cursor.execute("DROP TABLE IF EXISTS nodes")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS nodes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        node VARCHAR(64) UNIQUE,
        node_type VARCHAR(32),
        status VARCHAR(16),
        last_seen DATETIME,
        down_since DATETIME
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INT AUTO_INCREMENT PRIMARY KEY,
        node VARCHAR(64),
        event_type VARCHAR(16),
        timestamp DATETIME,
        downtime_seconds INT
    )
    """)

    print("[CENTRAL] Database initialized")

init_db()

# ================= HELPERS =================

def now():
    return datetime.utcnow()

def require_key(req):
    return req.headers.get("X-API-Key") == API_KEY

# ================= ROUTES =================

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    if not require_key(request):
        return jsonify({"error": "unauthorized"}), 401

    data = request.json or {}
    node = data.get("node")
    node_type = data.get("node_type", "Unknown")

    if not node:
        return jsonify({"error": "missing node"}), 400

    cursor.execute("SELECT * FROM nodes WHERE node=%s", (node,))
    existing = cursor.fetchone()

    if not existing:
        cursor.execute("""
        INSERT INTO nodes (node, node_type, status, last_seen)
        VALUES (%s, %s, 'online', %s)
        """, (node, node_type, now()))

        cursor.execute("""
        INSERT INTO events (node, event_type, timestamp)
        VALUES (%s, 'online', %s)
        """, (node, now()))

        return jsonify({"ok": True, "created": True})

    # Node exists
    if existing["status"] == "offline":
        downtime = int((now() - existing["down_since"]).total_seconds())
        cursor.execute("""
        INSERT INTO events (node, event_type, timestamp, downtime_seconds)
        VALUES (%s, 'up', %s, %s)
        """, (node, now(), downtime))

        cursor.execute("""
        UPDATE nodes
        SET status='online', down_since=NULL, last_seen=%s
        WHERE node=%s
        """, (now(), node))
    else:
        cursor.execute("""
        UPDATE nodes SET last_seen=%s WHERE node=%s
        """, (now(), node))

    return jsonify({"ok": True})

@app.route("/nodes")
def nodes():
    cursor.execute("SELECT * FROM nodes")
    return jsonify(cursor.fetchall())

@app.route("/events")
def events():
    cursor.execute("""
    SELECT * FROM events
    ORDER BY timestamp DESC
    LIMIT 50
    """)
    return jsonify(cursor.fetchall())

# ================= OFFLINE CHECKER =================

def check_nodes():
    while True:
        time.sleep(10)
        cursor.execute("SELECT * FROM nodes WHERE status='online'")
        nodes = cursor.fetchall()

        for n in nodes:
            delta = (now() - n["last_seen"]).total_seconds()
            if delta > OFFLINE_THRESHOLD:
                cursor.execute("""
                UPDATE nodes
                SET status='offline', down_since=%s
                WHERE node=%s
                """, (now(), n["node"]))

                cursor.execute("""
                INSERT INTO events (node, event_type, timestamp)
                VALUES (%s, 'down', %s)
                """, (n["node"], now()))

                print(f"[CENTRAL] Node {n['node']} marked OFFLINE")

threading.Thread(target=check_nodes, daemon=True).start()

# ================= START =================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
