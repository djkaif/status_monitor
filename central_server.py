import os
import time
import mysql.connector
from mysql.connector import pooling
from flask import Flask, request, jsonify, abort
from dotenv import load_dotenv

load_dotenv()

# =======================
# CONFIG
# =======================

API_KEY = os.getenv("CENTRAL_API_KEY")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True
}

HEARTBEAT_TIMEOUT = 120  # seconds

# =======================
# APP INIT
# =======================

app = Flask(__name__)

db_pool = pooling.MySQLConnectionPool(
    pool_name="central_pool",
    pool_size=5,
    **DB_CONFIG
)

# =======================
# DATABASE SETUP
# =======================

def init_db():
    conn = db_pool.get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            node_name VARCHAR(64) UNIQUE NOT NULL,
            last_seen BIGINT NOT NULL,
            status ENUM('online','offline','alert') NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS status_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            node_name VARCHAR(64) NOT NULL,
            old_status VARCHAR(16),
            new_status VARCHAR(16),
            timestamp BIGINT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS uptime_snapshots (
            id INT AUTO_INCREMENT PRIMARY KEY,
            node_name VARCHAR(64) NOT NULL,
            is_online BOOLEAN NOT NULL,
            timestamp BIGINT NOT NULL
        )
    """)

    cursor.close()
    conn.close()
    print("[CENTRAL] Database initialized")

# =======================
# AUTH
# =======================

def verify_key(req):
    key = req.headers.get("X-API-Key")
    if key != API_KEY:
        abort(401, "Invalid API key")

# =======================
# ROUTES
# =======================

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    verify_key(request)
    data = request.json

    node_name = data.get("node")
    status = data.get("status", "online")
    now = int(time.time())

    if not node_name:
        return jsonify({"error": "node missing"}), 400

    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        "SELECT status FROM nodes WHERE node_name=%s",
        (node_name,)
    )
    existing = cursor.fetchone()

    if existing:
        if existing["status"] != status:
            cursor.execute(
                "INSERT INTO status_events (node_name, old_status, new_status, timestamp) VALUES (%s,%s,%s,%s)",
                (node_name, existing["status"], status, now)
            )

        cursor.execute(
            "UPDATE nodes SET last_seen=%s, status=%s WHERE node_name=%s",
            (now, status, node_name)
        )
    else:
        cursor.execute(
            "INSERT INTO nodes (node_name, last_seen, status) VALUES (%s,%s,%s)",
            (node_name, now, status)
        )

    cursor.execute(
        "INSERT INTO uptime_snapshots (node_name, is_online, timestamp) VALUES (%s,%s,%s)",
        (node_name, status == "online", now)
    )

    cursor.close()
    conn.close()

    return jsonify({"ok": True})

@app.route("/nodes", methods=["GET"])
def list_nodes():
    verify_key(request)

    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM nodes")
    nodes = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(nodes)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# =======================
# STARTUP
# =======================

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8000)
