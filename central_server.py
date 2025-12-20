from flask import Flask, request, jsonify
import mysql.connector
import os
import time
import threading
from dotenv import load_dotenv

# ----------------- Load environment -----------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "us.mysql.db.bot-hosting.net")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
SECRET_KEY = os.getenv("CENTRAL_SECRET")
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", 60))

app = Flask(__name__)

# ----------------- Helper: Get DB connection -----------------
def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        ssl_disabled=True,
        connection_timeout=10
    )

# ----------------- Initialize tables -----------------
try:
    cnx = get_db_connection()
    cur = cnx.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id VARCHAR(50) PRIMARY KEY,
            node_type VARCHAR(20),
            last_seen INT,
            status VARCHAR(20) DEFAULT 'offline'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS status_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            node_id VARCHAR(50),
            old_status VARCHAR(20),
            new_status VARCHAR(20),
            timestamp INT
        )
    """)
    cnx.commit()
    cur.close()
    cnx.close()
    print("[CENTRAL] Database initialized")
except mysql.connector.Error as err:
    print(f"[CENTRAL][ERROR] Database init failed: {err}")
    exit(1)

# ----------------- Heartbeat endpoint -----------------
@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    try:
        auth = request.headers.get("X-API-Key")
        if auth != SECRET_KEY:
            return jsonify({"error": "Unauthorized"}), 401

        data = request.json
        node_id = data.get("node")
        node_type = data.get("node_type", "Free")
        timestamp = int(time.time())

        if not node_id:
            return jsonify({"error": "Missing node ID"}), 400

        # Use a fresh connection per request
        cnx = get_db_connection()
        cur = cnx.cursor(dictionary=True)

        cur.execute("""
            INSERT INTO nodes (node_id, node_type, last_seen, status)
            VALUES (%s, %s, %s, 'online')
            ON DUPLICATE KEY UPDATE last_seen=%s, status='online'
        """, (node_id, node_type, timestamp, timestamp))
        cnx.commit()
        cur.close()
        cnx.close()

        print(f"[HEARTBEAT] {node_id} online at {timestamp}")
        return jsonify({"ok": True})

    except Exception as e:
        print(f"[HEARTBEAT ERROR] {e}")
        return jsonify({"error": str(e)}), 500

# ----------------- Node checker thread -----------------
def check_nodes():
    while True:
        try:
            cnx = get_db_connection()
            cur = cnx.cursor(dictionary=True)
            cur.execute("SELECT * FROM nodes")
            nodes = cur.fetchall()
            cur.close()
            cnx.close()

            current_time = int(time.time())
            for node in nodes:
                node_id = node["node_id"]
                status = node["status"]
                last_seen = node["last_seen"]

                if current_time - last_seen > HEARTBEAT_TIMEOUT:
                    if status != "offline":
                        cnx = get_db_connection()
                        cur = cnx.cursor()
                        cur.execute("UPDATE nodes SET status='offline' WHERE node_id=%s", (node_id,))
                        cur.execute(
                            "INSERT INTO status_events (node_id, old_status, new_status, timestamp) VALUES (%s,%s,%s,%s)",
                            (node_id, status, "offline", current_time)
                        )
                        cnx.commit()
                        cur.close()
                        cnx.close()
                        print(f"[ALERT] Node {node_id} is DOWN")
                else:
                    if status != "online":
                        cnx = get_db_connection()
                        cur = cnx.cursor()
                        cur.execute("UPDATE nodes SET status='online' WHERE node_id=%s", (node_id,))
                        cur.execute(
                            "INSERT INTO status_events (node_id, old_status, new_status, timestamp) VALUES (%s,%s,%s,%s)",
                            (node_id, status, "online", current_time)
                        )
                        cnx.commit()
                        cur.close()
                        cnx.close()
                        print(f"[ALERT] Node {node_id} is UP")
        except Exception as e:
            print(f"[CHECK_NODES ERROR] {e}")

        time.sleep(max(HEARTBEAT_TIMEOUT // 2, 5))

# ----------------- Health endpoint -----------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# ----------------- Run server -----------------
if __name__ == "__main__":
    t = threading.Thread(target=check_nodes, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8000)
