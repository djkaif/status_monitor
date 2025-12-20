from flask import Flask, request, jsonify
import mysql.connector
import os
import time

app = Flask(__name__)

# ===================== CONFIG =====================
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
SECRET_KEY = os.getenv("CENTRAL_SECRET")
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", 60))  # seconds

# ===================== DATABASE =====================
db = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)
cursor = db.cursor(dictionary=True)

# Create tables if not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS nodes (
    node_id VARCHAR(50) PRIMARY KEY,
    node_type VARCHAR(20),
    last_seen INT,
    status VARCHAR(20) DEFAULT 'offline'
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS status_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    node_id VARCHAR(50),
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    timestamp INT
)
""")
db.commit()

# ===================== HEARTBEAT ROUTE =====================
@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    auth = request.headers.get("X-API-Key")
    if auth != SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    node_id = data.get("node")
    node_type = data.get("node_type", "free")
    timestamp = int(time.time())

    # Insert or update node
    cursor.execute("""
        INSERT INTO nodes (node_id, node_type, last_seen, status)
        VALUES (%s, %s, %s, 'online')
        ON DUPLICATE KEY UPDATE last_seen=%s, status='online'
    """, (node_id, node_type, timestamp, timestamp))
    db.commit()

    print(f"[HEARTBEAT] {node_id} online at {timestamp}")
    return jsonify({"ok": True})

# ===================== NODE STATUS CHECK =====================
def check_nodes():
    while True:
        current_time = int(time.time())
        cursor.execute("SELECT * FROM nodes")
        nodes = cursor.fetchall()

        for node in nodes:
            last_seen = node["last_seen"]
            status = node["status"]
            node_id = node["node_id"]

            if current_time - last_seen > HEARTBEAT_TIMEOUT:
                if status != "offline":
                    # Node went down
                    cursor.execute("""
                        UPDATE nodes SET status='offline' WHERE node_id=%s
                    """, (node_id,))
                    cursor.execute("""
                        INSERT INTO status_events (node_id, old_status, new_status, timestamp)
                        VALUES (%s, %s, %s, %s)
                    """, (node_id, status, "offline", current_time))
                    db.commit()
                    print(f"[ALERT] Node {node_id} is DOWN")
            else:
                if status != "online":
                    # Node recovered
                    cursor.execute("""
                        UPDATE nodes SET status='online' WHERE node_id=%s
                    """, (node_id,))
                    cursor.execute("""
                        INSERT INTO status_events (node_id, old_status, new_status, timestamp)
                        VALUES (%s, %s, %s, %s)
                    """, (node_id, status, "online", current_time))
                    db.commit()
                    print(f"[ALERT] Node {node_id} is UP")
        time.sleep(HEARTBEAT_TIMEOUT // 2)  # check twice per timeout

# ===================== HEALTH CHECK =====================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# ===================== RUN SERVER =====================
if __name__ == "__main__":
    import threading
    t = threading.Thread(target=check_nodes, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8000)
