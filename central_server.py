import os
import time
import threading
from datetime import datetime, timedelta

import mysql.connector
from flask import Flask, request, jsonify

# =======================
# ENV CONFIG
# =======================

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

API_KEY = os.getenv("CENTRAL_API_KEY")
RESET_DB = os.getenv("RESET_DATABASE", "false").lower() == "true"

OFFLINE_AFTER = int(os.getenv("OFFLINE_AFTER", 60))  # seconds
print("[DEBUG] DB_HOST =", os.getenv("DB_HOST"))

# =======================
# APP INIT
# =======================

app = Flask(__name__)

db = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    ssl_disabled=True
)

cursor = db.cursor(dictionary=True)

# =======================
# DATABASE INIT
# =======================

def init_db():
    if RESET_DB:
        cursor.execute("DROP TABLE IF EXISTS nodes")
        db.commit()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS nodes (
        node_id INT AUTO_INCREMENT PRIMARY KEY,
        node_name VARCHAR(50) UNIQUE,
        node_type VARCHAR(20),
        status VARCHAR(10),
        last_seen DATETIME,
        offline_since DATETIME,
        total_uptime BIGINT DEFAULT 0,
        total_downtime BIGINT DEFAULT 0
    )
    """)
    db.commit()
    print("[CENTRAL] Database initialized")

init_db()

# =======================
# HEARTBEAT ENDPOINT
# =======================

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    if request.headers.get("X-API-Key") != API_KEY:
        return jsonify({"error": "unauthorized"}), 401

    data = request.json
    node = data.get("node")
    node_type = data.get("node_type", "Free")

    if not node:
        return jsonify({"error": "node missing"}), 400

    now = datetime.utcnow()

    cursor.execute("SELECT * FROM nodes WHERE node_name=%s", (node,))
    existing = cursor.fetchone()

    if not existing:
        cursor.execute("""
            INSERT INTO nodes (node_name, node_type, status, last_seen)
            VALUES (%s,%s,'online',%s)
        """, (node, node_type, now))
        db.commit()
    else:
        cursor.execute("""
            UPDATE nodes SET last_seen=%s WHERE node_name=%s
        """, (now, node))
        db.commit()

    return jsonify({"ok": True})

# =======================
# NODE MONITOR THREAD
# =======================

def monitor_nodes():
    while True:
        time.sleep(30)
        cursor.execute("SELECT * FROM nodes")
        nodes = cursor.fetchall()
        now = datetime.utcnow()

        for n in nodes:
            if not n["last_seen"]:
                continue

            delta = (now - n["last_seen"]).total_seconds()

            # OFFLINE
            if delta > OFFLINE_AFTER and n["status"] == "online":
                cursor.execute("""
                    UPDATE nodes
                    SET status='offline',
                        offline_since=%s,
                        total_uptime=total_uptime+%s
                    WHERE node_id=%s
                """, (
                    now,
                    OFFLINE_AFTER,
                    n["node_id"]
                ))
                db.commit()

            # ONLINE AGAIN
            elif delta <= OFFLINE_AFTER and n["status"] == "offline":
                downtime = int((now - n["offline_since"]).total_seconds())
                cursor.execute("""
                    UPDATE nodes
                    SET status='online',
                        offline_since=NULL,
                        total_downtime=total_downtime+%s
                    WHERE node_id=%s
                """, (downtime, n["node_id"]))
                db.commit()

threading.Thread(target=monitor_nodes, daemon=True).start()

# =======================
# API FOR DISCORD BOT
# =======================

@app.route("/api/nodes")
def api_nodes():
    cursor.execute("SELECT * FROM nodes")
    nodes = cursor.fetchall()

    result = []
    for n in nodes:
        uptime = n["total_uptime"]
        downtime = n["total_downtime"]
        total = uptime + downtime
        percent = round((uptime / total) * 100, 2) if total > 0 else 100

        result.append({
            "node_name": n["node_name"],
            "node_type": n["node_type"],
            "status": n["status"],
            "last_seen": n["last_seen"],
            "uptime_percent": percent,
            "offline_since": n["offline_since"]
        })

    return jsonify(result)

# =======================
# WEB DASHBOARD
# =======================

@app.route("/status")
def status():
    cursor.execute("SELECT * FROM nodes")
    nodes = cursor.fetchall()

    html = """
    <html>
    <head>
        <meta http-equiv="refresh" content="10">
        <style>
            body{background:#111;color:#eee;font-family:Arial}
            table{width:100%;border-collapse:collapse}
            th,td{padding:10px;border-bottom:1px solid #333}
            .online{color:#2ecc71}
            .offline{color:#e74c3c}
        </style>
    </head>
    <body>
    <h2>Node Status</h2>
    <table>
    <tr><th>Node</th><th>Type</th><th>Status</th><th>Last Seen</th></tr>
    """

    for n in nodes:
        html += f"""
        <tr>
            <td>{n['node_name']}</td>
            <td>{n['node_type']}</td>
            <td class="{n['status']}">{n['status'].upper()}</td>
            <td>{n['last_seen']}</td>
        </tr>
        """

    html += "</table></body></html>"
    return html

# =======================
# START SERVER
# =======================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
