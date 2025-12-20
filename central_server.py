import os
import mysql.connector
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", 3306))
RESET_DB = os.getenv("RESET_DATABASE", "False").lower() == "true"
CENTRAL_API_KEY = os.getenv("CENTRAL_API_KEY")
OFFLINE_THRESHOLD = int(os.getenv("OFFLINE_THRESHOLD", 60))

app = Flask(__name__)

# Database connection
db = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=DB_PORT
)
cursor = db.cursor(dictionary=True)

# Create tables
def init_db():
    if RESET_DB:
        cursor.execute("DROP TABLE IF EXISTS status_history")
        cursor.execute("DROP TABLE IF EXISTS nodes")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS nodes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        node VARCHAR(255) UNIQUE NOT NULL,
        node_type VARCHAR(50),
        last_seen DATETIME,
        status VARCHAR(20)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS status_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        node_id INT,
        timestamp DATETIME,
        status VARCHAR(20),
        FOREIGN KEY(node_id) REFERENCES nodes(id)
    )
    """)
    db.commit()
    print("[CENTRAL] Database initialized")

init_db()

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    if request.headers.get("X-API-Key") != CENTRAL_API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    node = data.get("node")
    node_type = data.get("node_type", "Free")
    status = data.get("status", "online")

    if not node:
        return jsonify({"error": "Missing node name"}), 400

    cursor.execute("SELECT * FROM nodes WHERE node=%s", (node,))
    node_data = cursor.fetchone()
    now = datetime.utcnow()

    if not node_data:
        cursor.execute("INSERT INTO nodes (node, node_type, last_seen, status) VALUES (%s,%s,%s,%s)",
                       (node, node_type, now, status))
        db.commit()
        cursor.execute("SELECT * FROM nodes WHERE node=%s", (node,))
        node_data = cursor.fetchone()
    else:
        cursor.execute("UPDATE nodes SET last_seen=%s, status=%s WHERE node=%s", (now, status, node))
        db.commit()

    cursor.execute("INSERT INTO status_history (node_id, timestamp, status) VALUES (%s,%s,%s)",
                   (node_data["id"], now, status))
    db.commit()

    return jsonify({"ok": True})

@app.route("/nodes/status", methods=["GET"])
def get_status():
    cursor.execute("SELECT * FROM nodes")
    nodes = cursor.fetchall()
    return jsonify(nodes)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
