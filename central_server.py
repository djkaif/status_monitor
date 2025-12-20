import os
import time
import threading
from flask import Flask, request, jsonify
import mysql.connector

# Load environment variables from Render
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
CENTRAL_API_KEY = os.getenv("CENTRAL_API_KEY")
OFFLINE_THRESHOLD = int(os.getenv("OFFLINE_THRESHOLD", 60))  # in seconds
RESET_DATABASE = os.getenv("RESET_DATABASE", "False").lower() == "true"

app = Flask(__name__)

# Connect to MySQL
def connect_db():
    try:
        db = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connection_timeout=300,  # 5 min
            ssl_disabled=True
        )
        print("[CENTRAL] Database connected successfully")
        return db
    except mysql.connector.Error as e:
        print(f"[CENTRAL] Database connection error: {e}")
        raise e

db = connect_db()

# Helper function to get a cursor safely
def get_cursor():
    try:
        db.ping(reconnect=True)
    except mysql.connector.Error as e:
        print(f"[CENTRAL] MySQL ping failed, reconnecting: {e}")
        db.reconnect()
    return db.cursor(dictionary=True)

# Reset database if requested
def init_db():
    cursor = get_cursor()
    if RESET_DATABASE:
        cursor.execute("DROP TABLE IF EXISTS nodes")
        print("[CENTRAL] Old database dropped")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node VARCHAR(255) PRIMARY KEY,
            node_type VARCHAR(50),
            status VARCHAR(50),
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()
    cursor.close()
    print("[CENTRAL] Database initialized")

init_db()

# Heartbeat endpoint
@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    key = request.headers.get("X-API-Key")
    if key != CENTRAL_API_KEY:
        return jsonify({"error": "Invalid API Key"}), 403

    data = request.get_json()
    node = data.get("node")
    node_type = data.get("node_type")
    status = data.get("status", "online")

    if not node or not node_type:
        return jsonify({"error": "Missing node or node_type"}), 400

    try:
        cursor = get_cursor()
        # Check if node exists
        cursor.execute("SELECT * FROM nodes WHERE node=%s", (node,))
        result = cursor.fetchall()
        if result:
            cursor.execute(
                "UPDATE nodes SET status=%s, last_update=CURRENT_TIMESTAMP WHERE node=%s",
                (status, node)
            )
        else:
            cursor.execute(
                "INSERT INTO nodes (node, node_type, status) VALUES (%s, %s, %s)",
                (node, node_type, status)
            )
        db.commit()
        cursor.close()
        return jsonify({"ok": True})
    except mysql.connector.Error as e:
        print(f"[CENTRAL] Database error: {e}")
        return jsonify({"error": str(e)}), 500

# Optional: simple status endpoint
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# Background thread to check for offline nodes
def check_nodes():
    while True:
        try:
            cursor = get_cursor()
            cursor.execute("SELECT * FROM nodes")
            nodes = cursor.fetchall()
            for node in nodes:
                last_update = node["last_update"]
                diff = (time.time() - last_update.timestamp())
                if diff > OFFLINE_THRESHOLD:
                    # Here you can handle alerts
                    print(f"[ALERT] Node {node['node']} is offline")
            cursor.close()
            time.sleep(30)
        except mysql.connector.Error as e:
            print(f"[CENTRAL] Background thread DB error: {e}")
            time.sleep(5)

threading.Thread(target=check_nodes, daemon=True).start()

if __name__ == "__main__":
    print("[CENTRAL] Server running...")
    app.run(host="0.0.0.0", port=8000)
