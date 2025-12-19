from flask import Flask, request, jsonify
import sqlite3
import os

DB = "database.db"
app = Flask(__name__)

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS node_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node TEXT,
            status TEXT,
            timestamp INTEGER
        )
    """)
    conn.commit()
    conn.close()

@app.route("/report", methods=["POST"])
def report():
    data = request.json
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute(
        "INSERT INTO node_status (node, status, timestamp) VALUES (?, ?, ?)",
        (data["node"], data["status"], data["timestamp"])
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/status", methods=["GET"])
def status():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("SELECT node, status, timestamp FROM node_status ORDER BY id DESC LIMIT 100")
    rows = c.fetchall()
    conn.close()
    return jsonify(rows)

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
