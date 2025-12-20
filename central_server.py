import os
import time
import sqlite3
import asyncio

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ================== CONFIG ==================

CENTRAL_SECRET = os.getenv("CENTRAL_SECRET")
OFFLINE_TIMEOUT = int(os.getenv("OFFLINE_TIMEOUT", "90"))
DB_PATH = "data/status.db"

if not CENTRAL_SECRET:
    raise RuntimeError("CENTRAL_SECRET is not set")

# ================== APP ==================

app = FastAPI(title="Bot Hosting Central Server")

# ================== DATABASE ==================

def init_db():
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect(DB_PATH) as db:
        cur = db.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            node_group TEXT,
            status TEXT,
            last_seen INTEGER
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id TEXT,
            event TEXT,
            timestamp INTEGER
        )
        """)
        db.commit()

init_db()

# ================== MODELS ==================

class Heartbeat(BaseModel):
    node_id: str
    group: str
    secret: str
    timestamp: int

# ================== HELPERS ==================

def get_node(node_id):
    with sqlite3.connect(DB_PATH) as db:
        cur = db.cursor()
        cur.execute(
            "SELECT status FROM nodes WHERE node_id=?",
            (node_id,)
        )
        row = cur.fetchone()
        return row[0] if row else None

def upsert_node(node_id, group, status, ts):
    with sqlite3.connect(DB_PATH) as db:
        cur = db.cursor()
        cur.execute("""
        INSERT INTO nodes (node_id, node_group, status, last_seen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            node_group=excluded.node_group,
            status=excluded.status,
            last_seen=excluded.last_seen
        """, (node_id, group, status, ts))
        db.commit()

def add_event(node_id, event, ts):
    with sqlite3.connect(DB_PATH) as db:
        cur = db.cursor()
        cur.execute(
            "INSERT INTO events (node_id, event, timestamp) VALUES (?, ?, ?)",
            (node_id, event, ts)
        )
        db.commit()

# ================== API ==================

@app.post("/api/heartbeat")
def heartbeat(data: Heartbeat):
    if data.secret != CENTRAL_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    now = int(time.time())
    previous_status = get_node(data.node_id)

    # Detect recovery
    if previous_status == "offline":
        add_event(data.node_id, "UP", now)

    # First-time node
    if previous_status is None:
        add_event(data.node_id, "UP", now)

    upsert_node(
        node_id=data.node_id,
        group=data.group,
        status="online",
        ts=now
    )

    return {"ok": True}

@app.get("/api/nodes")
def list_nodes():
    with sqlite3.connect(DB_PATH) as db:
        cur = db.cursor()
        cur.execute("""
        SELECT node_id, node_group, status, last_seen
        FROM nodes
        ORDER BY node_group, node_id
        """)
        rows = cur.fetchall()

    return {
        "nodes": [
            {
                "node_id": r[0],
                "group": r[1],
                "status": r[2],
                "last_seen": r[3]
            }
            for r in rows
        ]
    }

@app.get("/api/events")
def list_events(after_id: int = 0):
    with sqlite3.connect(DB_PATH) as db:
        cur = db.cursor()
        cur.execute("""
        SELECT id, node_id, event, timestamp
        FROM events
        WHERE id > ?
        ORDER BY id ASC
        """, (after_id,))
        rows = cur.fetchall()

    return {
        "events": [
            {
                "id": r[0],
                "node_id": r[1],
                "event": r[2],
                "timestamp": r[3]
            }
            for r in rows
        ]
    }

# ================== OFFLINE WATCHER ==================

async def offline_watcher():
    while True:
        now = int(time.time())

        with sqlite3.connect(DB_PATH) as db:
            cur = db.cursor()
            cur.execute(
                "SELECT node_id, status, last_seen FROM nodes"
            )
            nodes = cur.fetchall()

        for node_id, status, last_seen in nodes:
            if status == "online" and now - last_seen > OFFLINE_TIMEOUT:
                add_event(node_id, "DOWN", now)
                upsert_node(node_id, "unknown", "offline", last_seen)

        await asyncio.sleep(10)

@app.on_event("startup")
async def startup():
    asyncio.create_task(offline_watcher())
