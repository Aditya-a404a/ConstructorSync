import asyncio
import json
import os
import random
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Define schema and config
app = FastAPI(
    title="Constructor.io API Mock Server",
    description="High-fidelity mock of Constructor's Catalog Ingestion API with rate-limiting, size constraints, and chaos mode.",
    version="2.0.0"
)

# Configuration from environment
API_KEY = os.getenv("CONSTRUCTOR_API_KEY", "test_api_key_12345")
DB_PATH = os.getenv("CONSTRUCTOR_MOCK_DB_PATH", "data/constructor_mock.db")

# Chaos Mode state
chaos_config = {
    "enabled": False,
    "error_rate": 0.0,      # Probability of random 500 error
    "latency_min_ms": 0,    # Minimum delay in ms
    "latency_max_ms": 0,    # Maximum delay in ms
    "timeout_rate": 0.0,    # Probability of simulating a timeout (long sleep)
}

# Ensure data directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Initialize SQLite database
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    # Table for ingested items
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            url TEXT,
            image_url TEXT,
            data TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Table for async tasks
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL, -- QUEUED, PROCESSING, COMPLETED, FAILED
            inserted_count INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

init_db()

# Models
class ItemModel(BaseModel):
    id: str
    name: str
    url: Optional[str] = None
    image_url: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

class IngestionPayload(BaseModel):
    items: List[ItemModel]

class ChaosSettings(BaseModel):
    enabled: bool
    error_rate: float = Field(..., ge=0.0, le=1.0)
    latency_min_ms: int = Field(..., ge=0)
    latency_max_ms: int = Field(..., ge=0)
    timeout_rate: float = Field(..., ge=0.0, le=1.0)

# Helper to check active tasks
def get_active_tasks_count() -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tasks WHERE status IN ('QUEUED', 'PROCESSING')")
    count = cursor.fetchone()[0]
    conn.close()
    return count

# Authenticate requests
def authenticate(authorization: Optional[str]):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    
    # Try basic authentication or token authentication
    token = authorization.replace("Bearer ", "").replace("Token ", "").replace("Basic ", "").strip()
    if token != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized API Key")

# Background processing of ingestion tasks
async def process_task(task_id: str, items: List[Dict[str, Any]]):
    # Update status to PROCESSING
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE tasks SET status = 'PROCESSING', updated_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), task_id)
    )
    conn.commit()

    # Simulate some processing delay
    await asyncio.sleep(random.uniform(0.5, 2.0))

    try:
        inserted = 0
        for item in items:
            item_id = item["id"]
            name = item["name"]
            url = item.get("url")
            image_url = item.get("image_url")
            data_str = json.dumps(item.get("data") or {})
            
            cursor.execute(
                """
                INSERT INTO items (id, name, url, image_url, data, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    url=excluded.url,
                    image_url=excluded.image_url,
                    data=excluded.data,
                    updated_at=excluded.updated_at
                """,
                (item_id, name, url, image_url, data_str, datetime.utcnow().isoformat())
            )
            inserted += 1

        cursor.execute(
            "UPDATE tasks SET status = 'COMPLETED', inserted_count = ?, updated_at = ? WHERE id = ?",
            (inserted, datetime.utcnow().isoformat(), task_id)
        )
        conn.commit()
    except Exception as e:
        cursor.execute(
            "UPDATE tasks SET status = 'FAILED', error_message = ?, updated_at = ? WHERE id = ?",
            (str(e), datetime.utcnow().isoformat(), task_id)
        )
        conn.commit()
    finally:
        conn.close()

# Middleware to apply Chaos and Rate Limit Headers
@app.middleware("http")
async def chaos_and_rate_limit_middleware(request: Request, call_next):
    # Skip admin/chaos/metrics endpoints from chaos checks
    if request.url.path.startswith("/admin") or request.url.path == "/health":
        return await call_next(request)

    # 1. Simulate Connection Latency / Timeout
    if chaos_config["enabled"]:
        # Simulate Timeout (e.g. long sleep)
        if random.random() < chaos_config["timeout_rate"]:
            await asyncio.sleep(15.0)  # Long sleep to trigger client timeout
            return Response("Gateway Timeout", status_code=504)
        
        # Simulate random latency
        if int(chaos_config["latency_max_ms"]) > 0:
            delay = random.randint(int(chaos_config["latency_min_ms"]), int(chaos_config["latency_max_ms"]))
            await asyncio.sleep(delay / 1000.0)

        # Simulate Random 500 Errors
        if random.random() < chaos_config["error_rate"]:
            return JSONResponse(
                status_code=500,
                content={"message": "Internal server error (Chaos Mode)"}
            )

    # 2. Get active queue count and enforce Queue Limit (1,000 max active tasks)
    active_tasks = get_active_tasks_count()
    remaining_slots = max(0, 1000 - active_tasks)

    # If active tasks exceed 1000, reject request with 429
    if active_tasks >= 1000 and request.url.path.endswith("/v2/items"):
        return JSONResponse(
            status_code=429,
            headers={"X-RateLimit-Tasks-Remaining": "0"},
            content={"message": "Too Many Requests - Constructor Ingestion Queue is Full (1,000 tasks max)"}
        )

    # Process response
    response = await call_next(request)
    
    # Add Rate Limit header to every API response
    response.headers["X-RateLimit-Tasks-Remaining"] = str(remaining_slots)
    return response

@app.get("/health")
async def health():
    return {"status": "ok", "chaos_mode": chaos_config["enabled"]}

@app.post("/v2/items")
@app.put("/v2/items")
async def ingest_items(
    payload: IngestionPayload,
    background_tasks: BackgroundTasks,
    authorization: Optional[str] = Header(None)
):
    # 1. Authenticate
    authenticate(authorization)

    items = payload.items

    # 2. Batch size check (Max 1,000 items)
    if len(items) > 1000:
        raise HTTPException(
            status_code=400,
            detail="Batch size exceeds maximum limit of 1,000 items per request"
        )
    if len(items) == 0:
        raise HTTPException(status_code=400, detail="Batch must contain at least 1 item")

    # 3. Item & Batch size validations
    total_data_size = 0
    for item in items:
        # Check size of the nested "data" dictionary
        data_bytes = json.dumps(item.data or {}).encode("utf-8")
        item_data_size_kb = len(data_bytes) / 1024.0
        
        # Max size of data field per item is 200KB
        if item_data_size_kb > 200:
            raise HTTPException(
                status_code=400,
                detail=f"Item '{item.id}' data field size ({item_data_size_kb:.2f}KB) exceeds limit of 200KB"
            )
        total_data_size += len(data_bytes)

    # Average data field size across batch must be <= 30KB
    avg_data_size_kb = (total_data_size / len(items)) / 1024.0
    if avg_data_size_kb > 30:
        raise HTTPException(
            status_code=400,
            detail=f"Average data field size across batch ({avg_data_size_kb:.2f}KB) exceeds limit of 30KB"
        )

    # 4. Enqueue task in SQLite
    task_id = str(uuid.uuid4())
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO tasks (id, status, created_at, updated_at) VALUES (?, 'QUEUED', ?, ?)",
        (task_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

    # Convert Pydantic items to dicts for serialization safety in thread/background worker
    items_list = [item.model_dump() for item in items]

    # Start async task process
    background_tasks.add_task(process_task, task_id, items_list)

    return {
        "task_id": task_id,
        "task_status_path": f"/v2/tasks/{task_id}"
    }

@app.get("/v2/tasks/{task_id}")
async def get_task_status(task_id: str, authorization: Optional[str] = Header(None)):
    authenticate(authorization)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, status, inserted_count, error_message, created_at, updated_at FROM tasks WHERE id = ?", (task_id,))
    task = cursor.fetchone()
    conn.close()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": task["id"],
        "status": task["status"],
        "inserted_count": task["inserted_count"],
        "error_message": task["error_message"],
        "created_at": task["created_at"],
        "updated_at": task["updated_at"]
    }

# Admin endpoints for configuring Chaos mode & verifying ingested items in tests
@app.post("/admin/chaos")
async def configure_chaos(settings: ChaosSettings):
    global chaos_config
    chaos_config.update(settings.model_dump())
    return {"message": "Chaos mode configured successfully", "config": chaos_config}

@app.get("/admin/items")
async def list_items(limit: int = 100, offset: int = 0):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, url, image_url, data, updated_at FROM items LIMIT ? OFFSET ?", (limit, offset))
    rows = cursor.fetchall()
    conn.close()
    
    items = []
    for row in rows:
        items.append({
            "id": row["id"],
            "name": row["name"],
            "url": row["url"],
            "image_url": row["image_url"],
            "data": json.loads(row["data"]),
            "updated_at": row["updated_at"]
        })
    return {"items": items}

@app.delete("/admin/clear")
async def clear_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM items")
    cursor.execute("DELETE FROM tasks")
    conn.commit()
    conn.close()
    return {"message": "Mock database cleared successfully"}
