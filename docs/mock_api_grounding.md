# Constructor Ingestion API Mock Server: Implementation & Grounding Specs

This document explains the high-fidelity mock server for Constructor's Catalog Ingestion API. The mock server behaves precisely like Constructor's production system to serve as a reliable local sandbox for building and testing ingestion workflows.

---

## 1. Grounding in Official Constructor Docs

According to [Constructor.io Developer Documentation](https://docs.constructor.com/):

1. **Endpoint Paths**: Intraday catalog updates are sent to the asynchronous Items API at `POST /v2/items` or `PUT /v2/items`.
2. **Asynchronous Architecture**: Ingesting items is non-blocking. The system returns a `task_id` and a `task_status_path`. Clients poll `/v2/tasks/{task_id}` to check if the insertion/update succeeded.
3. **Queue / Tasks Rate Limiting**: The ingestion queue is capped at **1,000 tasks**. If exceeded, the API returns a `429 Too Many Requests` error. Responses contain the `X-RateLimit-Tasks-Remaining` header representing how many task slots are left.
4. **Batch size limit**: Max **1,000 items** per API call.
5. **Payload Size Constraints**:
   - The size of the `data` dictionary for any single item must be **$\le$ 200KB**.
   - The average size of the `data` dictionary across the entire batch must be **$\le$ 30KB**.

---

## 2. Line-by-Line Code Walkthrough

Below is a detailed line-by-line explanation of the [mock_api.py](file:///Users/adityaarora/ConstructorSync/src/constructsync/mock_api.py) implementation.

### Imports & Configuration (Lines 1 - 32)
```python
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
```
* **FastAPI & Pydantic**: Used to build the REST API endpoints and enforce type validation.
* **SQLite3**: Used to simulate Constructor's database storage. Ingested products are kept in SQLite so tests and demos can verify the data was successfully cleaned and stored.
* **typing**: Enforces static type safety (validated via `mypy`).

### Global Config & Chaos Mode Configuration
```python
API_KEY = os.getenv("CONSTRUCTOR_API_KEY", "test_api_key_12345")
DB_PATH = os.getenv("CONSTRUCTOR_MOCK_DB_PATH", "data/constructor_mock.db")

chaos_config = {
    "enabled": False,
    "error_rate": 0.0,      # Probability of random 500 error
    "latency_min_ms": 0,    # Minimum delay in ms
    "latency_max_ms": 0,    # Maximum delay in ms
    "timeout_rate": 0.0,    # Probability of simulating a timeout (long sleep)
}
```
* **`API_KEY` / `DB_PATH`**: Configurable via environment variables.
* **`chaos_config`**: Stores variables that dictate simulation failures.

### SQLite Schema Design (Lines 33 - 61)
```python
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
```
* **`items` table**: Replicates how Constructor stores catalog entries. Custom attributes are stored as serialized JSON strings in the `data` column.
* **`tasks` table**: Tracks status (`QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`) to power task status retrieval.

### Type-Safe Pydantic Models (Lines 62 - 82)
```python
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
```
* **`ItemModel`**: Validates fields. `id` and `name` are mandatory; metadata goes into `data`.
* **`IngestionPayload`**: Strictly expects an `items` array.
* **`ChaosSettings`**: Enforces validation parameters (e.g. error rate must be between 0.0 and 1.0).

### Authentication Middleware (Lines 83 - 103)
```python
def authenticate(authorization: Optional[str]):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    
    token = authorization.replace("Bearer ", "").replace("Token ", "").replace("Basic ", "").strip()
    if token != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized API Key")
```
* Ensures every ingestion and task request has a valid api key. Compatible with standard header formats (Token, Bearer, Basic).

### Asynchronous Background Processing (Lines 104 - 150)
```python
async def process_task(task_id: str, items: List[Dict[str, Any]]):
    ...
    # Simulate processing delay
    await asyncio.sleep(random.uniform(0.5, 2.0))
    ...
    cursor.execute("""
        INSERT INTO items (id, name, url, image_url, data, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET ...
    """)
```
* Implements `ON CONFLICT` upsert logic, reflecting Constructor's catalog synchronization where sending an existing SKU updates its contents.

### Chaos & Rate Limit Middleware (Lines 151 - 192)
```python
@app.middleware("http")
async def chaos_and_rate_limit_middleware(request: Request, call_next):
    # Simulate Latency/Timeout/500 Errors
    if chaos_config["enabled"]:
        if random.random() < chaos_config["timeout_rate"]:
            await asyncio.sleep(15.0)
            return Response("Gateway Timeout", status_code=504)
        
        if int(chaos_config["latency_max_ms"]) > 0:
            delay = random.randint(int(chaos_config["latency_min_ms"]), int(chaos_config["latency_max_ms"]))
            await asyncio.sleep(delay / 1000.0)
            
        if random.random() < chaos_config["error_rate"]:
            return JSONResponse(status_code=500, content={"message": "Internal error (Chaos Mode)"})

    # Enforce queue limit
    active_tasks = get_active_tasks_count()
    remaining_slots = max(0, 1000 - active_tasks)
    
    if active_tasks >= 1000 and request.url.path.endswith("/v2/items"):
        return JSONResponse(
            status_code=429,
            headers={"X-RateLimit-Tasks-Remaining": "0"},
            content={"message": "Too Many Requests - Queue Full"}
        )
```
* Intercepts HTTP requests to inject latency/errors or trigger `429 Too Many Requests`.
* Injects `X-RateLimit-Tasks-Remaining` header into all responses.

### Ingestion Validation Logic (Lines 193 - 269)
```python
    # Batch size check
    if len(items) > 1000:
        raise HTTPException(status_code=400, detail="Batch size exceeds maximum limit of 1,000 items")

    # Size validations
    total_data_size = 0
    for item in items:
        data_bytes = json.dumps(item.data or {}).encode("utf-8")
        item_data_size_kb = len(data_bytes) / 1024.0
        
        # Max size of data field per item is 200KB
        if item_data_size_kb > 200:
            raise HTTPException(status_code=400, detail="Item data field size exceeds limit of 200KB")
        total_data_size += len(data_bytes)

    # Average data field size across batch must be <= 30KB
    avg_data_size_kb = (total_data_size / len(items)) / 1024.0
    if avg_data_size_kb > 30:
        raise HTTPException(status_code=400, detail="Average data field size across batch exceeds limit of 30KB")
```
* Enforces exact limits on single-item size (200KB) and batch averages (30KB).
