import os
import json
import pytest
import sqlite3
from fastapi.testclient import TestClient

# Set env before importing the app
os.environ["CONSTRUCTOR_API_KEY"] = "test_key"
os.environ["CONSTRUCTOR_MOCK_DB_PATH"] = "data/test_constructor_mock.db"

from constructsync.mock_api import app, get_db_connection, init_db, chaos_config

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_db():
    # Setup test database
    init_db()
    # Clear any previous data
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM items")
    cursor.execute("DELETE FROM tasks")
    conn.commit()
    conn.close()
    # Reset chaos config
    chaos_config.update({
        "enabled": False,
        "error_rate": 0.0,
        "latency_min_ms": 0,
        "latency_max_ms": 0,
        "timeout_rate": 0.0,
    })
    yield

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_auth_failure():
    payload = {"items": [{"id": "1", "name": "Item 1"}]}
    # No Auth Header
    response = client.post("/v2/items", json=payload)
    assert response.status_code == 401
    
    # Invalid Auth Header
    response = client.post("/v2/items", json=payload, headers={"Authorization": "Bearer wrong"})
    assert response.status_code == 401

def test_successful_ingestion():
    payload = {
        "items": [
            {
                "id": "item-100",
                "name": "Super Cool Shoes",
                "url": "https://example.com/shoes",
                "image_url": "https://example.com/shoes.jpg",
                "data": {"brand": "Nike", "price": 99.99}
            }
        ]
    }
    headers = {"Authorization": "Bearer test_key"}
    response = client.post("/v2/items", json=payload, headers=headers)
    
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert "task_status_path" in data
    
    task_id = data["task_id"]
    
    # Check task status
    status_response = client.get(f"/v2/tasks/{task_id}", headers=headers)
    assert status_response.status_code == 200
    assert status_response.json()["status"] in ["QUEUED", "PROCESSING", "COMPLETED"]

def test_batch_size_limit():
    # Build a batch of 1001 items
    items = [{"id": f"item-{i}", "name": f"Item {i}"} for i in range(1001)]
    payload = {"items": items}
    headers = {"Authorization": "Bearer test_key"}
    
    response = client.post("/v2/items", json=payload, headers=headers)
    assert response.status_code == 400
    assert "exceeds maximum limit" in response.json()["detail"]

def test_item_size_limit():
    # Build an item with data > 200KB
    large_data = "a" * (205 * 1024) # 205 KB
    payload = {
        "items": [
            {
                "id": "too-big",
                "name": "Big Data Item",
                "data": {"content": large_data}
            }
        ]
    }
    headers = {"Authorization": "Bearer test_key"}
    response = client.post("/v2/items", json=payload, headers=headers)
    assert response.status_code == 400
    assert "exceeds limit of 200KB" in response.json()["detail"]

def test_average_size_limit():
    # Average size across the batch must be <= 30KB.
    # We send 2 items: one 55KB, one 1KB. Avg is 28KB -> Allowed.
    # If we send 2 items: one 59KB, one 5KB. Avg is 32KB -> Rejected.
    headers = {"Authorization": "Bearer test_key"}
    
    # Under limit (average 28KB)
    payload_ok = {
        "items": [
            {"id": "item-1", "name": "Item 1", "data": {"content": "a" * (55 * 1024)}},
            {"id": "item-2", "name": "Item 2", "data": {"content": "a" * (1 * 1024)}}
        ]
    }
    response = client.post("/v2/items", json=payload_ok, headers=headers)
    assert response.status_code == 200
    
    # Over limit (average 32KB)
    payload_bad = {
        "items": [
            {"id": "item-1", "name": "Item 1", "data": {"content": "a" * (59 * 1024)}},
            {"id": "item-2", "name": "Item 2", "data": {"content": "a" * (5 * 1024)}}
        ]
    }
    response = client.post("/v2/items", json=payload_bad, headers=headers)
    assert response.status_code == 400
    assert "Average data field size across batch" in response.json()["detail"]

def test_rate_limiting():
    # Simulate 1,000 active tasks by inserting them directly into SQLite
    conn = get_db_connection()
    cursor = conn.cursor()
    for i in range(1000):
        cursor.execute(
            "INSERT INTO tasks (id, status) VALUES (?, 'QUEUED')",
            (f"task-{i}",)
        )
    conn.commit()
    conn.close()
    
    # Post a new request, should trigger 429
    payload = {"items": [{"id": "item-new", "name": "New Item"}]}
    headers = {"Authorization": "Bearer test_key"}
    response = client.post("/v2/items", json=payload, headers=headers)
    
    assert response.status_code == 429
    assert response.headers.get("X-RateLimit-Tasks-Remaining") == "0"

def test_chaos_mode_error():
    # Configure 100% error rate
    chaos_payload = {
        "enabled": True,
        "error_rate": 1.0,
        "latency_min_ms": 0,
        "latency_max_ms": 0,
        "timeout_rate": 0.0
    }
    response = client.post("/admin/chaos", json=chaos_payload)
    assert response.status_code == 200
    
    # Make request to items endpoint, should return 500
    headers = {"Authorization": "Bearer test_key"}
    payload = {"items": [{"id": "item-new", "name": "New Item"}]}
    response = client.post("/v2/items", json=payload, headers=headers)
    assert response.status_code == 500
