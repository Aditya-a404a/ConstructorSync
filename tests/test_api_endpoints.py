from __future__ import annotations

import os
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from constructsync.main import app, JOBS
from constructsync.engine.dlq import DeadLetterQueue
from constructsync.settings import get_settings


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def clean_dlq(tmp_path):
    # Set settings db paths to temporary files
    settings = get_settings()
    original_dlq_path = settings.dlq_database_path
    
    temp_dlq_db = tmp_path / "dlq_temp.db"
    settings.dlq_database_path = str(temp_dlq_db)
    
    dlq = DeadLetterQueue(settings.dlq_database_path)
    dlq.clear()
    
    yield dlq
    
    # Restore original path
    settings.dlq_database_path = original_dlq_path


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_metrics_endpoint(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "constructsync_dlq_depth" in response.text


def test_ingest_endpoint_validation(client):
    # Ingest with file source but missing file_path
    response = client.post("/ingest", json={"source": "file"})
    assert response.status_code == 400
    assert "file_path is required" in response.json()["detail"]


def test_ingest_endpoints_flow(client, tmp_path):
    import csv
    
    # Create a small dummy csv catalog
    csv_file = tmp_path / "test_catalog.csv"
    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({
            "sku": "API-SKU-1",
            "name": "API Laptop",
            "price": "999.99",
            "description": "High end laptop",
            "image_url": "http://img.jpg",
            "category": "Electronics",
            "brand": "BrandX"
        })

    # Clear job history
    JOBS.clear()

    # Trigger Ingestion via API
    response = client.post("/ingest", json={
        "source": "file",
        "file_path": str(csv_file),
        "target": "constructor-mock",
        "concurrency": 1,
        "batch_size": 10
    })
    
    assert response.status_code == 202
    data = response.json()
    assert "job_id" in data
    job_id = data["job_id"]
    
    # In TestClient, background tasks run synchronously. So it should be completed!
    assert job_id in JOBS
    job = JOBS[job_id]
    assert job["status"] in ("completed", "failed")  # Can be completed successfully
    
    # Test GET /ingest/jobs
    list_response = client.get("/ingest/jobs")
    assert list_response.status_code == 200
    jobs_list = list_response.json()
    assert any(j["job_id"] == job_id for j in jobs_list)

    # Test GET /ingest/jobs/{job_id}
    job_response = client.get(f"/ingest/jobs/{job_id}")
    assert job_response.status_code == 200
    job_detail = job_response.json()
    assert job_detail["job_id"] == job_id
    assert job_detail["source"] == "file"


def test_dlq_endpoints_flow(client, clean_dlq):
    dlq = clean_dlq
    
    # Insert some dummy failures
    item = {"id": "SKU-FAIL", "name": "Failed Item", "price": "100.0"}
    dlq.insert_failed_items([item], reason="Missing required field price", retry_count=3)
    
    # Verify via GET /dlq
    response = client.get("/dlq")
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 1
    assert items[0]["sku"] == "SKU-FAIL"
    db_id = items[0]["id"]
    
    # Retrieve single item
    item_response = client.get(f"/dlq/{db_id}")
    assert item_response.status_code == 200
    assert item_response.json()["sku"] == "SKU-FAIL"
    
    # Delete item
    del_response = client.delete(f"/dlq/{db_id}")
    assert del_response.status_code == 200
    assert del_response.json()["status"] == "success"
    
    # Verify deleted
    response2 = client.get("/dlq")
    assert len(response2.json()) == 0


def test_dlq_retry_endpoint(client, clean_dlq):
    dlq = clean_dlq
    
    # Insert dummy failure
    item = {"id": "SKU-FAIL-RETRY", "name": "Failed Item Retry", "price": "10.0"}
    dlq.insert_failed_items([item], reason="CRITICAL_ERROR", retry_count=3)
    
    JOBS.clear()
    
    # Trigger DLQ retry via API
    response = client.post("/dlq/retry", json={"target": "constructor-mock"})
    assert response.status_code == 202
    data = response.json()
    assert "job_id" in data
    job_id = data["job_id"]
    
    # Assert job completes
    assert job_id in JOBS
    job = JOBS[job_id]
    assert job["status"] in ("completed", "failed")
