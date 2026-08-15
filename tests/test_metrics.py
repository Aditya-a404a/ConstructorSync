"""
Tests for the Prometheus /metrics endpoint in main.py.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from constructsync.main import app


def test_metrics_endpoint_exposition_format():
    """Verify that /metrics returns valid Prometheus exposition text format with expected metrics."""
    client = TestClient(app)

    metrics_data = {
        "timestamp": 1234567890.0,
        "items_processed": {
            "success": 950,
            "failed": 10,
            "skipped": 40
        },
        "items_sanitized": 950,
        "api_requests": {
            "200": 4,
            "429": 1,
            "500": 1
        },
        "api_request_durations": [0.12, 0.45, 0.08],
        "batch_size": 250,
        "current_concurrency": 6,
        "health_scores": [100.0, 95.0, 80.0, 50.0]
    }

    metrics_file = Path("data/metrics.json")
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Write a real metrics file to data/metrics.json
    with open(metrics_file, "w") as f:
        json.dump(metrics_data, f)

    try:
        # Mock DeadLetterQueue to return 3 failed items
        with patch("constructsync.main.DeadLetterQueue") as mock_dlq_cls:
            mock_dlq_instance = mock_dlq_cls.return_value
            mock_dlq_instance.list_failed_items.return_value = [{}, {}, {}]

            response = client.get("/metrics")

        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]
        assert "version=0.0.4" in response.headers["content-type"]

        body = response.text

        # Verify HELP and TYPE definitions exist
        assert "# HELP constructsync_items_processed_total" in body
        assert "# TYPE constructsync_items_processed_total counter" in body
        
        # Verify labeled counters
        assert 'constructsync_items_processed_total{status="success"} 950.0' in body
        assert 'constructsync_items_processed_total{status="failed"} 10.0' in body
        assert 'constructsync_items_processed_total{status="skipped"} 40.0' in body

        # Verify sanitized counter
        assert 'constructsync_items_sanitized_total 950.0' in body

        # Verify API requests counter
        assert 'constructsync_api_requests_total{status_code="200"} 4.0' in body
        assert 'constructsync_api_requests_total{status_code="429"} 1.0' in body
        assert 'constructsync_api_requests_total{status_code="500"} 1.0' in body

        # Verify duration histogram
        assert 'constructsync_api_request_duration_seconds_bucket{le="0.1"} 1' in body
        assert 'constructsync_api_request_duration_seconds_bucket{le="0.5"} 3' in body
        assert 'constructsync_api_request_duration_seconds_bucket{le="+Inf"} 3' in body
        assert 'constructsync_api_request_duration_seconds_sum 0.65' in body
        assert 'constructsync_api_request_duration_seconds_count 3' in body

        # Verify gauges
        assert 'constructsync_batch_size 250.0' in body
        assert 'constructsync_current_concurrency 6.0' in body
        assert 'constructsync_dlq_depth 3.0' in body

        # Verify health score histogram
        assert 'constructsync_health_score_bucket{le="10.0"} 0' in body
        assert 'constructsync_health_score_bucket{le="50.0"} 1' in body
        assert 'constructsync_health_score_bucket{le="100.0"} 4' in body
        assert 'constructsync_health_score_sum 325.0' in body
        assert 'constructsync_health_score_count 4' in body
        
    finally:
        # Clean up the file
        if metrics_file.exists():
            metrics_file.unlink()
