from __future__ import annotations

import json
import time
from pathlib import Path
from fastapi import FastAPI, Response

from constructsync.engine.dlq import DeadLetterQueue
from constructsync.settings import get_settings

app = FastAPI(
    title="ConstructSync Pipeline API",
    description="A high-concurrency catalog ingestion, quality assurance, and security sanitization pipeline middleware",
    version="0.1.0"
)


@app.get("/health")
async def health_check():
    return {"status": "ok"}


def format_metric_lines(
    name: str,
    type_str: str,
    help_str: str,
    series: list[tuple[dict[str, str], float]]
) -> str:
    """Format single or multi-labeled series lines for a metric."""
    lines = [
        f"# HELP {name} {help_str}",
        f"# TYPE {name} {type_str}",
    ]
    for labels, val in series:
        if labels:
            label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
            lines.append(f"{name}{{{label_str}}} {val}")
        else:
            lines.append(f"{name} {val}")
    return "\n".join(lines) + "\n"


def format_histogram(
    name: str,
    values: list[float],
    help_str: str,
    buckets: list[float]
) -> str:
    """Format standard Prometheus histogram bucket lines, sum, and count."""
    lines = [
        f"# HELP {name} {help_str}",
        f"# TYPE {name} histogram",
    ]
    bucket_counts = {b: 0 for b in buckets}
    for val in values:
        for b in buckets:
            if val <= b:
                bucket_counts[b] += 1

    for b in buckets:
        le_str = "+Inf" if b == float("inf") else str(b)
        lines.append(f'{name}_bucket{{le="{le_str}"}} {bucket_counts[b]}')

    lines.append(f"{name}_sum {sum(values)}")
    lines.append(f"{name}_count {len(values)}")
    return "\n".join(lines) + "\n"


@app.get("/metrics")
async def get_metrics():
    settings = get_settings()

    # 1. Query DLQ depth live from SQLite DB
    dlq_depth = 0
    try:
        dlq = DeadLetterQueue(settings.dlq_database_path)
        dlq_depth = len(dlq.list_failed_items())
    except Exception:
        pass

    # 2. Read live metrics from the shared JSON file
    metrics_path = Path("data/metrics.json")
    metrics_data = {}
    is_active = False

    if metrics_path.exists():
        try:
            mtime = metrics_path.stat().st_mtime
            # If the metrics file has not been modified in > 15s, engine is inactive
            if time.time() - mtime < 15.0:
                with open(metrics_path, "r") as f:
                    metrics_data = json.load(f)
                is_active = True
        except Exception:
            pass

    # Extract metrics values with safe fallbacks
    items_proc = metrics_data.get("items_processed", {"success": 0, "failed": 0, "skipped": 0})
    items_sanitized = metrics_data.get("items_sanitized", 0)
    api_reqs = metrics_data.get("api_requests", {})
    api_durations = metrics_data.get("api_request_durations", [])
    batch_size = metrics_data.get("batch_size", 0)
    curr_concurrency = metrics_data.get("current_concurrency", 0) if is_active else 0
    health_scores = metrics_data.get("health_scores", [])

    output = []

    # constructsync_items_processed_total
    proc_series = [
        ({"status": "success"}, float(items_proc.get("success", 0))),
        ({"status": "failed"}, float(items_proc.get("failed", 0))),
        ({"status": "skipped"}, float(items_proc.get("skipped", 0))),
    ]
    output.append(
        format_metric_lines(
            "constructsync_items_processed_total",
            "counter",
            "Total number of items processed",
            proc_series
        )
    )

    # constructsync_items_sanitized_total
    san_series = [
        ({}, float(items_sanitized)),
    ]
    output.append(
        format_metric_lines(
            "constructsync_items_sanitized_total",
            "counter",
            "Total number of items sanitized",
            san_series
        )
    )

    # constructsync_api_requests_total
    api_series = [
        ({"status_code": "200"}, float(api_reqs.get("200", 0))),
        ({"status_code": "429"}, float(api_reqs.get("429", 0))),
        ({"status_code": "500"}, float(api_reqs.get("500", 0))),
    ]
    output.append(
        format_metric_lines(
            "constructsync_api_requests_total",
            "counter",
            "Total number of API requests sent",
            api_series
        )
    )

    # constructsync_api_request_duration_seconds
    dur_buckets = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, float("inf")]
    output.append(
        format_histogram(
            "constructsync_api_request_duration_seconds",
            api_durations,
            "Ingestion API request duration in seconds",
            dur_buckets
        )
    )

    # constructsync_batch_size
    output.append(
        format_metric_lines(
            "constructsync_batch_size",
            "gauge",
            "Current ingestion batch size",
            [({}, float(batch_size))]
        )
    )

    # constructsync_current_concurrency
    output.append(
        format_metric_lines(
            "constructsync_current_concurrency",
            "gauge",
            "Current ingestion worker concurrency",
            [({}, float(curr_concurrency))]
        )
    )

    # constructsync_dlq_depth
    output.append(
        format_metric_lines(
            "constructsync_dlq_depth",
            "gauge",
            "Current Dead-Letter Queue depth",
            [({}, float(dlq_depth))]
        )
    )

    # constructsync_health_score
    health_buckets = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, float("inf")]
    output.append(
        format_histogram(
            "constructsync_health_score",
            health_scores,
            "Searchability health score distribution",
            health_buckets
        )
    )

    return Response(
        content="".join(output),
        media_type="text/plain; version=0.0.4; charset=utf-8"
    )
