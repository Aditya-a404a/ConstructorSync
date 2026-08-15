from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Literal, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, Field

from constructsync.engine.dlq import DeadLetterQueue
from constructsync.settings import get_settings

logger = logging.getLogger(__name__)

app = FastAPI(
    title="ConstructSync Pipeline API",
    description="A high-concurrency catalog ingestion, quality assurance, and security sanitization pipeline middleware",
    version="0.1.0"
)

# In-memory job tracker
JOBS: dict[str, dict[str, Any]] = {}
active_job_id: str | None = None


class IngestRequest(BaseModel):
    source: Literal["file", "bestbuy", "dummyjson", "kafka"] = "file"
    file_path: Optional[str] = None
    category: Optional[str] = None
    limit: Optional[int] = 5000
    target: Optional[str] = "constructor-mock"
    force_sync: Optional[bool] = False
    health_threshold: Optional[int] = None
    batch_size: Optional[int] = None
    concurrency: Optional[int] = None
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class RetryRequest(BaseModel):
    target: Optional[str] = "constructor-mock"
    base_url: Optional[str] = None
    api_key: Optional[str] = None


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/ingest", status_code=202)
async def start_ingestion(request: IngestRequest, background_tasks: BackgroundTasks):
    global active_job_id

    # Check if there is already a running job
    if active_job_id is not None and JOBS.get(active_job_id, {}).get("status") == "running":
        raise HTTPException(status_code=409, detail="An ingestion job is already running.")

    if request.source == "file" and not request.file_path:
        raise HTTPException(status_code=400, detail="file_path is required when source is 'file'")

    job_id = f"job_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    active_job_id = job_id

    JOBS[job_id] = {
        "job_id": job_id,
        "status": "running",
        "source": request.source,
        "total_items": 0,
        "items_sent": 0,
        "items_skipped": 0,
        "items_failed": 0,
        "batches_sent": 0,
        "batches_remaining": 0,
        "throughput": 0.0,
        "concurrency": request.concurrency or 4,
        "api_calls": 0,
        "retries": 0,
        "start_time": time.time(),
        "end_time": None,
        "error": None,
        "report": None,
    }

    background_tasks.add_task(run_ingestion_job, job_id, request)

    return {
        "job_id": job_id,
        "status": "running",
        "message": "Ingestion job started in background.",
    }


@app.get("/ingest/jobs")
async def list_jobs():
    return list(JOBS.values())


@app.get("/ingest/jobs/{job_id}")
async def get_job_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(status_code=404, detail="Job not found")
    return JOBS[job_id]


@app.get("/dlq")
async def list_dlq_items(
    reason: Optional[str] = None,
    sku: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    settings = get_settings()
    try:
        dlq = DeadLetterQueue(settings.dlq_database_path)
        items = dlq.list_items(reason=reason, sku=sku, limit=limit)
        return items
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/dlq/{item_id}")
async def get_dlq_item(item_id: int):
    settings = get_settings()
    try:
        dlq = DeadLetterQueue(settings.dlq_database_path)
        item = dlq.get_item(item_id)
        if not item:
            raise HTTPException(status_code=404, detail="DLQ item not found")
        return item
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.delete("/dlq/{item_id}")
async def delete_dlq_item(item_id: int):
    settings = get_settings()
    try:
        dlq = DeadLetterQueue(settings.dlq_database_path)
        item = dlq.get_item(item_id)
        if not item:
            raise HTTPException(status_code=404, detail="DLQ item not found")
        dlq.delete_items([item_id])
        return {"status": "success", "message": f"Deleted DLQ item {item_id}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post("/dlq/retry", status_code=202)
async def trigger_dlq_retry(request: RetryRequest, background_tasks: BackgroundTasks):
    global active_job_id

    # Check if there is already a running job
    if active_job_id is not None and JOBS.get(active_job_id, {}).get("status") == "running":
        raise HTTPException(status_code=409, detail="An ingestion or retry job is already running.")

    job_id = f"retry_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    active_job_id = job_id

    JOBS[job_id] = {
        "job_id": job_id,
        "status": "running",
        "source": "dlq_retry",
        "total_items": 0,
        "items_sent": 0,
        "items_skipped": 0,
        "items_failed": 0,
        "batches_sent": 0,
        "batches_remaining": 0,
        "throughput": 0.0,
        "concurrency": 1,
        "api_calls": 0,
        "retries": 0,
        "start_time": time.time(),
        "end_time": None,
        "error": None,
        "report": None,
    }

    background_tasks.add_task(run_dlq_retry_job, job_id, request)

    return {
        "job_id": job_id,
        "status": "running",
        "message": "DLQ retry started in background.",
    }


async def run_ingestion_job(job_id: str, request: IngestRequest):
    global active_job_id
    from constructsync.engine.engine import IngestionEngine
    from constructsync.engine.sanitizer import SanitizerStage
    from constructsync.engine.scorer import HealthScorer
    from constructsync.settings import get_settings
    from constructsync.engine.report import SyncReportGenerator

    settings = get_settings()

    sanitizer = SanitizerStage(
        text_fields=settings.sanitize_text_fields,
        id_fields=settings.sanitize_id_fields,
        numeric_fields=settings.sanitize_numeric_fields,
        url_fields=settings.sanitize_url_fields,
    )

    health_threshold = request.health_threshold or settings.health_threshold
    health_scorer = HealthScorer(threshold=health_threshold)

    # Determine base-url based on target argument
    base_url = request.base_url
    if request.target == "constructor-mock":
        base_url = "http://127.0.0.1:8001"
    elif not base_url:
        base_url = settings.constructor_base_url

    engine = IngestionEngine(
        file_path=request.file_path,
        source=request.source,
        category=request.category,
        limit=request.limit,
        health_threshold=health_threshold,
        force_sync=request.force_sync,
        settings=settings,
        batch_size=request.batch_size,
        concurrency=request.concurrency,
        base_url=base_url,
        api_key=request.api_key or settings.constructor_api_key,
        pipeline_stages=[sanitizer, health_scorer],
        show_progress=False,  # Disable stdout Live table plotting in engine
    )

    # Spawn polling task to update progress counters in real-time
    async def poll_progress():
        while True:
            try:
                cc = engine.concurrency
                if engine.concurrency_controller:
                    cc = engine.concurrency_controller.current_concurrency

                JOBS[job_id].update({
                    "total_items": engine.stats.total_items,
                    "items_sent": engine.stats.items_sent,
                    "items_skipped": engine.stats.items_skipped,
                    "items_failed": engine.stats.items_failed,
                    "batches_sent": engine.stats.batches_sent,
                    "batches_remaining": engine.stats.batches_remaining,
                    "throughput": engine.stats.items_per_second,
                    "concurrency": cc,
                    "api_calls": engine.stats.api_calls,
                    "retries": engine.stats.retries,
                })
            except Exception:
                pass
            await asyncio.sleep(0.5)

    poller = asyncio.create_task(poll_progress())

    try:
        await engine.run()
        
        # Retrieve SanitizerStage and HealthScorer stats to construct report
        sanitizer_stats = None
        health_scorer_stats = None
        health_scores = []
        for stage in engine.pipeline_stages:
            if hasattr(stage, "stats") and isinstance(stage.stats, dict) and "items_sanitized" in stage.stats:
                sanitizer_stats = stage.stats
            elif hasattr(stage, "get_stats") and callable(stage.get_stats):
                health_scorer_stats = stage.get_stats()
                health_scores = getattr(stage, "scores", [])

        report_dict = SyncReportGenerator.generate_report_dict(
            stats=engine.stats,
            sanitizer_stats=sanitizer_stats,
            health_scorer_stats=health_scorer_stats,
            health_scores=health_scores,
            peak_concurrency=engine.peak_concurrency,
        )

        JOBS[job_id].update({
            "status": "completed",
            "end_time": time.time(),
            "report": report_dict,
            "total_items": engine.stats.total_items,
            "items_sent": engine.stats.items_sent,
            "items_skipped": engine.stats.items_skipped,
            "items_failed": engine.stats.items_failed,
            "batches_sent": engine.stats.batches_sent,
            "batches_remaining": engine.stats.batches_remaining,
            "throughput": engine.stats.items_per_second,
            "concurrency": engine.peak_concurrency,
            "api_calls": engine.stats.api_calls,
            "retries": engine.stats.retries,
        })
    except Exception as e:
        logger.exception("Job %s failed", job_id)
        JOBS[job_id].update({
            "status": "failed",
            "end_time": time.time(),
            "error": str(e)
        })
    finally:
        poller.cancel()
        if active_job_id == job_id:
            active_job_id = None


async def run_dlq_retry_job(job_id: str, request: RetryRequest):
    global active_job_id
    from constructsync.engine.client import ConstructorClient
    from constructsync.engine.dlq import DeadLetterQueue
    from constructsync.settings import get_settings

    settings = get_settings()

    # Determine base-url based on target argument
    base_url = request.base_url
    if request.target == "constructor-mock":
        base_url = "http://127.0.0.1:8001"
    elif not base_url:
        base_url = settings.constructor_base_url

    try:
        dlq = DeadLetterQueue(settings.dlq_database_path)
        records = dlq.list_items(limit=10000)

        if not records:
            JOBS[job_id].update({
                "status": "completed",
                "end_time": time.time(),
                "report": {"message": "No items found in DLQ to retry."}
            })
            return

        JOBS[job_id].update({
            "total_items": len(records),
            "batches_remaining": (len(records) + 999) // 1000,
        })

        async with ConstructorClient(
            base_url=base_url,
            api_key=request.api_key or settings.constructor_api_key,
        ) as client:
            items_sent = 0
            items_failed = 0
            for i in range(0, len(records), 1000):
                chunk = records[i : i + 1000]
                payloads = [r["sanitized_data"] for r in chunk]

                result = await client.send_batch(payloads)
                if result.success:
                    db_ids = [r["id"] for r in chunk]
                    dlq.delete_items(db_ids)
                    items_sent += len(chunk)
                else:
                    items_failed += len(chunk)

                JOBS[job_id].update({
                    "items_sent": items_sent,
                    "items_failed": items_failed,
                    "batches_sent": (i // 1000) + 1,
                    "batches_remaining": max(0, ((len(records) - i) // 1000) - 1),
                })

        JOBS[job_id].update({
            "status": "completed",
            "end_time": time.time(),
            "report": {
                "items_sent": items_sent,
                "items_failed": items_failed,
                "message": f"Retry complete: {items_sent} succeeded, {items_failed} failed."
            }
        })
    except Exception as e:
        logger.exception("DLQ Retry Job %s failed", job_id)
        JOBS[job_id].update({
            "status": "failed",
            "end_time": time.time(),
            "error": str(e)
        })
    finally:
        if active_job_id == job_id:
            active_job_id = None


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
