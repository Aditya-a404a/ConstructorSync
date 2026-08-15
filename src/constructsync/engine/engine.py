"""
Ingestion Engine — the orchestrator.

Architecture:
    Reader (streaming) → asyncio.Queue → Worker Pool (semaphore-bounded) → API Client

Each worker: dequeue batch → run pipeline stages → POST to API → record result.
Rich live table displays real-time progress.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

from constructsync.engine.client import ConstructorClient
from constructsync.engine.controller import ConcurrencyController
from constructsync.engine.dlq import DeadLetterQueue
from constructsync.engine.hash_filter import HashFilterStage
from constructsync.engine.hash_store import HashStore
from constructsync.engine.models import BatchResult, IngestionStats, PipelineStage
from constructsync.engine.reader import CatalogReader
from constructsync.engine.report import SyncReportGenerator
from constructsync.settings import ConstructSyncSettings, get_settings

logger = logging.getLogger(__name__)

# Sentinel value to signal workers to stop
_STOP = None

# Column names for the Constructor API item schema
_CSV_TO_CONSTRUCTOR_MAP = {
    "sku": "id",
    "name": "name",
    "price": "price",
    "description": "description",
    "image_url": "image_url",
    "category": "category",
    "brand": "brand",
}


def _map_item(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Map a raw CSV/JSONL row to the Constructor API item format.

    Constructor expects:
        { "id": str, "name": str, "url": str?, "image_url": str?, "data": {...} }

    We map 'sku' → 'id' and pack remaining fields into 'data'.
    """
    item_id = str(raw.get("sku") or raw.get("id") or "")
    name = str(raw.get("name") or raw.get("title") or "")

    # Pack everything else into the 'data' dict
    data: dict[str, Any] = {}
    for key, value in raw.items():
        lower_key = key.lower()
        if lower_key in ("sku", "id", "name", "title", "image_url", "url"):
            continue
        if value is not None and value != "":
            data[lower_key] = value

    return {
        "id": item_id,
        "name": name,
        "url": str(raw.get("url", "")),
        "image_url": str(raw.get("image_url", "")),
        "data": data,
    }


def _build_progress_table(stats: IngestionStats, concurrency: int) -> Table:
    """Build a Rich table showing live ingestion progress."""
    elapsed = stats.elapsed_seconds
    minutes, seconds = divmod(int(elapsed), 60)

    table = Table(
        title="⚡ ConstructSync Ingestion",
        show_header=True,
        header_style="bold cyan",
        border_style="bright_blue",
        title_style="bold white",
    )
    table.add_column("Metric", style="bold", width=22)
    table.add_column("Value", justify="right", width=18)

    # Progress
    if stats.batches_total > 0:
        pct = (stats.batches_sent + stats.batches_failed) / stats.batches_total * 100
    else:
        pct = 0

    bar_width = 12
    filled = int(bar_width * pct / 100)
    bar = "█" * filled + "░" * (bar_width - filled)

    table.add_row("Progress", f"{bar} {pct:.1f}%")
    table.add_row("Elapsed", f"{minutes:02d}:{seconds:02d}")
    table.add_row("", "")  # spacer

    table.add_row(
        "Items Sent",
        Text(f"{stats.items_sent:,}", style="green"),
    )
    table.add_row(
        "Items Skipped",
        Text(f"{stats.items_skipped:,}", style="cyan" if stats.items_skipped > 0 else "dim"),
    )
    table.add_row(
        "Items Failed",
        Text(f"{stats.items_failed:,}", style="red" if stats.items_failed > 0 else "dim"),
    )
    table.add_row("Total Items", f"{stats.total_items:,}")
    table.add_row("", "")

    # Batches
    table.add_row("Batches Sent", f"{stats.batches_sent:,}")
    table.add_row(
        "Batches Remaining",
        Text(f"{stats.batches_remaining:,}", style="yellow"),
    )
    table.add_row("", "")

    # Performance
    table.add_row(
        "Throughput",
        Text(f"{stats.items_per_second:,.0f} items/sec", style="bold green"),
    )
    table.add_row("Concurrency", f"{concurrency}")
    table.add_row("API Calls", f"{stats.api_calls:,}")
    table.add_row("Retries", f"{stats.retries:,}")

    # Status code breakdown (compact)
    if stats.status_codes:
        codes_str = " ".join(
            f"{code}:{count}" for code, count in sorted(stats.status_codes.items())
        )
        table.add_row("Status Codes", codes_str)

    return table


class IngestionEngine:
    """
    Orchestrates the full ingestion pipeline.

    1. Reads the catalog file in streaming batches.
    2. Pushes batches to an asyncio.Queue.
    3. N worker coroutines pull from the queue, bounded by a Semaphore.
    4. Each worker runs pipeline stages then POSTs to the API.
    5. Rich live table displays progress in real-time.
    """

    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 1.0  # seconds

    def __init__(
        self,
        file_path: str | Path | None = None,
        source: str = "file",
        category: str | None = None,
        limit: int | None = None,
        health_threshold: int | None = None,
        force_sync: bool = False,
        kafka_topic: str | None = None,
        kafka_bootstrap_servers: str | None = None,
        kafka_group_id: str | None = None,
        settings: ConstructSyncSettings | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        pipeline_stages: list[PipelineStage] | None = None,
        show_progress: bool = True,
    ) -> None:
        self.settings = settings or get_settings()
        self.show_progress = show_progress
        self.file_path = Path(file_path) if file_path is not None else None
        self.source = source.lower()
        self.category = category
        self.limit = limit or 5000
        self.health_threshold = health_threshold or self.settings.health_threshold
        self.force_sync = force_sync
        self.kafka_topic = kafka_topic or self.settings.kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or self.settings.kafka_bootstrap_servers
        self.kafka_group_id = kafka_group_id or self.settings.kafka_group_id
        self.batch_size = batch_size or self.settings.default_batch_size
        self.concurrency = concurrency or self.settings.initial_concurrency
        self.base_url = base_url or self.settings.constructor_base_url
        self.api_key = api_key or self.settings.constructor_api_key
        self.pipeline_stages = pipeline_stages or []

        self.min_concurrency = self.settings.min_concurrency
        self.max_concurrency = self.settings.max_concurrency

        self.stats = IngestionStats()
        self._shutdown = False
        # Queue and ConcurrencyController are created in run() to avoid
        # Python 3.9 "attached to a different loop" errors.
        self._queue: asyncio.Queue[list[dict] | None] | None = None
        self.concurrency_controller: ConcurrencyController | None = None
        self.dlq: DeadLetterQueue | None = None
        self.peak_concurrency = self.concurrency

    async def run(self) -> IngestionStats:
        """
        Execute the full ingestion pipeline.

        Returns:
            IngestionStats with final counters.
        """
        console = Console()

        # Select reader based on source
        if self.source in ("bestbuy", "dummyjson"):
            from constructsync.engine.dummyjson import DummyJSONReader
            reader = DummyJSONReader(
                category=self.category,
                limit=self.limit,
                batch_size=self.batch_size,
            )
        elif self.source == "kafka":
            reader = None
        else:
            if not self.file_path:
                raise ValueError("File path must be provided when source is 'file'")
            reader = CatalogReader(self.file_path, batch_size=self.batch_size)

        # Create asyncio primitives in the running loop (Python 3.9 compat)
        self._queue = asyncio.Queue(maxsize=self.concurrency * 2)
        self.concurrency_controller = ConcurrencyController(
            initial_concurrency=self.concurrency,
            min_concurrency=self.min_concurrency,
            max_concurrency=self.max_concurrency,
        )
        self.dlq = DeadLetterQueue(self.settings.dlq_database_path)
        
        # Initialize HashStore and HashFilterStage
        self.hash_store = HashStore(self.settings.hash_store_database_path)
        self.hash_filter = HashFilterStage(hash_store=self.hash_store, force_sync=self.force_sync)
        if not any(isinstance(s, HashFilterStage) for s in self.pipeline_stages):
            self.pipeline_stages.append(self.hash_filter)

        self.peak_concurrency = self.concurrency

        # Count total rows for progress tracking
        if self.source == "kafka":
            total_rows = 0
            self.stats.total_items = 0
            self.stats.batches_total = 0
        else:
            target_name = self.file_path.name if self.file_path else f"Live API ({self.source})"
            if self.show_progress:
                console.print(
                    f"[dim]Counting rows in {target_name}...[/dim]"
                )
            total_rows = reader.count_rows()
            self.stats.total_items = total_rows
            self.stats.batches_total = (total_rows + self.batch_size - 1) // self.batch_size

        if self.show_progress:
            console.print(
                f"[bold]Starting ingestion:[/bold] "
                f"{total_rows:,} items → {self.stats.batches_total:,} batches "
                f"(batch_size={self.batch_size}, concurrency={self.concurrency})"
            )
            console.print(
                f"[dim]Target: {self.base_url}/v2/items[/dim]"
            )
            console.print()

        self.stats.start_time = time.monotonic()

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._signal_shutdown)

        # Write initial metrics
        self._write_metrics_json()

        async with ConstructorClient(
            base_url=self.base_url,
            api_key=self.api_key,
            max_connections=self.concurrency,
        ) as client:
            # Launch producer and workers
            if self.source == "kafka":
                producer = asyncio.create_task(
                    self._produce_kafka_events(), name="kafka-producer"
                )
            else:
                producer = asyncio.create_task(
                    self._produce_batches(reader), name="producer"
                )
            workers = [
                asyncio.create_task(
                    self._worker(client, worker_id=i), name=f"worker-{i}"
                )
                for i in range(self.concurrency)
            ]

            if self.show_progress:
                # Live progress display
                with Live(
                    _build_progress_table(self.stats, self.concurrency_controller.current_concurrency),
                    console=console,
                    refresh_per_second=4,
                    transient=False,
                ) as live:
                    # Update progress table while workers are running
                    while not producer.done() or not all(w.done() for w in workers):
                        cc = self.concurrency_controller.current_concurrency
                        self.peak_concurrency = max(self.peak_concurrency, cc)
                        self.stats.items_skipped = self.hash_filter.stats["items_skipped"]
                        live.update(
                            _build_progress_table(self.stats, cc)
                        )
                        self._write_metrics_json()
                        await asyncio.sleep(0.25)

                    # Final update
                    cc = self.concurrency_controller.current_concurrency
                    self.peak_concurrency = max(self.peak_concurrency, cc)
                    self.stats.items_skipped = self.hash_filter.stats["items_skipped"]
                    live.update(
                        _build_progress_table(self.stats, cc)
                    )
                    self._write_metrics_json()
            else:
                while not producer.done() or not all(w.done() for w in workers):
                    cc = self.concurrency_controller.current_concurrency
                    self.peak_concurrency = max(self.peak_concurrency, cc)
                    self.stats.items_skipped = self.hash_filter.stats["items_skipped"]
                    self._write_metrics_json()
                    await asyncio.sleep(0.25)
                # Final update
                cc = self.concurrency_controller.current_concurrency
                self.peak_concurrency = max(self.peak_concurrency, cc)
                self.stats.items_skipped = self.hash_filter.stats["items_skipped"]
                self._write_metrics_json()

            # Collect any exceptions
            await producer
            await asyncio.gather(*workers, return_exceptions=True)

        # Retrieve SanitizerStage and HealthScorer stats if present
        sanitizer_stats = None
        health_scorer_stats = None
        health_scores = []
        for stage in self.pipeline_stages:
            if hasattr(stage, "stats") and isinstance(stage.stats, dict) and "items_sanitized" in stage.stats:
                sanitizer_stats = stage.stats
            elif hasattr(stage, "get_stats") and callable(stage.get_stats):
                health_scorer_stats = stage.get_stats()
                health_scores = getattr(stage, "scores", [])

        self.stats.items_skipped = self.hash_filter.stats["items_skipped"]

        # Generate final report
        report_dict = SyncReportGenerator.generate_report_dict(
            stats=self.stats,
            sanitizer_stats=sanitizer_stats,
            health_scorer_stats=health_scorer_stats,
            health_scores=health_scores,
            peak_concurrency=self.peak_concurrency,
        )
        report_file = SyncReportGenerator.write_json_report(report_dict)
        if self.show_progress:
            console.print(f"[dim]Sync report written to {report_file}[/dim]")

            # Print report in console
            SyncReportGenerator.print_console_report(report_dict, console=console)

        return self.stats

    async def _produce_batches(self, reader: CatalogReader) -> None:
        """Read the file and push batches to the queue."""
        try:
            for batch in reader.read_batches():
                if self._shutdown:
                    logger.info("Shutdown requested — stopping producer.")
                    break

                # Map raw rows to Constructor item format
                mapped_batch = [_map_item(row) for row in batch]
                await self._queue.put(mapped_batch)

        except Exception as e:
            logger.error("Producer error: %s", e)
            raise
        finally:
            # Send stop signal to all workers
            for _ in range(self.concurrency):
                await self._queue.put(_STOP)

    async def _worker(self, client: ConstructorClient, worker_id: int) -> None:
        """Worker coroutine: pull batches from queue, process, and send."""
        while True:
            batch = await self._queue.get()
            if batch is _STOP:
                self._queue.task_done()
                return

            try:
                async with self.concurrency_controller:
                    await self._process_and_send(client, batch, worker_id)
            except Exception as e:
                logger.error("Worker %d unhandled error: %s", worker_id, e)
                self.stats.items_failed += len(batch)
                self.stats.batches_failed += 1
            finally:
                self._queue.task_done()

    async def _process_and_send(
        self,
        client: ConstructorClient,
        batch: list[dict],
        worker_id: int,
    ) -> None:
        """Run pipeline stages then send batch to API, with retries."""
        # Run pipeline stages (future: sanitize → health-score → etc.)
        processed = batch
        for stage in self.pipeline_stages:
            processed = await stage.process(processed)
            if not processed:
                logger.info(
                    "Worker %d: all items in batch skipped (already synced)", worker_id
                )
                self.stats.batches_sent += 1
                self._write_metrics_json()
                return

        # Send with retries
        for attempt in range(1, self.MAX_RETRIES + 1):
            result = await client.send_batch(processed)
            self.stats.api_calls += 1
            self.stats.record_status_code(result.status_code)
            if hasattr(result, "latency_ms") and result.latency_ms is not None:
                self.stats.latencies.append(result.latency_ms / 1000.0)

            # Register result with ConcurrencyController
            await self.concurrency_controller.register_result(result.success, result.status_code)

            if result.success:
                self.stats.items_sent += result.item_count
                self.stats.batches_sent += 1
                # Commit hashes for successfully sent items
                self.hash_filter.commit_hashes([item.get("id") or item.get("sku") for item in processed])
                self._write_metrics_json()
                return

            # Retry on server errors (5xx) and rate limits (429)
            if result.status_code in (429, 500, 502, 503, 504) and attempt < self.MAX_RETRIES:
                delay = self.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                import random
                
                # Extra backoff with jitter on 429
                if result.status_code == 429:
                    delay = delay * 2 * random.uniform(0.5, 1.5)
                else:
                    delay = delay * random.uniform(0.8, 1.2)
                    
                logger.warning(
                    "Worker %d: %d on attempt %d/%d — retrying in %.1fs",
                    worker_id,
                    result.status_code,
                    attempt,
                    self.MAX_RETRIES,
                    delay,
                )
                self.stats.retries += 1
                await asyncio.sleep(delay)
                continue

            # Non-retryable error or max retries exhausted
            break

        # All retries exhausted — record failure
        logger.error(
            "Worker %d: batch FAILED after %d attempts (status=%d, error=%s)",
            worker_id,
            self.MAX_RETRIES,
            result.status_code,
            result.error_message,
        )
        self.stats.items_failed += result.item_count
        self.stats.batches_failed += 1
        self._write_metrics_json()

        # Save failed batch items into Dead-Letter Queue
        self.dlq.insert_failed_items(
            items=processed,
            reason=f"HTTP {result.status_code}: {result.error_message}",
            retry_count=self.MAX_RETRIES,
        )

    async def _produce_kafka_events(self) -> None:
        """Consume events from Kafka, micro-batch, and push to queue."""
        import json
        from aiokafka import AIOKafkaConsumer

        logger.info("Connecting to Kafka topic '%s' at %s...", self.kafka_topic, self.kafka_bootstrap_servers)

        consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest"
        )

        try:
            await consumer.start()
            logger.info("Kafka consumer started successfully.")
            is_mock = False
        except Exception as e:
            logger.warning("Could not connect to Kafka broker (%s). Falling back to mock simulation mode...", e)
            is_mock = True

        try:
            collected_events = []
            last_flush_time = time.monotonic()

            if is_mock:
                import random
                adjectives = ["Premium", "Ultra", "Pro", "Eco", "Smart", "Classic", "Deluxe", "Sleek"]
                products = ["Laptop", "Headphones", "Monitor", "Speaker", "Phone Case", "Desk Lamp", "Yoga Mat", "Water Bottle"]
                categories = ["Electronics", "Computers", "Audio", "Home & Kitchen", "Sports & Outdoors"]
                brands = ["TechVault", "NovaPeak", "EcoSphere", "ApexWave", "VoltCraft"]

                # Emit 1,000 events incrementally
                for i in range(1, 1001):
                    if self._shutdown:
                        break

                    sku = f"SKU-KAFKA-{i:04d}"
                    # Occasional delete event to test deleted event skipping
                    if random.random() < 0.02:
                        event = {"event": "product.deleted", "sku": sku, "data": {}}
                    else:
                        event = {
                            "event": "product.updated",
                            "sku": sku,
                            "data": {
                                "name": f"{random.choice(adjectives)} {random.choice(products)}",
                                "price": round(random.uniform(9.99, 1499.99), 2),
                                "description": f"Experience the incredible product from {random.choice(brands)}. Exceeds fifty characters for perfect score.",
                                "image_url": f"https://cdn.example.com/products/{sku}.jpg",
                                "category": random.choice(categories),
                                "brand": random.choice(brands),
                            }
                        }

                    evt_name = event.get("event")
                    sku = event.get("sku")

                    if evt_name == "product.deleted":
                        logger.info("Received delete event for SKU %s (not processed/sent)", sku)
                        continue

                    if evt_name in ("product.created", "product.updated"):
                        raw_item = {
                            "sku": sku,
                            **(event.get("data") or {})
                        }
                        collected_events.append(raw_item)

                    # Simulate real-time stream arrival
                    await asyncio.sleep(0.005)

                    # Flush check
                    now = time.monotonic()
                    if collected_events and (len(collected_events) >= self.batch_size or (now - last_flush_time) >= 5.0):
                        mapped_batch = [_map_item(e) for e in collected_events]
                        self.stats.total_items += len(mapped_batch)
                        self.stats.batches_total += 1
                        await self._queue.put(mapped_batch)
                        collected_events = []
                        last_flush_time = now
            else:
                while not self._shutdown:
                    try:
                        # Retrieve one message with a short timeout to allow check of shutdown flag
                        msg = await asyncio.wait_for(consumer.getone(), timeout=0.5)
                        event = msg.value

                        if isinstance(event, dict):
                            evt_name = event.get("event")
                            sku = event.get("sku")

                            if evt_name == "product.deleted":
                                logger.info("Received delete event for SKU %s (not processed/sent)", sku)
                                continue

                            if evt_name in ("product.created", "product.updated"):
                                raw_item = {
                                    "sku": sku,
                                    **(event.get("data") or {})
                                }
                                collected_events.append(raw_item)

                    except asyncio.TimeoutError:
                        pass
                    except Exception as e:
                        logger.error("Error retrieving event from Kafka: %s", e)
                        await asyncio.sleep(1.0)

                    # Flush batch if batch size reached or 5 seconds elapsed
                    now = time.monotonic()
                    if collected_events and (len(collected_events) >= self.batch_size or (now - last_flush_time) >= 5.0):
                        mapped_batch = [_map_item(e) for e in collected_events]
                        self.stats.total_items += len(mapped_batch)
                        self.stats.batches_total += 1
                        await self._queue.put(mapped_batch)
                        collected_events = []
                        last_flush_time = now

            # Flush any remaining items before shutdown
            if collected_events:
                mapped_batch = [_map_item(e) for e in collected_events]
                self.stats.total_items += len(mapped_batch)
                self.stats.batches_total += 1
                await self._queue.put(mapped_batch)

        except Exception as e:
            logger.error("Kafka producer task error: %s", e)
            raise
        finally:
            logger.info("Stopping Kafka consumer...")
            try:
                await consumer.stop()
            except Exception:
                pass
            # Send stop signal to all workers
            for _ in range(self.concurrency):
                await self._queue.put(_STOP)

    def _signal_shutdown(self) -> None:
        """Handle SIGINT/SIGTERM for graceful shutdown."""
        if not self._shutdown:
            logger.info("Shutdown signal received — draining queue...")
            self._shutdown = True

    def _write_metrics_json(self) -> None:
        """Write current stats to a shared JSON file for Prometheus observability."""
        import json
        import os
        metrics_file = Path("data/metrics.json")
        os.makedirs(metrics_file.parent, exist_ok=True)

        items_sanitized = 0
        for stage in self.pipeline_stages:
            if hasattr(stage, "stats") and isinstance(stage.stats, dict) and "items_sanitized" in stage.stats:
                items_sanitized = stage.stats["items_sanitized"]

        health_scores = []
        for stage in self.pipeline_stages:
            if hasattr(stage, "scores") and isinstance(stage.scores, list):
                health_scores = stage.scores

        data = {
            "timestamp": time.time(),
            "items_processed": {
                "success": self.stats.items_sent,
                "failed": self.stats.items_failed,
                "skipped": self.stats.items_skipped,
            },
            "items_sanitized": items_sanitized,
            "api_requests": {str(code): count for code, count in self.stats.status_codes.items()},
            "api_request_durations": getattr(self.stats, "latencies", []),
            "batch_size": self.batch_size,
            "current_concurrency": self.concurrency_controller.current_concurrency,
            "health_scores": health_scores,
        }

        # Write atomically using a temp file to prevent concurrent read mangling
        temp_file = metrics_file.with_suffix(".tmp")
        try:
            with open(temp_file, "w") as f:
                json.dump(data, f)
            os.replace(temp_file, metrics_file)
        except Exception as e:
            logger.warning("Failed to write live metrics file: %s", e)
