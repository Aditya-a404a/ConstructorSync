"""
CLI entry point for ConstructSync.

Usage:
    python -m constructsync.cli ingest --file data/raw/demo_products.csv
    python -m constructsync.cli ingest --file data.csv --batch-size 500 --concurrency 8
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys


def _setup_logging(debug: bool = False) -> None:
    """Configure logging for CLI usage."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet down noisy loggers
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def cmd_ingest(args: argparse.Namespace) -> None:
    """Execute the ingestion command."""
    from constructsync.engine.engine import IngestionEngine
    from constructsync.engine.sanitizer import SanitizerStage
    from constructsync.settings import get_settings

    settings = get_settings()

    sanitizer = SanitizerStage(
        text_fields=settings.sanitize_text_fields,
        id_fields=settings.sanitize_id_fields,
        numeric_fields=settings.sanitize_numeric_fields,
        url_fields=settings.sanitize_url_fields,
    )

    engine = IngestionEngine(
        file_path=args.file,
        settings=settings,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        base_url=args.base_url or settings.constructor_base_url,
        api_key=args.api_key or settings.constructor_api_key,
        pipeline_stages=[sanitizer],
    )

    stats = asyncio.run(engine.run())

    # Exit with error code if there were failures
    if stats.items_failed > 0:
        sys.exit(1)


def cmd_dlq_list(args: argparse.Namespace) -> None:
    """List items currently in the Dead-Letter Queue."""
    from constructsync.engine.dlq import DeadLetterQueue
    from constructsync.settings import get_settings
    from rich.console import Console
    from rich.table import Table

    settings = get_settings()
    dlq = DeadLetterQueue(settings.dlq_database_path)
    records = dlq.list_items(reason=args.reason, sku=args.sku, limit=args.limit)

    console = Console()
    if not records:
        console.print("[yellow]No items found in the Dead-Letter Queue.[/yellow]")
        return

    table = Table(title="💀 Dead-Letter Queue Items", border_style="red")
    table.add_column("DB ID", style="dim", justify="right")
    table.add_column("SKU/ID", style="bold")
    table.add_column("Reason", style="red")
    table.add_column("Timestamp", style="cyan")
    table.add_column("Retries", justify="center")

    for r in records:
        table.add_row(
            str(r["id"]),
            r["sku"],
            r["reason"] or "N/A",
            r["timestamp"],
            str(r["retry_count"]),
        )

    console.print(table)


def cmd_dlq_retry(args: argparse.Namespace) -> None:
    """Reprocess and retry failed items stored in the DLQ."""
    import asyncio
    from constructsync.engine.client import ConstructorClient
    from constructsync.engine.dlq import DeadLetterQueue
    from constructsync.settings import get_settings

    settings = get_settings()
    dlq = DeadLetterQueue(settings.dlq_database_path)
    records = dlq.list_items(limit=10000)

    if not records:
        print("No items found in the Dead-Letter Queue to retry.")
        return

    print(f"Found {len(records)} items in DLQ. Retrying...")

    async def run_retry():
        base_url = args.base_url or settings.constructor_base_url
        api_key = args.api_key or settings.constructor_api_key
        
        async with ConstructorClient(base_url=base_url, api_key=api_key) as client:
            # Batch items up to 1000
            for i in range(0, len(records), 1000):
                chunk = records[i : i + 1000]
                payloads = [r["sanitized_data"] for r in chunk]
                
                result = await client.send_batch(payloads)
                if result.success:
                    # Remove successfully processed records from DLQ
                    db_ids = [r["id"] for r in chunk]
                    dlq.delete_items(db_ids)
                    print(f"Successfully retried and cleared {len(db_ids)} items.")
                else:
                    print(f"Failed to retry batch of {len(chunk)} items: {result.error_message}")

    asyncio.run(run_retry())


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="constructsync",
        description="ConstructSync — Catalog ingestion pipeline for Constructor.io",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ── ingest ─────────────────────────────────────────────────────────
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingest a catalog file into the Constructor API",
    )
    ingest_parser.add_argument(
        "--file", "-f",
        required=True,
        help="Path to the catalog file (CSV or JSONL)",
    )
    ingest_parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=None,
        help="Items per API batch (default: from settings, typically 1000)",
    )
    ingest_parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=None,
        help="Number of concurrent workers (default: from settings, typically 4)",
    )
    ingest_parser.add_argument(
        "--base-url",
        type=str,
        default=None,
        help="Constructor API base URL (default: from .env)",
    )
    ingest_parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Constructor API key (default: from .env)",
    )
    ingest_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    # ── dlq-list ───────────────────────────────────────────────────────
    dlq_list_parser = subparsers.add_parser(
        "dlq-list",
        help="List failed items currently stored in the DLQ",
    )
    dlq_list_parser.add_argument(
        "--reason",
        type=str,
        default=None,
        help="Filter items by failure reason",
    )
    dlq_list_parser.add_argument(
        "--sku",
        type=str,
        default=None,
        help="Filter items by specific SKU/ID",
    )
    dlq_list_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of items to display (default: 100)",
    )
    dlq_list_parser.set_defaults(func=cmd_dlq_list)

    # ── dlq-retry ──────────────────────────────────────────────────────
    dlq_retry_parser = subparsers.add_parser(
        "dlq-retry",
        help="Reprocess and retry failed items in the DLQ",
    )
    dlq_retry_parser.add_argument(
        "--base-url",
        type=str,
        default=None,
        help="Constructor API base URL (default: from .env)",
    )
    dlq_retry_parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Constructor API key (default: from .env)",
    )
    dlq_retry_parser.set_defaults(func=cmd_dlq_retry)

    return parser


def main() -> None:
    """Main CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    _setup_logging(getattr(args, "debug", False))
    args.func(args)


if __name__ == "__main__":
    main()
