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
    from constructsync.settings import get_settings

    settings = get_settings()

    engine = IngestionEngine(
        file_path=args.file,
        settings=settings,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        base_url=args.base_url or settings.constructor_base_url,
        api_key=args.api_key or settings.constructor_api_key,
    )

    stats = asyncio.run(engine.run())

    # Exit with error code if there were failures
    if stats.items_failed > 0:
        sys.exit(1)


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
