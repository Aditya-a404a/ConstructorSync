"""
Ingestion Sync Report Generator.

Aggregates statistics from the ingestion run, outputs a final console table
using Rich, and writes the summary to a JSON file.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

from constructsync.engine.models import IngestionStats


class SyncReportGenerator:
    """
    Generates, prints, and stores reports for catalog synchronization runs.
    """

    @staticmethod
    def generate_report_dict(
        stats: IngestionStats,
        sanitizer_stats: dict | None,
        peak_concurrency: int,
    ) -> dict:
        """Construct a structured report dictionary containing all sync metrics."""
        summary = {
            "total_items": stats.total_items,
            "items_sent": stats.items_sent,
            "items_failed": stats.items_failed,
            "batches_total": stats.batches_total,
            "batches_sent": stats.batches_sent,
            "batches_failed": stats.batches_failed,
            "time_elapsed_seconds": round(stats.elapsed_seconds, 2),
            "avg_throughput_items_sec": round(stats.items_per_second, 2),
            "api_calls": stats.api_calls,
            "retries": stats.retries,
            "peak_concurrency": peak_concurrency,
        }

        san_stats = {
            "items_sanitized": 0,
            "items_failed_validation": 0,
            "tags_stripped": 0,
            "entities_encoded": 0,
            "double_encoded_normalized": 0,
        }
        if sanitizer_stats:
            san_stats.update({
                "items_sanitized": sanitizer_stats.get("items_sanitized", 0),
                "items_failed_validation": sanitizer_stats.get("items_failed_validation", 0),
                "tags_stripped": sanitizer_stats.get("tags_stripped", 0),
                "entities_encoded": sanitizer_stats.get("entities_encoded", 0),
                "double_encoded_normalized": sanitizer_stats.get("double_encoded_normalized", 0),
            })

        # Placeholders for Issue #9 (Catalog Health Scoring Engine)
        health_stats = {
            "avg_score": 0.0,
            "min_score": 0.0,
            "max_score": 0.0,
            "items_below_threshold": 0,
        }

        # Handle numeric formatting for status codes
        status_codes = {str(code): count for code, count in stats.status_codes.items()}

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "run_summary": summary,
            "sanitization_stats": san_stats,
            "health_score_distribution": health_stats,
            "status_codes": status_codes,
        }

    @staticmethod
    def write_json_report(report_dict: dict, output_dir: str | Path = "reports") -> str:
        """Write the report dictionary to a JSON file named with a timestamp."""
        output_path = Path(output_dir)
        os.makedirs(output_path, exist_ok=True)
        
        # Format: sync_report_YYYYMMDD_HHMMSS.json
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_file = output_path / f"sync_report_{timestamp_str}.json"
        
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report_dict, f, indent=2)
            
        return str(report_file)

    @staticmethod
    def print_console_report(report_dict: dict, console: Console | None = None) -> None:
        """Print a professional summary report table to the console."""
        if console is None:
            console = Console()

        summary = report_dict["run_summary"]
        san_stats = report_dict["sanitization_stats"]
        status_codes = report_dict["status_codes"]
        
        elapsed = summary["time_elapsed_seconds"]
        minutes, seconds = divmod(int(elapsed), 60)

        console.print()
        console.rule("[bold green]Ingestion Complete[/bold green]")

        table = Table(show_header=False, border_style="green", padding=(0, 2))
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")

        table.add_row("Total Items", f"{summary['total_items']:,}")
        table.add_row("Items Sent", f"[green]{summary['items_sent']:,}[/green]")
        table.add_row("Items Failed", f"[red]{summary['items_failed']:,}[/red]")
        table.add_row("Time Elapsed", f"{minutes:02d}:{seconds:02d}")
        table.add_row("Avg Throughput", f"{summary['avg_throughput_items_sec']:,.0f} items/sec")
        table.add_row("API Calls", f"{summary['api_calls']:,}")
        table.add_row("Retries", f"{summary['retries']:,}")
        table.add_row("Peak Concurrency", f"{summary['peak_concurrency']:,}")

        # Show sanitization stats
        table.add_row("Items Sanitized", f"[yellow]{san_stats['items_sanitized']:,}[/yellow]")
        table.add_row("Validation Failures", f"[red]{san_stats['items_failed_validation']:,}[/red]")
        table.add_row("Tags Stripped", f"{san_stats['tags_stripped']:,}")
        table.add_row("Entities Encoded", f"{san_stats['entities_encoded']:,}")
        table.add_row("Double Entities Norm", f"{san_stats['double_encoded_normalized']:,}")

        if status_codes:
            codes_str = ", ".join(f"{code}: {count}" for code, count in sorted(status_codes.items()))
            table.add_row("Status Codes", codes_str)

        console.print(table)
        console.print()
