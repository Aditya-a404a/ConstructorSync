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
        health_scorer_stats: dict | None = None,
        health_scores: list[int] | None = None,
    ) -> dict:
        """Construct a structured report dictionary containing all sync metrics."""
        summary = {
            "total_items": stats.total_items,
            "items_sent": stats.items_sent,
            "items_skipped": stats.items_skipped,
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

        health_stats = {
            "avg_score": 0.0,
            "min_score": 0.0,
            "max_score": 0.0,
            "items_below_threshold": 0,
            "threshold": 70,
            "score_histogram": {},
        }
        if health_scorer_stats:
            health_stats.update({
                "avg_score": health_scorer_stats.get("avg_score", 0.0),
                "min_score": health_scorer_stats.get("min_score", 0.0),
                "max_score": health_scorer_stats.get("max_score", 0.0),
                "items_below_threshold": health_scorer_stats.get("items_below_threshold", 0),
                "threshold": health_scorer_stats.get("threshold", 70),
            })
        if health_scores:
            health_stats["score_histogram"] = SyncReportGenerator._calculate_histogram(health_scores)

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
    def _calculate_histogram(scores: list[int]) -> dict[str, int]:
        buckets = {
            "0-10": 0, "11-20": 0, "21-30": 0, "31-40": 0, "41-50": 0,
            "51-60": 0, "61-70": 0, "71-80": 0, "81-90": 0, "91-100": 0
        }
        for s in scores:
            if s <= 10:
                buckets["0-10"] += 1
            elif s <= 20:
                buckets["11-20"] += 1
            elif s <= 30:
                buckets["21-30"] += 1
            elif s <= 40:
                buckets["31-40"] += 1
            elif s <= 50:
                buckets["41-50"] += 1
            elif s <= 60:
                buckets["51-60"] += 1
            elif s <= 70:
                buckets["61-70"] += 1
            elif s <= 80:
                buckets["71-80"] += 1
            elif s <= 90:
                buckets["81-90"] += 1
            else:
                buckets["91-100"] += 1
        return buckets

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
        health = report_dict["health_score_distribution"]
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
        if summary.get("items_skipped", 0) > 0:
            table.add_row("Items Skipped", f"[cyan]{summary['items_skipped']:,}[/cyan]")
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

        # Show health scoring stats
        if health.get("avg_score", 0.0) > 0.0 or health.get("max_score", 0.0) > 0.0:
            table.add_row("Health Score Average", f"{health['avg_score']:.1f}")
            table.add_row("Health Score Min/Max", f"{health['min_score']} / {health['max_score']}")
            table.add_row(
                "Health Below Threshold",
                f"[bold red]{health['items_below_threshold']:,}[/bold red] (threshold={health['threshold']})"
            )

        if status_codes:
            codes_str = ", ".join(f"{code}: {count}" for code, count in sorted(status_codes.items()))
            table.add_row("Status Codes", codes_str)

        console.print(table)
        console.print()

        # Render ASCII Histogram
        hist = health.get("score_histogram", {})
        if hist and any(count > 0 for count in hist.values()):
            console.print("   [bold cyan]📊 Searchability Health Score Histogram:[/bold cyan]")
            max_count = max(hist.values())
            scale_width = 30
            
            # Sort buckets chronologically: 0-10 up to 91-100
            def bucket_key(b_str: str) -> int:
                try:
                    return int(b_str.split("-")[0])
                except Exception:
                    return 0

            for bucket, count in sorted(hist.items(), key=lambda x: bucket_key(x[0])):
                bar_len = int((count / max_count) * scale_width) if max_count > 0 else 0
                bar = "█" * bar_len
                # If there are items, show bar, else empty space
                bar_str = f"[cyan]{bar}[/cyan]" if bar_len > 0 else ""
                console.print(f"      {bucket:<7} : {count:>5} | {bar_str}")
            console.print()

        # Print overall status summary line
        skipped_count = summary.get("items_skipped", 0)
        sent_count = summary.get("items_sent", 0)
        console.print(f"   [bold dim]Skipped {skipped_count:,} unchanged items. Synced {sent_count:,} modified items.[/bold dim]")
        console.print()
