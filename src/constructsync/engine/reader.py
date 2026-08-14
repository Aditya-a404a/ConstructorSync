"""
Streaming catalog file reader.

Reads CSV or JSONL files in chunks using Polars, yielding batches of
item dicts without ever loading the full file into memory.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Generator

import polars as pl

logger = logging.getLogger(__name__)


class CatalogReader:
    """
    Memory-efficient reader for CSV and JSONL catalog files.

    Usage:
        reader = CatalogReader("data/raw/products.csv", batch_size=1000)
        for batch in reader.read_batches():
            # batch is a list[dict] of up to 1000 items
            process(batch)
    """

    SUPPORTED_EXTENSIONS = {".csv", ".jsonl", ".ndjson"}

    def __init__(self, file_path: str | Path, batch_size: int = 1000) -> None:
        self.file_path = Path(file_path)
        self.batch_size = batch_size

        if not self.file_path.exists():
            raise FileNotFoundError(f"Catalog file not found: {self.file_path}")

        self.format = self._detect_format()
        logger.info(
            "CatalogReader initialized: file=%s format=%s batch_size=%d",
            self.file_path,
            self.format,
            self.batch_size,
        )

    def _detect_format(self) -> str:
        """Detect file format from extension."""
        ext = self.file_path.suffix.lower()
        if ext == ".csv":
            return "csv"
        elif ext in (".jsonl", ".ndjson"):
            return "jsonl"
        else:
            raise ValueError(
                f"Unsupported file format: {ext}. "
                f"Supported: {', '.join(self.SUPPORTED_EXTENSIONS)}"
            )

    def count_rows(self) -> int:
        """
        Count total rows in the file (for progress tracking).

        Uses Polars lazy scanning for CSV to avoid loading the file.
        """
        if self.format == "csv":
            # scan_csv + count is memory-efficient
            try:
                return pl.scan_csv(
                    self.file_path, truncate_ragged_lines=True
                ).select(pl.len()).collect().item()
            except Exception:
                # Fallback: count lines minus header
                with open(self.file_path, "r") as f:
                    return sum(1 for _ in f) - 1
        else:
            # JSONL: count lines
            with open(self.file_path, "r") as f:
                return sum(1 for line in f if line.strip())

    def read_batches(self) -> Generator[list[dict], None, None]:
        """Yield batches of item dicts from the file."""
        if self.format == "csv":
            yield from self._read_csv_batches()
        else:
            yield from self._read_jsonl_batches()

    def _read_csv_batches(self) -> Generator[list[dict], None, None]:
        """Stream CSV in chunks using Polars batched reader."""
        reader = pl.read_csv_batched(
            self.file_path,
            batch_size=self.batch_size,
            truncate_ragged_lines=True,
        )

        buffer: list[dict] = []

        while True:
            frames = reader.next_batches(1)
            if not frames:
                break

            df = frames[0]
            buffer.extend(df.to_dicts())

            # Yield complete batches from the buffer
            while len(buffer) >= self.batch_size:
                yield buffer[: self.batch_size]
                buffer = buffer[self.batch_size :]

        # Yield any remaining items in the buffer
        if buffer:
            yield buffer

    def _read_jsonl_batches(self) -> Generator[list[dict], None, None]:
        """Stream JSONL line by line, accumulating into batches."""
        batch: list[dict] = []

        with open(self.file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    item = json.loads(line)
                    batch.append(item)
                except json.JSONDecodeError as e:
                    logger.warning(
                        "Skipping malformed JSONL line %d: %s", line_num, e
                    )
                    continue

                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        # Yield any remaining items
        if batch:
            yield batch
