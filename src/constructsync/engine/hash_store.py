"""
SQLite-backed Hash Store to persist catalog item content hashes.
Allows checking if items have changed since they were last successfully synced.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path


class HashStore:
    """
    Hash Store backed by SQLite.
    Maps product SKU to its last successfully synchronized content hash.
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        os.makedirs(self.db_path.parent, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create the hashes table if it does not exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS hashes (
                    sku TEXT PRIMARY KEY,
                    hash TEXT NOT NULL,
                    last_synced_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def get_hash(self, sku: str) -> str | None:
        """Retrieve the last synced hash for a product SKU."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT hash FROM hashes WHERE sku = ?", (sku,))
            row = cursor.fetchone()
            return row["hash"] if row else None

    def update_hash(self, sku: str, hash_val: str) -> None:
        """Insert or update a product SKU hash."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO hashes (sku, hash, last_synced_at)
                VALUES (?, ?, ?)
                ON CONFLICT(sku) DO UPDATE SET
                    hash = excluded.hash,
                    last_synced_at = excluded.last_synced_at
                """,
                (sku, hash_val, now),
            )
            conn.commit()

    def update_hashes(self, sku_hash_pairs: list[tuple[str, str]]) -> None:
        """Bulk insert or update product SKU hashes for speed."""
        if not sku_hash_pairs:
            return
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO hashes (sku, hash, last_synced_at)
                VALUES (?, ?, ?)
                ON CONFLICT(sku) DO UPDATE SET
                    hash = excluded.hash,
                    last_synced_at = excluded.last_synced_at
                """,
                [(sku, hash_val, now) for sku, hash_val in sku_hash_pairs],
            )
            conn.commit()

    def clear(self) -> None:
        """Clear all records from the hash store."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM hashes")
            conn.commit()
