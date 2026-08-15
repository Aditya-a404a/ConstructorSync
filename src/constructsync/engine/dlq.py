"""
Dead-Letter Queue (DLQ) backed by SQLite.

Stores failed ingestion items with full error context, reason, and original payloads.
Allows operator querying and retrying of failures.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


class DeadLetterQueue:
    """
    SQLite-backed Dead-Letter Queue.
    
    Usage:
        dlq = DeadLetterQueue("dlq/dlq.db")
        dlq.insert_failed_items(items, reason="429 Too Many Requests")
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        # Ensure containing directory exists
        os.makedirs(self.db_path.parent, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create the dlq table if it doesn't already exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dlq (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku TEXT NOT NULL,
                    reason TEXT,
                    timestamp TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    original_data TEXT NOT NULL,
                    sanitized_data TEXT
                )
                """
            )
            # Add an index on SKU and timestamp for fast lookups
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dlq_sku ON dlq(sku)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dlq_timestamp ON dlq(timestamp)")
            conn.commit()

    def insert_failed_items(
        self,
        items: list[dict[str, Any]],
        reason: str,
        retry_count: int = 3,
    ) -> None:
        """Insert a batch of failed items into the DLQ."""
        if not items:
            return

        timestamp = datetime.utcnow().isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            for item in items:
                sku = str(item.get("id") or item.get("sku") or "UNKNOWN")
                
                # Treat raw item data as original unless explicitly structured
                original_data = json.dumps(item)
                sanitized_data = json.dumps(item)
                
                cursor.execute(
                    """
                    INSERT INTO dlq (sku, reason, timestamp, retry_count, original_data, sanitized_data)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (sku, reason, timestamp, retry_count, original_data, sanitized_data)
                )
            conn.commit()

    def list_failed_items(self) -> list[dict[str, Any]]:
        """Retrieve all failed items in the DLQ (backwards compatibility helper)."""
        return self.list_items(limit=100000)

    def list_items(
        self,
        reason: str | None = None,
        sku: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        List failed items inside the DLQ with optional filters.
        """
        query = "SELECT id, sku, reason, timestamp, retry_count, original_data, sanitized_data FROM dlq"
        params = []
        conditions = []

        if reason:
            conditions.append("reason LIKE ?")
            params.append(f"%{reason}%")
        if sku:
            conditions.append("sku = ?")
            params.append(sku)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            results = []
            for r in rows:
                results.append({
                    "id": r["id"],
                    "sku": r["sku"],
                    "reason": r["reason"],
                    "timestamp": r["timestamp"],
                    "retry_count": r["retry_count"],
                    "original_data": json.loads(r["original_data"]),
                    "sanitized_data": json.loads(r["sanitized_data"]) if r["sanitized_data"] else None,
                })
            return results

    def get_item(self, item_id: int) -> dict[str, Any] | None:
        """Retrieve a single DLQ record by database ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dlq WHERE id = ?", (item_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row["id"],
                "sku": row["sku"],
                "reason": row["reason"],
                "timestamp": row["timestamp"],
                "retry_count": row["retry_count"],
                "original_data": json.loads(row["original_data"]),
                "sanitized_data": json.loads(row["sanitized_data"]) if row["sanitized_data"] else None,
            }

    def delete_items(self, item_ids: list[int]) -> None:
        """Delete DLQ records by their database IDs."""
        if not item_ids:
            return
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Split deletes if lists are exceptionally large
            placeholders = ",".join("?" for _ in item_ids)
            cursor.execute(f"DELETE FROM dlq WHERE id IN ({placeholders})", item_ids)
            conn.commit()

    def clear(self) -> None:
        """Clear all records from the DLQ database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM dlq")
            conn.commit()
