"""
Database models and data access layer for DiskTrend.
Uses SQLite with async support via aiosqlite.
"""

import sqlite3
import aiosqlite
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)


class Database:
    """SQLite database manager with connection pooling."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_sync()

    def _init_sync(self):
        """Initialize database schema synchronously (for startup)."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript("""
                -- Snapshots table: records each scan session
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mount_point TEXT NOT NULL,
                    started_at DATETIME NOT NULL,
                    completed_at DATETIME,
                    total_size INTEGER DEFAULT 0,
                    total_files INTEGER DEFAULT 0,
                    total_dirs INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'running'
                );

                -- Directory entries: stores size data for each path
                CREATE TABLE IF NOT EXISTS entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    name TEXT NOT NULL,
                    size INTEGER NOT NULL DEFAULT 0,
                    file_count INTEGER NOT NULL DEFAULT 0,
                    dir_count INTEGER NOT NULL DEFAULT 0,
                    depth INTEGER NOT NULL DEFAULT 0,
                    parent_path TEXT,
                    is_dir INTEGER NOT NULL DEFAULT 1,
                    error TEXT,
                    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
                );

                -- Scan errors log
                CREATE TABLE IF NOT EXISTS scan_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
                );

                -- Indexes for performance
                CREATE INDEX IF NOT EXISTS idx_entries_snapshot ON entries(snapshot_id);
                CREATE INDEX IF NOT EXISTS idx_entries_path ON entries(path);
                CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(parent_path);
                CREATE INDEX IF NOT EXISTS idx_entries_depth ON entries(snapshot_id, depth);
                CREATE INDEX IF NOT EXISTS idx_snapshots_mount ON snapshots(mount_point);
                CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(completed_at);
            """)
            conn.commit()
        finally:
            conn.close()

    @asynccontextmanager
    async def connection(self):
        """Get an async database connection."""
        conn = await aiosqlite.connect(self.db_path)
        conn.row_factory = aiosqlite.Row
        try:
            yield conn
        finally:
            await conn.close()

    async def create_snapshot(self, mount_point: str) -> int:
        """Create a new snapshot record and return its ID."""
        async with self.connection() as conn:
            cursor = await conn.execute(
                """INSERT INTO snapshots (mount_point, started_at, status)
                   VALUES (?, ?, 'running')""",
                (mount_point, datetime.now().isoformat())
            )
            await conn.commit()
            return cursor.lastrowid

    async def complete_snapshot(self, snapshot_id: int, total_size: int,
                                 total_files: int, total_dirs: int):
        """Mark a snapshot as completed."""
        async with self.connection() as conn:
            await conn.execute(
                """UPDATE snapshots
                   SET completed_at = ?, total_size = ?, total_files = ?,
                       total_dirs = ?, status = 'completed'
                   WHERE id = ?""",
                (datetime.now().isoformat(), total_size, total_files,
                 total_dirs, snapshot_id)
            )
            await conn.commit()

    async def fail_snapshot(self, snapshot_id: int, error: str):
        """Mark a snapshot as failed."""
        async with self.connection() as conn:
            await conn.execute(
                """UPDATE snapshots
                   SET completed_at = ?, status = 'failed'
                   WHERE id = ?""",
                (datetime.now().isoformat(), snapshot_id)
            )
            await conn.execute(
                """INSERT INTO scan_errors (snapshot_id, path, error_type, error_message)
                   VALUES (?, '/', 'FATAL', ?)""",
                (snapshot_id, error)
            )
            await conn.commit()

    async def insert_entries_batch(self, snapshot_id: int, entries: list[dict]):
        """Insert multiple directory entries in a single transaction."""
        if not entries:
            return
        async with self.connection() as conn:
            await conn.executemany(
                """INSERT INTO entries
                   (snapshot_id, path, name, size, file_count, dir_count,
                    depth, parent_path, is_dir, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(snapshot_id, e['path'], e['name'], e['size'],
                  e['file_count'], e['dir_count'], e['depth'],
                  e['parent_path'], e['is_dir'], e.get('error'))
                 for e in entries]
            )
            await conn.commit()

    async def log_error(self, snapshot_id: int, path: str,
                        error_type: str, message: str):
        """Log a scan error."""
        async with self.connection() as conn:
            await conn.execute(
                """INSERT INTO scan_errors (snapshot_id, path, error_type, error_message)
                   VALUES (?, ?, ?, ?)""",
                (snapshot_id, path, error_type, message)
            )
            await conn.commit()

    async def get_latest_snapshot(self, mount_point: str = None) -> Optional[dict]:
        """Get the most recent completed snapshot."""
        async with self.connection() as conn:
            if mount_point:
                cursor = await conn.execute(
                    """SELECT * FROM snapshots
                       WHERE mount_point = ? AND status = 'completed'
                       ORDER BY completed_at DESC LIMIT 1""",
                    (mount_point,)
                )
            else:
                cursor = await conn.execute(
                    """SELECT * FROM snapshots
                       WHERE status = 'completed'
                       ORDER BY completed_at DESC LIMIT 1"""
                )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_snapshots(self, mount_point: str = None,
                            days: int = None, limit: int = 100) -> list[dict]:
        """Get snapshot history."""
        async with self.connection() as conn:
            query = "SELECT * FROM snapshots WHERE status = 'completed'"
            params = []

            if mount_point:
                query += " AND mount_point = ?"
                params.append(mount_point)

            if days:
                cutoff = (datetime.now() - timedelta(days=days)).isoformat()
                query += " AND completed_at >= ?"
                params.append(cutoff)

            query += " ORDER BY completed_at DESC LIMIT ?"
            params.append(limit)

            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_entries(self, snapshot_id: int, parent_path: str = None,
                          depth: int = None) -> list[dict]:
        """Get directory entries for a snapshot."""
        async with self.connection() as conn:
            query = "SELECT * FROM entries WHERE snapshot_id = ?"
            params = [snapshot_id]

            if parent_path is not None:
                query += " AND parent_path = ?"
                params.append(parent_path)

            if depth is not None:
                query += " AND depth = ?"
                params.append(depth)

            query += " ORDER BY size DESC"
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_entry_by_path(self, snapshot_id: int, path: str) -> Optional[dict]:
        """Get a specific entry by path."""
        async with self.connection() as conn:
            cursor = await conn.execute(
                """SELECT * FROM entries
                   WHERE snapshot_id = ? AND path = ?""",
                (snapshot_id, path)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_path_history(self, path: str, days: int = 30) -> list[dict]:
        """Get size history for a specific path across snapshots."""
        async with self.connection() as conn:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            cursor = await conn.execute(
                """SELECT e.size, e.file_count, e.dir_count,
                          s.completed_at, s.id as snapshot_id
                   FROM entries e
                   JOIN snapshots s ON e.snapshot_id = s.id
                   WHERE e.path = ? AND s.status = 'completed'
                         AND s.completed_at >= ?
                   ORDER BY s.completed_at ASC""",
                (path, cutoff)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_top_growth(self, limit: int = 10) -> list[dict]:
        """Get directories with largest growth since last scan."""
        async with self.connection() as conn:
            cursor = await conn.execute(
                """WITH recent AS (
                       SELECT id FROM snapshots
                       WHERE status = 'completed'
                       ORDER BY completed_at DESC LIMIT 2
                   ),
                   latest AS (SELECT id FROM recent LIMIT 1),
                   previous AS (SELECT id FROM recent LIMIT 1 OFFSET 1)
                   SELECT
                       curr.path,
                       curr.name,
                       curr.size as current_size,
                       prev.size as previous_size,
                       (curr.size - COALESCE(prev.size, 0)) as growth,
                       CASE WHEN prev.size > 0
                            THEN ROUND((curr.size - prev.size) * 100.0 / prev.size, 2)
                            ELSE 100.0
                       END as growth_percent
                   FROM entries curr
                   LEFT JOIN entries prev ON curr.path = prev.path
                        AND prev.snapshot_id = (SELECT id FROM previous)
                   WHERE curr.snapshot_id = (SELECT id FROM latest)
                         AND curr.is_dir = 1
                         AND (curr.size - COALESCE(prev.size, 0)) > 0
                   ORDER BY growth DESC
                   LIMIT ?""",
                (limit,)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_running_snapshot(self) -> Optional[dict]:
        """Check if there's a scan currently running."""
        async with self.connection() as conn:
            cursor = await conn.execute(
                """SELECT * FROM snapshots WHERE status = 'running'
                   ORDER BY started_at DESC LIMIT 1"""
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def cleanup_old_snapshots(self, retention_days: int):
        """Remove snapshots older than retention period."""
        if retention_days <= 0:
            return
        async with self.connection() as conn:
            cutoff = (datetime.now() - timedelta(days=retention_days)).isoformat()
            await conn.execute(
                """DELETE FROM snapshots WHERE completed_at < ?""",
                (cutoff,)
            )
            await conn.commit()
            logger.info(f"Cleaned up snapshots older than {retention_days} days")

    async def get_scan_errors(self, snapshot_id: int, limit: int = 100) -> list[dict]:
        """Get errors for a specific snapshot."""
        async with self.connection() as conn:
            cursor = await conn.execute(
                """SELECT * FROM scan_errors
                   WHERE snapshot_id = ?
                   ORDER BY timestamp DESC LIMIT ?""",
                (snapshot_id, limit)
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
