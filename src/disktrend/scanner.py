"""
Disk scanner module for DiskTrend.
Handles recursive directory traversal and size calculation.
"""

import os
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Callable, Optional
import logging

from .models import Database

logger = logging.getLogger(__name__)


class DiskScanner:
    """High-performance disk scanner with error handling."""

    BATCH_SIZE = 1000  # Number of entries to batch before DB insert

    def __init__(self, db: Database, skip_paths: list[str] = None,
                 max_depth: int = 0):
        self.db = db
        self.skip_paths = set(skip_paths or [])
        self.max_depth = max_depth

        # Default virtual filesystems to skip
        self.default_skip = {
            '/proc', '/sys', '/dev', '/run', '/snap',
            '/var/snap', '/tmp', '/var/tmp'
        }
        self.skip_paths.update(self.default_skip)

        # Scan state
        self._running = False
        self._cancelled = False
        self._progress_callback: Optional[Callable] = None

    def set_progress_callback(self, callback: Callable):
        """Set callback for progress updates."""
        self._progress_callback = callback

    def cancel(self):
        """Cancel the running scan."""
        self._cancelled = True

    def is_running(self) -> bool:
        """Check if scan is currently running."""
        return self._running

    def _should_skip(self, path: str) -> bool:
        """Check if path should be skipped."""
        # Exact match
        if path in self.skip_paths:
            return True
        # Check if path is under a skip directory
        for skip in self.skip_paths:
            if path.startswith(skip + '/'):
                return True
        return False

    def _get_depth(self, path: str, base_path: str) -> int:
        """Calculate depth relative to base path."""
        if path == base_path:
            return 0
        rel_path = os.path.relpath(path, base_path)
        return len(rel_path.split(os.sep))

    async def scan(self, mount_point: str) -> dict:
        """
        Perform a full scan of the mount point.
        Returns scan statistics.
        """
        if self._running:
            raise RuntimeError("Scan already in progress")

        self._running = True
        self._cancelled = False
        snapshot_id = None

        stats = {
            'total_size': 0,
            'total_files': 0,
            'total_dirs': 0,
            'errors': 0,
            'started_at': datetime.now(),
            'completed_at': None
        }

        try:
            # Normalize mount point
            mount_point = os.path.abspath(mount_point)
            if not os.path.isdir(mount_point):
                raise ValueError(f"Mount point does not exist: {mount_point}")

            logger.info(f"Starting scan of {mount_point}")

            # Create snapshot record
            snapshot_id = await self.db.create_snapshot(mount_point)
            logger.info(f"Created snapshot {snapshot_id}")

            # Dictionary to accumulate directory sizes
            dir_sizes: dict[str, dict] = {}
            entries_batch: list[dict] = []

            # Walk the directory tree
            for root, dirs, files in os.walk(mount_point, topdown=False,
                                             onerror=self._walk_error_handler):
                if self._cancelled:
                    logger.warning("Scan cancelled by user")
                    break

                if self._should_skip(root):
                    continue

                depth = self._get_depth(root, mount_point)
                if self.max_depth > 0 and depth > self.max_depth:
                    continue

                # Initialize directory entry
                dir_entry = {
                    'path': root,
                    'name': os.path.basename(root) or root,
                    'size': 0,
                    'file_count': 0,
                    'dir_count': 0,
                    'depth': depth,
                    'parent_path': os.path.dirname(root) if root != mount_point else None,
                    'is_dir': 1
                }

                # Process files in this directory
                for filename in files:
                    filepath = os.path.join(root, filename)
                    try:
                        # Use lstat to not follow symlinks
                        stat_info = os.lstat(filepath)
                        file_size = stat_info.st_size
                        dir_entry['size'] += file_size
                        dir_entry['file_count'] += 1
                        stats['total_files'] += 1
                        stats['total_size'] += file_size
                    except (OSError, PermissionError) as e:
                        stats['errors'] += 1
                        await self.db.log_error(
                            snapshot_id, filepath,
                            type(e).__name__, str(e)
                        )

                # Add sizes from subdirectories (bottom-up traversal)
                for dirname in dirs:
                    dirpath = os.path.join(root, dirname)
                    if dirpath in dir_sizes:
                        subdir = dir_sizes[dirpath]
                        dir_entry['size'] += subdir['size']
                        dir_entry['file_count'] += subdir['file_count']
                        dir_entry['dir_count'] += subdir['dir_count'] + 1

                # Store for parent calculation
                dir_sizes[root] = dir_entry
                stats['total_dirs'] += 1

                # Add to batch
                entries_batch.append(dir_entry)

                # Flush batch if needed
                if len(entries_batch) >= self.BATCH_SIZE:
                    await self.db.insert_entries_batch(snapshot_id, entries_batch)
                    entries_batch.clear()

                    # Progress callback
                    if self._progress_callback:
                        await self._maybe_call_progress({
                            'current_path': root,
                            'dirs_scanned': stats['total_dirs'],
                            'files_scanned': stats['total_files'],
                            'size_scanned': stats['total_size']
                        })

            # Flush remaining entries
            if entries_batch:
                await self.db.insert_entries_batch(snapshot_id, entries_batch)

            stats['completed_at'] = datetime.now()

            # Mark snapshot as completed
            if not self._cancelled:
                await self.db.complete_snapshot(
                    snapshot_id,
                    stats['total_size'],
                    stats['total_files'],
                    stats['total_dirs']
                )
                logger.info(
                    f"Scan completed: {stats['total_dirs']} dirs, "
                    f"{stats['total_files']} files, "
                    f"{self._format_size(stats['total_size'])}"
                )
            else:
                await self.db.fail_snapshot(snapshot_id, "Cancelled by user")

        except Exception as e:
            logger.exception(f"Scan failed: {e}")
            if snapshot_id:
                await self.db.fail_snapshot(snapshot_id, str(e))
            raise
        finally:
            self._running = False

        return stats

    def _walk_error_handler(self, error: OSError):
        """Handle errors during os.walk."""
        logger.warning(f"Walk error: {error}")

    async def _maybe_call_progress(self, data: dict):
        """Call progress callback if set."""
        if self._progress_callback:
            if asyncio.iscoroutinefunction(self._progress_callback):
                await self._progress_callback(data)
            else:
                self._progress_callback(data)

    @staticmethod
    def _format_size(size: int) -> str:
        """Format size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if abs(size) < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"


async def run_scan(db: Database, mount_points: list[str],
                   skip_paths: list[str] = None,
                   max_depth: int = 0,
                   progress_callback: Callable = None) -> list[dict]:
    """
    Run scans for multiple mount points.
    Returns list of scan statistics.
    """
    scanner = DiskScanner(db, skip_paths, max_depth)
    if progress_callback:
        scanner.set_progress_callback(progress_callback)

    results = []
    for mount_point in mount_points:
        try:
            stats = await scanner.scan(mount_point)
            results.append({
                'mount_point': mount_point,
                'status': 'success',
                'stats': stats
            })
        except Exception as e:
            logger.error(f"Failed to scan {mount_point}: {e}")
            results.append({
                'mount_point': mount_point,
                'status': 'failed',
                'error': str(e)
            })

    return results
