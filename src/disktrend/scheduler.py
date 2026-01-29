"""
Scheduler module for DiskTrend.
Handles periodic disk scans using APScheduler.
"""

import asyncio
from datetime import datetime
from typing import Callable, Optional
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .models import Database
from .scanner import run_scan

logger = logging.getLogger(__name__)


class ScanScheduler:
    """Manages scheduled disk scans."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.config = config
        self.scheduler = AsyncIOScheduler()
        self._scan_task: Optional[asyncio.Task] = None
        self._status_callback: Optional[Callable] = None

    def set_status_callback(self, callback: Callable):
        """Set callback for status updates."""
        self._status_callback = callback

    def start(self):
        """Start the scheduler."""
        if not self.config.get('scheduler', {}).get('enabled', True):
            logger.info("Scheduler disabled in config")
            return

        scan_time = self.config.get('scheduler', {}).get('scan_time', '03:00')
        timezone = self.config.get('scheduler', {}).get('timezone', 'UTC')

        try:
            hour, minute = map(int, scan_time.split(':'))
        except ValueError:
            logger.error(f"Invalid scan_time format: {scan_time}")
            hour, minute = 3, 0

        # Schedule daily scan
        self.scheduler.add_job(
            self._run_scheduled_scan,
            CronTrigger(hour=hour, minute=minute, timezone=timezone),
            id='daily_scan',
            name='Daily Disk Scan',
            replace_existing=True
        )

        # Schedule cleanup job (run after scan)
        retention_days = self.config.get('retention', {}).get('days', 365)
        if retention_days > 0:
            self.scheduler.add_job(
                self._run_cleanup,
                CronTrigger(hour=hour, minute=minute + 30, timezone=timezone),
                id='cleanup',
                name='Old Snapshot Cleanup',
                replace_existing=True
            )

        self.scheduler.start()
        logger.info(f"Scheduler started. Daily scan at {scan_time} ({timezone})")

    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")

    async def _run_scheduled_scan(self):
        """Execute scheduled scan."""
        logger.info("Starting scheduled scan")
        await self._notify_status('scan_started', {
            'timestamp': datetime.now().isoformat(),
            'scheduled': True
        })

        try:
            await self.trigger_scan()
            await self._notify_status('scan_completed', {
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.exception(f"Scheduled scan failed: {e}")
            await self._notify_status('scan_failed', {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            })

    async def _run_cleanup(self):
        """Execute scheduled cleanup."""
        retention_days = self.config.get('retention', {}).get('days', 365)
        if retention_days > 0:
            logger.info(f"Running cleanup for snapshots older than {retention_days} days")
            await self.db.cleanup_old_snapshots(retention_days)

    async def trigger_scan(self, mount_points: list[str] = None) -> list[dict]:
        """
        Trigger a manual scan.
        Returns scan results.
        """
        if self._scan_task and not self._scan_task.done():
            raise RuntimeError("A scan is already in progress")

        # Get mount points from config if not provided
        if not mount_points:
            mount_points = self.config.get('scanner', {}).get('mount_points', ['/'])

        skip_paths = self.config.get('scanner', {}).get('skip_paths', [])
        max_depth = self.config.get('scanner', {}).get('max_depth', 0)

        # Run scan
        results = await run_scan(
            self.db,
            mount_points,
            skip_paths,
            max_depth,
            progress_callback=self._notify_progress
        )

        return results

    async def _notify_progress(self, data: dict):
        """Send progress notification."""
        await self._notify_status('scan_progress', data)

    async def _notify_status(self, event: str, data: dict):
        """Send status notification via callback."""
        if self._status_callback:
            if asyncio.iscoroutinefunction(self._status_callback):
                await self._status_callback(event, data)
            else:
                self._status_callback(event, data)

    def get_next_run(self) -> Optional[datetime]:
        """Get next scheduled scan time."""
        job = self.scheduler.get_job('daily_scan')
        if job:
            return job.next_run_time
        return None

    def is_scan_running(self) -> bool:
        """Check if a scan is currently running."""
        return self._scan_task is not None and not self._scan_task.done()
