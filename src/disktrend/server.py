"""
FastAPI web server for DiskTrend.
Provides REST API and serves the web dashboard.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

import yaml
import psutil
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .models import Database
from .scheduler import ScanScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
db: Optional[Database] = None
scheduler: Optional[ScanScheduler] = None
config: dict = {}
websocket_clients: set[WebSocket] = set()


def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file or environment."""
    # Default config path
    if not config_path:
        config_path = os.environ.get('DISKTREND_CONFIG', 'config.yaml')

    cfg = {
        'server': {'host': '0.0.0.0', 'port': 8080},
        'database': {'path': './data/disktrend.db'},
        'scanner': {
            'mount_points': ['/'],
            'skip_paths': ['/proc', '/sys', '/dev', '/run', '/snap', '/tmp'],
            'max_depth': 0
        },
        'scheduler': {
            'enabled': True,
            'scan_time': '03:00',
            'timezone': 'UTC'
        },
        'retention': {'days': 365}
    }

    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            file_config = yaml.safe_load(f) or {}
            # Deep merge
            for key, value in file_config.items():
                if isinstance(value, dict) and key in cfg:
                    cfg[key].update(value)
                else:
                    cfg[key] = value
        logger.info(f"Loaded config from {config_path}")
    else:
        logger.warning(f"Config file not found: {config_path}, using defaults")

    return cfg


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global db, scheduler, config

    # Load config
    config = load_config()

    # Initialize database
    db_path = config['database']['path']
    db = Database(db_path)
    logger.info(f"Database initialized at {db_path}")

    # Initialize scheduler
    scheduler = ScanScheduler(db, config)
    scheduler.set_status_callback(broadcast_status)
    scheduler.start()

    yield

    # Cleanup
    if scheduler:
        scheduler.stop()


# Create FastAPI app
app = FastAPI(
    title="DiskTrend Web",
    description="Linux Storage Analyzer with Web Dashboard",
    version="1.0.0",
    lifespan=lifespan
)


# Pydantic models
class ScanRequest(BaseModel):
    mount_points: Optional[list[str]] = None


class PathQuery(BaseModel):
    path: str
    days: int = 30


# WebSocket broadcast
async def broadcast_status(event: str, data: dict):
    """Broadcast status to all connected WebSocket clients."""
    message = {'event': event, 'data': data, 'timestamp': datetime.now().isoformat()}
    disconnected = set()
    for ws in websocket_clients:
        try:
            await ws.send_json(message)
        except Exception:
            disconnected.add(ws)
    websocket_clients.difference_update(disconnected)


# API Routes
@app.get("/api/status")
async def get_status():
    """Get system and scan status."""
    # Get disk usage info
    disk_info = []
    for mount in config.get('scanner', {}).get('mount_points', ['/']):
        try:
            usage = psutil.disk_usage(mount)
            disk_info.append({
                'mount_point': mount,
                'total': usage.total,
                'used': usage.used,
                'free': usage.free,
                'percent': usage.percent
            })
        except Exception as e:
            logger.warning(f"Could not get disk usage for {mount}: {e}")

    # Get latest snapshot info
    latest = await db.get_latest_snapshot()
    running = await db.get_running_snapshot()

    return {
        'disk_info': disk_info,
        'latest_snapshot': latest,
        'running_scan': running,
        'next_scan': scheduler.get_next_run().isoformat() if scheduler.get_next_run() else None,
        'scheduler_enabled': config.get('scheduler', {}).get('enabled', True)
    }


@app.get("/api/snapshots")
async def get_snapshots(
    mount_point: str = None,
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=100, ge=1, le=1000)
):
    """Get snapshot history."""
    snapshots = await db.get_snapshots(mount_point, days, limit)
    return {'snapshots': snapshots}


@app.get("/api/snapshot/{snapshot_id}")
async def get_snapshot(snapshot_id: int):
    """Get details for a specific snapshot."""
    async with db.connection() as conn:
        cursor = await conn.execute(
            "SELECT * FROM snapshots WHERE id = ?", (snapshot_id,)
        )
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(404, "Snapshot not found")
        return dict(row)


@app.get("/api/snapshot/{snapshot_id}/entries")
async def get_snapshot_entries(
    snapshot_id: int,
    parent_path: str = Query(default=None),
    depth: int = Query(default=None, ge=0)
):
    """Get directory entries for a snapshot."""
    entries = await db.get_entries(snapshot_id, parent_path, depth)
    return {'entries': entries}


@app.get("/api/snapshot/{snapshot_id}/errors")
async def get_snapshot_errors(
    snapshot_id: int,
    limit: int = Query(default=100, ge=1, le=1000)
):
    """Get scan errors for a snapshot."""
    errors = await db.get_scan_errors(snapshot_id, limit)
    return {'errors': errors}


@app.get("/api/browse")
async def browse_directory(
    path: str = Query(default="/"),
    snapshot_id: int = Query(default=None)
):
    """
    Browse directory contents.
    If snapshot_id not provided, uses latest snapshot.
    """
    if snapshot_id is None:
        latest = await db.get_latest_snapshot()
        if not latest:
            raise HTTPException(404, "No snapshots available")
        snapshot_id = latest['id']

    # Get the entry for the requested path
    entry = await db.get_entry_by_path(snapshot_id, path)
    if not entry:
        raise HTTPException(404, f"Path not found: {path}")

    # Get children
    children = await db.get_entries(snapshot_id, parent_path=path)

    return {
        'current': entry,
        'children': children,
        'snapshot_id': snapshot_id
    }


@app.get("/api/history")
async def get_path_history(
    path: str = Query(...),
    days: int = Query(default=30, ge=1, le=365)
):
    """Get size history for a specific path."""
    history = await db.get_path_history(path, days)
    if not history:
        raise HTTPException(404, f"No history found for path: {path}")
    return {'path': path, 'history': history}


@app.get("/api/growth")
async def get_top_growth(
    limit: int = Query(default=10, ge=1, le=100)
):
    """Get directories with largest growth since last scan."""
    growth = await db.get_top_growth(limit)
    return {'growth': growth}


@app.get("/api/overview")
async def get_overview():
    """Get dashboard overview data."""
    # Get latest snapshot
    latest = await db.get_latest_snapshot()
    if not latest:
        return {
            'has_data': False,
            'message': 'No scans completed yet. Run a scan to see data.'
        }

    # Get top-level directories
    top_entries = await db.get_entries(latest['id'], depth=1)

    # Get growth data
    growth = await db.get_top_growth(10)

    # Get disk usage
    disk_info = []
    for mount in config.get('scanner', {}).get('mount_points', ['/']):
        try:
            usage = psutil.disk_usage(mount)
            disk_info.append({
                'mount_point': mount,
                'total': usage.total,
                'used': usage.used,
                'free': usage.free,
                'percent': usage.percent
            })
        except Exception:
            pass

    return {
        'has_data': True,
        'snapshot': latest,
        'top_directories': top_entries[:20],  # Top 20 by size
        'top_growth': growth,
        'disk_usage': disk_info
    }


@app.post("/api/scan")
async def trigger_scan(request: ScanRequest = None):
    """Trigger a manual scan."""
    if scheduler.is_scan_running():
        raise HTTPException(409, "A scan is already in progress")

    mount_points = request.mount_points if request else None

    # Run scan in background
    asyncio.create_task(_run_scan_task(mount_points))

    return {'status': 'started', 'message': 'Scan started in background'}


async def _run_scan_task(mount_points: list[str] = None):
    """Background scan task."""
    try:
        await broadcast_status('scan_started', {
            'timestamp': datetime.now().isoformat()
        })
        results = await scheduler.trigger_scan(mount_points)
        await broadcast_status('scan_completed', {
            'timestamp': datetime.now().isoformat(),
            'results': results
        })
    except Exception as e:
        logger.exception(f"Scan failed: {e}")
        await broadcast_status('scan_failed', {
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        })


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await websocket.accept()
    websocket_clients.add(websocket)
    try:
        while True:
            # Keep connection alive
            data = await websocket.receive_text()
            # Echo ping/pong
            if data == 'ping':
                await websocket.send_text('pong')
    except WebSocketDisconnect:
        websocket_clients.discard(websocket)


# Serve frontend
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the main dashboard page."""
    template_path = Path(__file__).parent.parent.parent / "templates" / "index.html"
    if template_path.exists():
        return template_path.read_text()

    # Fallback: check relative to working directory
    alt_path = Path("templates/index.html")
    if alt_path.exists():
        return alt_path.read_text()

    return HTMLResponse(
        "<h1>DiskTrend Web</h1><p>Template not found. Please ensure templates/index.html exists.</p>",
        status_code=500
    )


def main():
    """Main entry point."""
    import uvicorn

    # Load config for server settings
    cfg = load_config()
    host = cfg['server']['host']
    port = cfg['server']['port']

    # Check for sudo requirement
    if os.geteuid() != 0:
        print("Warning: Not running as root. Some directories may not be accessible.")
        print("For full system scan, run with: sudo -E uv run disktrend")

    print(f"Starting DiskTrend Web on http://{host}:{port}")
    uvicorn.run(
        "disktrend.server:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
