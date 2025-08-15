from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple

from fastapi import FastAPI, APIRouter, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse

# ---------- Data models ----------

@dataclass
class LogEvent:
    """
    Structured log event transported to WebSocket clients and REST endpoints.
    """
    type: str
    level: str
    logger: str
    message: str
    timestamp: str
    pathname: str
    lineno: int
    func_name: str
    process: int
    thread: int
    extra: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_record(record: logging.LogRecord) -> "LogEvent":
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        # Capture extra keys safely
        reserved = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys())
        record_dict = record.__dict__
        extra = {k: v for k, v in record_dict.items() if k not in reserved}
        return LogEvent(
            type="backend_log",
            level=record.levelname,
            logger=record.name,
            message=str(record.getMessage()),
            timestamp=ts,
            pathname=record.pathname or "",
            lineno=int(record.lineno or 0),
            func_name=record.funcName or "",
            process=int(record.process or 0),
            thread=int(record.thread or 0),
            extra=extra or {},
        )

# ---------- In-memory ring buffer for REST fallback ----------

class InMemoryLogBuffer:
    """
    Fixed-size ring buffer that stores the most recent log events.
    Thread-safe for append via the logging handler (uses loop.call_soon_threadsafe).
    """

    def __init__(self, maxlen: int = 1000) -> None:
        self._buf: Deque[LogEvent] = deque(maxlen=maxlen)
        self._lock = asyncio.Lock()

    async def append(self, event: LogEvent) -> None:
        async with self._lock:
            self._buf.append(event)

    async def recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with self._lock:
            return [asdict(e) for e in list(self._buf)[-limit:]]

# ---------- Broadcaster ----------

class WebSocketLogBroadcaster:
    """
    Manages per-client queues and pushes new LogEvent items to all connected clients.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, buffer: InMemoryLogBuffer) -> None:
        self._loop = loop
        self._clients: Set[asyncio.Queue] = set()
        self._buffer = buffer
        self._lock = asyncio.Lock()

    async def register(self) -> asyncio.Queue:
        """
        Register a new client; returns its queue.
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        async with self._lock:
            self._clients.add(q)
        return q

    async def unregister(self, q: asyncio.Queue) -> None:
        async with self._lock:
            self._clients.discard(q)

    def publish_from_handler(self, event: LogEvent) -> None:
        """
        This is called from logging.Handler.emit, often from non-async threads.
        Fan-out must be scheduled thread-safely onto the event loop.
        """
        self._loop.call_soon_threadsafe(self._publish_safe, event)

    def _publish_safe(self, event: LogEvent) -> None:
        # Don't await in this thread-safe path; schedule tasks on the loop
        asyncio.create_task(self._fan_out(event))

    async def _fan_out(self, event: LogEvent) -> None:
        await self._buffer.append(event)
        async with self._lock:
            dead: List[asyncio.Queue] = []
            for q in self._clients:
                try:
                    if q.full():
                        # Drop oldest to make space (non-blocking)
                        _ = q.get_nowait()
                    q.put_nowait(asdict(event))
                except Exception:
                    dead.append(q)
            for q in dead:
                self._clients.discard(q)

# ---------- Logging handler ----------

class WebSocketBroadcastHandler(logging.Handler):
    """
    Logging handler that forwards records to WebSocketLogBroadcaster.
    """

    def __init__(self, broadcaster: WebSocketLogBroadcaster, level: int = logging.INFO) -> None:
        super().__init__(level=level)
        self._broadcaster = broadcaster

    def emit(self, record: logging.LogRecord) -> None:
        try:
            event = LogEvent.from_record(record)
            self._broadcaster.publish_from_handler(event)
        except Exception:
            # Never throw from emit
            self.handleError(record)

# ---------- FastAPI router ----------

router = APIRouter()

def _compile_filters(
    level: Optional[str], contains: Optional[str], regex: Optional[str]
) -> Tuple[int, Optional[str], Optional[re.Pattern]]:
    level_map = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    min_level = level_map.get((level or "INFO").upper(), logging.INFO)
    contains_str = contains or None
    rx = re.compile(regex) if regex else None
    return min_level, contains_str, rx

@router.websocket("/ws/logs")
async def ws_logs(
    websocket: WebSocket,
    level: Optional[str] = None,
    contains: Optional[str] = None,
    regex: Optional[str] = None,
) -> None:
    """
    WebSocket endpoint that streams JSON log events in real time.

    Query params:
    - level:     Min level (DEBUG|INFO|WARNING|ERROR|CRITICAL)
    - contains:  Simple substring match on message
    - regex:     Regex match on message (applied if provided)
    """
    await websocket.accept()
    app: FastAPI = websocket.app
    broadcaster: WebSocketLogBroadcaster = app.state.log_broadcaster  # type: ignore[attr-defined]

    q = await broadcaster.register()
    min_level, contains_str, rx = _compile_filters(level, contains, regex)

    # Initial hello
    await websocket.send_text(
        json.dumps(
            {
                "type": "backend_log",
                "level": "INFO",
                "logger": "ws.logs",
                "message": "Connected to backend log stream",
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                "pathname": __file__,
                "lineno": 0,
                "func_name": "ws_logs",
                "process": 0,
                "thread": 0,
                "extra": {"filters": {"level": level, "contains": contains, "regex": regex}},
            }
        )
    )
    try:
        while True:
            item = await q.get()
            try:
                # Per-connection filtering
                lvl_ok = logging.getLevelName(item["level"]) >= logging.getLevelName(min_level)
            except Exception:
                lvl_ok = True

            msg = item.get("message", "")
            if contains_str and contains_str not in msg:
                continue
            if rx and not rx.search(msg):
                continue

            await websocket.send_text(json.dumps(item))
    except WebSocketDisconnect:
        pass
    finally:
        await broadcaster.unregister(q)

@router.get("/api/v1/logs/recent")
async def recent_logs(limit: int = Query(50, ge=1, le=500)) -> JSONResponse:
    """
    Return recent log events from the in-memory buffer (REST fallback).
    """
    app: FastAPI
    # The router will be included into an app; FastAPI injects request.app in dependencies.
    # Here we fetch via router dependency-free approach using starlette context:
    from starlette_context import context as _ctx  # optional; handle without if not present
    app = _ctx.get("app") or router  # fall back; replaced in include_router
    try:
        buffer: InMemoryLogBuffer = app.state.log_buffer  # type: ignore[attr-defined]
        items = await buffer.recent(limit=limit)
        return JSONResponse({"logs": items})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

# ---------- Installer ----------

def install_ws_logging(app: FastAPI, level: int = logging.INFO) -> None:
    """
    Attach a WebSocket/REST-capable logging pipeline to a FastAPI app.

    :param app: FastAPI application.
    :param level: Minimum level to capture from root.
    :return: None.
    """
    loop = asyncio.get_event_loop()
    buffer = InMemoryLogBuffer(maxlen=2000)
    broadcaster = WebSocketLogBroadcaster(loop=loop, buffer=buffer)

    # Expose on app.state
    app.state.log_broadcaster = broadcaster  # type: ignore[attr-defined]
    app.state.log_buffer = buffer  # type: ignore[attr-defined]

    # Add our handler to root and key loggers
    handler = WebSocketBroadcastHandler(broadcaster=broadcaster, level=logging.DEBUG)

    root = logging.getLogger()
    root.setLevel(min(root.level, logging.DEBUG))
    root.addHandler(handler)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        lg = logging.getLogger(name)
        lg.setLevel(min(lg.level, logging.DEBUG))
        lg.addHandler(handler)

    # Include our router
    app.include_router(router)

