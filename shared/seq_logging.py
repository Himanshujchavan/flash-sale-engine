"""
Ships structured log events to Seq (http://localhost:5341 by default, see
docker-compose.yml) using Seq's raw CLEF ingestion endpoint
(POST /api/events/raw, newline-delimited JSON, one object per log line).

Design constraints that shaped this:
  - Every service here is asyncio-based. A synchronous HTTP POST on every
    single log call would add real latency to the request path (or the
    consumer path) for something that's purely a nice-to-have (centralized
    log viewing). So log events go onto an in-memory queue.Queue()
    (thread-safe, non-blocking `put_nowait`) and a SEPARATE background
    thread drains it and does the actual HTTP POSTs, in batches.
  - Seq being down (or not running at all, e.g. you skipped `docker compose
    up`) must NEVER crash or slow down the service. Every failure mode here
    is caught and silently dropped -- logs still go to the console
    regardless (see logging_config.py), Seq is purely additive.
"""
from __future__ import annotations

import json
import queue
import threading
from datetime import datetime, timezone

from shared.settings import get_settings

_seq_queue: "queue.Queue[dict]" = queue.Queue(maxsize=10_000)
_thread_started = False
_thread_lock = threading.Lock()

_SCALAR_TYPES = (str, int, float, bool, type(None))


def _to_clef(event_dict: dict) -> dict:
    """Convert a structlog event_dict into a Seq CLEF-shaped record.
    CLEF reserves keys starting with '@' for its own metadata (@t = timestamp,
    @m = rendered message, @l = level); everything else becomes a regular,
    filterable/searchable property in Seq's UI."""
    clef: dict = {
        "@t": datetime.now(timezone.utc).isoformat(),
        "@m": str(event_dict.get("event", "")),
        "@l": str(event_dict.get("level", "info")),
    }
    for key, value in event_dict.items():
        if key in ("event", "level", "timestamp"):
            continue
        clef[key] = value if isinstance(value, _SCALAR_TYPES) else str(value)
    return clef


def enqueue_for_seq(event_dict: dict) -> None:
    """Non-blocking -- drops the event on the floor if the queue is somehow
    full (Seq unreachable for a long time) rather than ever applying
    backpressure to the caller."""
    _ensure_worker_started()
    try:
        _seq_queue.put_nowait(_to_clef(event_dict))
    except queue.Full:
        pass


def _ensure_worker_started() -> None:
    global _thread_started
    if _thread_started:
        return
    with _thread_lock:
        if _thread_started:
            return
        thread = threading.Thread(target=_worker_loop, name="seq-log-forwarder", daemon=True)
        thread.start()
        _thread_started = True


def _worker_loop() -> None:
    import httpx  # local import: only needed by this background thread

    settings = get_settings()
    url = f"{settings.seq_url}/api/events/raw?clef"
    client = httpx.Client(timeout=2.0)

    while True:
        batch: list[dict] = []
        try:
            batch.append(_seq_queue.get(timeout=1.0))
        except queue.Empty:
            continue

        # Drain a bit more without blocking, so a burst of logs becomes one
        # HTTP request instead of one-request-per-line.
        while len(batch) < 100:
            try:
                batch.append(_seq_queue.get_nowait())
            except queue.Empty:
                break

        body = "\n".join(json.dumps(item, default=str) for item in batch)
        try:
            client.post(url, content=body, headers={"Content-Type": "application/vnd.serilog.clef"})
        except Exception:
            # Seq down/unreachable/network hiccup -- drop this batch and keep
            # going. Console logging (the source of truth for a human
            # watching a terminal) is completely unaffected by this.
            pass
