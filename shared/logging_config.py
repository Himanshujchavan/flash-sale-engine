"""
Structured logging shared by all services. Logs to console as JSON (readable
in each terminal window) and ALSO forwards every event to Seq for
centralized viewing/searching across all 5 services at once (see
shared/seq_logging.py for how that forwarding works and why it's
fire-and-forget). Every log line includes `service` so you can tell which
service emitted it either in a terminal window or filtering in Seq's UI.
"""
from __future__ import annotations

import logging
import sys

import structlog

from shared.seq_logging import enqueue_for_seq


def _forward_to_seq(logger, method_name, event_dict):
    """A structlog processor that ships a copy of the event to Seq as a
    side effect, then passes the event_dict through UNCHANGED so the
    remaining processors (console rendering) behave exactly as before.
    Must run before ConsoleRenderer, which replaces event_dict with a
    plain string."""
    enqueue_for_seq(event_dict)
    return event_dict


def configure_logging(service_name: str) -> structlog.stdlib.BoundLogger:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            _forward_to_seq,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logger = structlog.get_logger().bind(service=service_name)
    return logger
