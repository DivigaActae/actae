# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Structured logging with correlation IDs, context propagation, and
enterprise-format output (JSON for production, human-readable for dev).

Pattern: Each log record carries:
  - correlation_id:  UUID tracing a single request/run
  - component:       Which module emitted the log
  - environment:     dev / staging / production
  - extra context:   Arbitrary key=value pairs via the `extra` dict
"""

import os
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional


#  Log record structure 
# In production, logs are JSON for ingestion into Elasticsearch / Loki / etc.
# In development, we use a compact human-readable format.

_JSON_FORMAT = os.environ.get("LOG_FORMAT", "console").lower() == "json"
_CORRELATION_ID: Optional[str] = None


def get_correlation_id() -> str:
    """Return the current run's correlation ID, creating one if needed."""
    global _CORRELATION_ID
    if _CORRELATION_ID is None:
        _CORRELATION_ID = uuid.uuid4().hex[:12]
    return _CORRELATION_ID


def reset_correlation_id() -> str:
    """Force a new correlation ID (called at pipeline start)."""
    global _CORRELATION_ID
    _CORRELATION_ID = uuid.uuid4().hex[:12]
    return _CORRELATION_ID


#  Custom formatter 
class StructuredFormatter(logging.Formatter):
    """Emits JSON-logfmt hybrid records."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        correlation_id = getattr(record, "correlation_id", None) or get_correlation_id()
        component = getattr(record, "component", record.name)

        if _JSON_FORMAT:
            payload = {
                "timestamp": ts,
                "level": record.levelname,
                "correlation_id": correlation_id,
                "component": component,
                "message": record.getMessage(),
            }
            if record.exc_info and record.exc_info[0]:
                payload["exception"] = self.formatException(record.exc_info)
            return json.dumps(payload, default=str)
        else:
            # Human-readable format
            msg = record.getMessage()
            parts = [
                ts[11:23],  # Time only
                record.levelname[0],  # Single letter: I, W, E, D
                f"[{correlation_id}]",
                f"<{component}>",
                msg,
            ]
            if record.exc_info and record.exc_info[0]:
                parts.append(f"({record.exc_info[0].__name__})")
            return " ".join(parts)


#  Logger factory 
_initialized = False


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger for the given component name.

    Usage:
        logger = get_logger(__name__)
        logger.info("Agent started", extra={"agent": "researcher", "step": 1})
    """
    global _initialized

    if not _initialized:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        root = logging.getLogger()
        root.addHandler(handler)
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        root.setLevel(getattr(logging, level, logging.INFO))
        _initialized = True

    return logging.getLogger(name)
