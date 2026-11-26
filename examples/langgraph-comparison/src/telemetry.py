# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Observability utilities — metrics, timing, and tracing for agent pipelines.

In production, these would feed into Prometheus / OpenTelemetry / Datadog.
Here they provide structured timing data that Actae captures as event metadata.
"""

import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


#  Timer 
@dataclass
class Span:
    """A named timed operation with metadata — analogous to an OpenTelemetry span."""
    name: str
    start_time: float
    end_time: Optional[float] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    parent_id: Optional[str] = None
    span_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])

    @property
    def duration_ms(self) -> float:
        """Wall-clock duration in milliseconds."""
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000

    def finish(self, **attrs: Any) -> "Span":
        self.end_time = time.time()
        self.attributes.update(attrs)
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "span_id": self.span_id,
            "name": self.name,
            "duration_ms": round(self.duration_ms, 1),
            "attributes": self.attributes,
        }


#  Tracer — a simple span tree 
class Tracer:
    """Collects execution spans into a tree for export to Actae / logs.

    Usage:
        tracer = Tracer()
        with tracer.span("researcher") as sp:
            sp.attributes["findings"] = 5
        tracer.flush()  # → records spans as Actae events
    """

    def __init__(self) -> None:
        self._spans: List[Span] = []
        self._stack: List[str] = []  # span_id stack for parent tracking

    @contextmanager
    def span(self, name: str, **attrs: Any):
        """Context manager that times a named operation."""
        span = Span(
            name=name,
            start_time=time.time(),
            attributes=attrs,
            parent_id=self._stack[-1] if self._stack else None,
        )
        self._stack.append(span.span_id)
        try:
            yield span
        except Exception as exc:
            span.attributes["error"] = str(exc)
            span.finish()
            self._spans.append(span)
            raise
        else:
            span.finish()
            self._spans.append(span)
        finally:
            if self._stack and self._stack[-1] == span.span_id:
                self._stack.pop()

    @property
    def spans(self) -> List[Span]:
        return list(self._spans)

    @property
    def total_duration_ms(self) -> float:
        if not self._spans:
            return 0.0
        first = self._spans[0].start_time
        last = max(s.end_time or time.time() for s in self._spans)
        return (last - first) * 1000

    def clear(self) -> None:
        self._spans.clear()
        self._stack.clear()
