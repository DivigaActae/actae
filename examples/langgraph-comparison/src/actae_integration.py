# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Actae integration for agent pipelines — persistence, observability, and
lifecycle management with enterprise-grade patterns.

Key features:
  - Channel lifecycle (create / fork / delete)
  - Event recording with structured metadata
  - State checkpointing and resume
  - Execution tree management (fork hierarchy)
  - Circuit-breaker protected Actae calls

Usage:
    actae = await ActaeIntegration.create(config)
    async with actae.session("my-pipeline") as session:
        await session.record_step("researcher", input={...}, output={...})
"""

import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional

from src.config import ActaeConfig
from src.resilience import CircuitBreaker, retry_async

logger = logging.getLogger(__name__)


#  Actae client wrapper 
class ActaeIntegration:
    """Production wrapper around ActaeClient with resilience patterns.

    This is NOT a thin wrapper — it adds:
      - Circuit breaker to prevent cascading failures
      - Retry with exponential backoff for transient errors
      - Structured event metadata (correlation IDs, timestamps)
      - Channel lifecycle management
      - Logging with context propagation
    """

    def __init__(self, config: ActaeConfig):
        self._config = config
        self._client: Optional[Any] = None
        self._circuit_breaker = CircuitBreaker(
            name="actae",
            failure_threshold=5,
            recovery_timeout_s=30.0,
        )
        self._channel: Optional[str] = None
        self._correlation_id: Optional[str] = None

    @classmethod
    async def create(
        cls,
        config: ActaeConfig,
        correlation_id: Optional[str] = None,
    ) -> "ActaeIntegration":
        """Factory: create and connect."""
        self = cls(config)
        self._correlation_id = correlation_id
        await self._connect()
        return self

    async def _connect(self) -> None:
        """Connect to Actae with retry and circuit breaker."""
        from actae_client import ActaeClient

        try:
            self._client = await retry_async(
                lambda: self._do_connect(),
                max_attempts=3,
                base_delay_s=0.5,
            )
            logger.info("Connected to Actae at %s", self._config.http_url)
        except Exception as exc:
            logger.error("Failed to connect to Actae after retries: %s", exc)
            raise

    async def _do_connect(self):
        """Single connection attempt."""
        from actae_client import ActaeClient

        client = ActaeClient(
            api_key=self._config.api_key,
            endpoint=self._config.http_url,
            ws_endpoint=self._config.ws_endpoint,
        )
        await client.connect()
        return client

    async def close(self) -> None:
        """Gracefully disconnect."""
        if self._client:
            try:
                await self._client.disconnect()
                logger.info("Disconnected from Actae")
            except Exception as exc:
                logger.warning("Error disconnecting from Actae: %s", exc)

    @property
    def client(self) -> Any:
        """Get the underlying Actae client (for advanced use)."""
        if self._client is None:
            raise RuntimeError("Actae not connected. Use 'async with' or await create().")
        return self._client

    #  Channel management 

    async def ensure_channel(
        self,
        channel_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Ensure a channel exists (idempotent)."""
        self._channel = channel_id
        logger.info("Channel: %s", channel_id)
        return channel_id

    async def fork_channel(
        self,
        source_channel: str,
        new_channel: str,
        at_cursor: int,
        reason: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fork a channel — like git branch for agent runs."""
        try:
            result = await self._circuit_breaker.call(
                lambda: self.client.fork(
                    source_channel_id=source_channel,
                    new_channel_id=new_channel,
                    at_cursor=at_cursor,
                    display_name=reason,
                    reason=reason,
                    experiment_metadata=metadata,
                )
            )
            logger.info("Forked %s \u2192 %s at cursor %d", source_channel, new_channel, at_cursor)
            return result
        except Exception as exc:
            logger.error("Fork failed: %s", exc)
            raise

    async def get_fork_tree(self, root_channel: str) -> Any:
        """Get the full execution tree (fork hierarchy)."""
        return await self.client.get_fork_tree(root_channel)

    #  Event recording 

    async def record_event(
        self,
        channel_id: str,
        event_type: str,
        payload: Dict[str, Any],
        *,
        actor: str = "pipeline",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Record an event with structured metadata."""
        event_metadata = {
            "correlation_id": self._correlation_id,
            "environment": "development",
            "pipeline_version": "1.0.0",
            **(metadata or {}),
        }

        try:
            event = await self._circuit_breaker.call(
                lambda: self.client.record(
                    channel_id=channel_id,
                    event_type=event_type,
                    payload=payload,
                    actor=actor,
                    metadata=event_metadata,
                )
            )
            return {
                "id": event.id,
                "cursor": event.cursor,
                "channel_cursor": event.channel_cursor,
                "timestamp": event.timestamp,
                "event_type": event.event_type,
            }
        except Exception as exc:
            logger.error("Failed to record event '%s': %s", event_type, exc)
            raise

    async def replay_events(
        self,
        channel_id: str,
        cursor: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Replay events from a cursor position (for resume)."""
        events = await self.client.replay(channel_id, cursor=cursor, limit=limit)
        return [
            {
                "cursor": e.cursor,
                "channel_cursor": e.channel_cursor,
                "event_type": e.event_type,
                "payload": e.payload,
                "actor": e.actor,
                "timestamp": e.timestamp,
            }
            for e in events
        ]

    async def get_latest_cursor(self, channel_id: str) -> Optional[int]:
        """Get the latest cursor for a channel."""
        return await self.client.get_cursor(channel_id)

    #  State management 

    async def save_state(
        self,
        channel_id: str,
        cursor: int,
        state: Dict[str, Any],
    ) -> int:
        """Save a versioned state snapshot."""
        version = await self.client.save_state(channel_id, cursor, state)
        logger.info(
            "State snapshot saved on %s at cursor %d (version %d)",
            channel_id, cursor, version,
        )
        return version

    async def load_latest_state(
        self,
        channel_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Load the latest state snapshot."""
        return await self.client.latest_state(channel_id)

    async def list_state_versions(self, channel_id: str) -> List[Dict[str, Any]]:
        """List all state versions (for rollback UI)."""
        return await self.client.list_states(channel_id)

    #  Context manager 

    async def __aenter__(self) -> "ActaeIntegration":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


#  Lightweight session for script use 
@dataclass
class AgentSession:
    """A single agent pipeline run with Actae instrumentation.

    This is a simplified version of AgentSession from the SDK, adapted
    for the comparison scripts with explicit step recording.
    """

    actae: ActaeIntegration
    channel_id: str
    params: Dict[str, Any] = field(default_factory=dict)
    _step_count: int = 0
    _last_cursor: Optional[int] = None

    async def record_step(
        self,
        step_type: str,
        *,
        input: Any = None,
        output: Any = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Record a single agent step."""
        self._step_count += 1
        payload = {
            "step_number": self._step_count,
            "step_type": step_type,
        }
        if input is not None:
            payload["input"] = input
        if output is not None:
            payload["output"] = output

        result = await self.actae.record_event(
            channel_id=self.channel_id,
            event_type=step_type,
            payload=payload,
            actor="pipeline",
            metadata=metadata,
        )
        self._last_cursor = result["cursor"]

        logger.debug(
            "Step %d [%s]: cursor=%d %s",
            self._step_count, step_type, result["cursor"],
            f"input={len(str(input))}b" if input is not None else "",
        )

        return result

    async def save_state(self, state: Dict[str, Any]) -> int:
        """Save a versioned state snapshot aligned to the last recorded cursor.

        This captures the full agent context (topic, findings, insights, draft,
        scores, revision notes) as a persistent snapshot that survives restarts.
        """
        if self._last_cursor is None:
            logger.warning("No cursor available for state snapshot — skipping")
            return -1
        return await self.actae.save_state(
            self.channel_id, self._last_cursor, state,
        )

    @asynccontextmanager
    async def trace_step(
        self,
        step_type: str,
        *,
        input: Any = None,
        output: Any = None,
        **metadata: Any,
    ) -> AsyncIterator[None]:
        """Context manager that records start/completion with full context.

        Records input state at start and output state at completion,
        so every agent step is fully visible in Actae events.
        """
        t0 = time.time()
        await self.record_step(f"{step_type}.started", input=input, metadata=metadata)
        try:
            yield
        except Exception as exc:
            elapsed_ms = round((time.time() - t0) * 1000)
            await self.record_step(
                f"{step_type}.failed",
                output={"error": str(exc), "elapsed_ms": elapsed_ms},
                metadata={"error": str(exc), "elapsed_ms": elapsed_ms, **metadata},
            )
            raise
        else:
            elapsed_ms = round((time.time() - t0) * 1000)
            await self.record_step(
                f"{step_type}.completed",
                output=output if output is not None else {"elapsed_ms": elapsed_ms},
                metadata={"elapsed_ms": elapsed_ms, **metadata},
            )
