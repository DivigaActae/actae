# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Resilience patterns — retry with exponential backoff, circuit breaker.

Enterprise systems don't fail on first error. They degrade gracefully.
"""

import asyncio
import logging
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


#  Retry with exponential backoff + jitter 
async def retry_async(
    coro_factory: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 3,
    base_delay_s: float = 1.0,
    max_delay_s: float = 30.0,
    jitter: bool = True,
    retryable_exceptions: tuple = (ConnectionError, TimeoutError, OSError),
) -> T:
    """Execute an async call with exponential backoff retry.

    Args:
        coro_factory: Async callable that returns the desired value.
        max_attempts: Total attempts (including the first).
        base_delay_s: Initial delay before first retry.
        max_delay_s: Cap on delay between retries.
        jitter: Add random ±50% jitter to avoid thundering herd.
        retryable_exceptions: Exception types that trigger a retry.

    Returns:
        The result of the successful call.

    Raises:
        The last exception if all attempts fail.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await coro_factory()
        except retryable_exceptions as exc:
            last_exc = exc
            if attempt == max_attempts:
                logger.error(
                    "All %d attempts failed for %s: %s",
                    max_attempts,
                    getattr(coro_factory, "__name__", coro_factory),
                    exc,
                )
                raise

            delay = min(base_delay_s * (2 ** (attempt - 1)), max_delay_s)
            if jitter:
                import random
                delay *= 0.5 + random.random()  # 50-150% of computed delay

            logger.warning(
                "Attempt %d/%d failed: %s. Retrying in %.1fs...",
                attempt, max_attempts, exc, delay,
            )
            await asyncio.sleep(delay)

    # Should not reach here, but satisfy the type checker
    raise RuntimeError("Unexpected retry exit")  # pragma: no cover


#  Simple circuit breaker 
class CircuitBreaker:
    """Prevents cascading failures by short-circuiting after N consecutive errors.

    States:
        CLOSED  →
        OPEN    → failing fast (no calls through)
        HALF_OPEN → testing if service is back
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_s: float = 30.0,
    ):
        self.name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout_s = recovery_timeout_s
        self._failure_count = 0
        self._state: str = "CLOSED"
        self._last_failure_time: Optional[float] = None

    @property
    def state(self) -> str:
        """Current circuit state: CLOSED, OPEN, HALF_OPEN."""
        if self._state == "OPEN" and self._last_failure_time:
            elapsed = asyncio.get_event_loop().time() - self._last_failure_time
            if elapsed >= self._recovery_timeout_s:
                self._state = "HALF_OPEN"
                logger.info("Circuit '%s' → HALF_OPEN (probing)", self.name)
        return self._state

    async def call(self, coro_factory: Callable[[], Awaitable[T]]) -> T:
        """Execute a call through the circuit breaker."""
        current_state = self.state

        if current_state == "OPEN":
            raise CircuitBreakerError(f"Circuit '{self.name}' is OPEN")

        try:
            result = await coro_factory()
            if current_state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failure_count = 0
                logger.info("Circuit '%s' → CLOSED (recovered)", self.name)
            return result
        except Exception as exc:
            self._failure_count += 1
            self._last_failure_time = asyncio.get_event_loop().time()
            if self._failure_count >= self._failure_threshold:
                self._state = "OPEN"
                logger.error(
                    "Circuit '%s' → OPEN after %d failures",
                    self.name, self._failure_count,
                )
            raise exc


class CircuitBreakerError(Exception):
    """Raised when a circuit breaker is OPEN."""
    pass
