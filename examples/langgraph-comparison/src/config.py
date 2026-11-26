# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Enterprise configuration management with environment-based overrides,
validation, and typed settings objects.

Pattern: Singleton config with frozen dataclass — loaded once at startup,
injected into components that need it.
"""

import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal


#  Sentinel for unset values 
_UNSET = object()


#  Configuration schema 
@dataclass(frozen=True)
class DeepSeekConfig:
    """DeepSeek LLM connection settings."""
    api_key: str = ""
    base_url: str = "https://api.deepseek.com"
    model: str = "deepseek-chat"
    max_retries: int = 3
    request_timeout_seconds: float = 60.0
    default_temperature: float = 0.7
    max_tokens: int = 2048

    def __post_init__(self):
        pass  # Validation happens lazily at LLM call time


@dataclass(frozen=True)
class ActaeConfig:
    """Actae connection settings."""
    http_url: str = "http://localhost:8002"
    ws_url: str = "ws://localhost:8002"
    ws_path: str = "/ws"
    api_key: str = "sk-dev-0000000000000000000000"
    channel_prefix: str = "langgraph-research"
    request_timeout_seconds: float = 30.0
    auto_reconnect: bool = True

    @property
    def ws_endpoint(self) -> str:
        return f"{self.ws_url}{self.ws_path}"


@dataclass(frozen=True)
class PipelineConfig:
    """Pipeline runtime settings."""
    topic: str = "The future of AI agents in software development: capabilities, risks, and adoption timeline"
    max_revisions: int = 2
    min_pass_score: int = 7
    snapshot_interval: int = 2
    log_level: str = "INFO"

    def __post_init__(self):
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR"}
        if self.log_level.upper() not in valid_levels:
            raise ValueError(
                f"Invalid log level: {self.log_level}. "
                f"Must be one of {valid_levels}"
            )


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""
    deepseek: DeepSeekConfig
    actae: ActaeConfig = field(default_factory=ActaeConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
    mcp_servers: List[Dict[str, Any]] = field(default_factory=list)


#  Config loader 
class ConfigLoader:
    """Loads configuration from environment variables with sensible defaults.

    Usage:
        config = ConfigLoader.load()
        llm = get_llm(config.deepseek)
    """

    @classmethod
    def load(cls, env_file: Optional[str] = None) -> AppConfig:
        """Load configuration from environment, optionally from a .env file."""
        if env_file:
            cls._load_dotenv(env_file)
        return AppConfig(
            deepseek=DeepSeekConfig(
                api_key=cls._get("DEEPSEEK_API_KEY", ""),
                base_url=cls._get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
                model=cls._get("DEEPSEEK_MODEL", "deepseek-chat"),
                max_retries=int(cls._get("DEEPSEEK_MAX_RETRIES", "3")),
                request_timeout_seconds=float(cls._get("DEEPSEEK_TIMEOUT", "60")),
                default_temperature=float(cls._get("DEEPSEEK_TEMPERATURE", "0.7")),
                max_tokens=int(cls._get("DEEPSEEK_MAX_TOKENS", "2048")),
            ),
            actae=ActaeConfig(
                http_url=cls._get("ACTAE_URL", "http://localhost:8002"),
                ws_url=cls._get("ACTAE_WS_URL", "ws://localhost:8002"),
                ws_path=cls._get("ACTAE_WS_PATH", "/ws"),
                api_key=cls._get("ACTAE_API_KEY", "sk-dev-0000000000000000000000"),
                channel_prefix=cls._get("ACTAE_CHANNEL_PREFIX", "langgraph-research"),
                request_timeout_seconds=float(cls._get("ACTAE_TIMEOUT", "30")),
                auto_reconnect=cls._get("ACTAE_AUTO_RECONNECT", "true").lower() == "true",
            ),
            pipeline=PipelineConfig(
                topic=cls._get(
                    "PIPELINE_TOPIC",
                    "The future of AI agents in software development: "
                    "capabilities, risks, and adoption timeline",
                ),
                max_revisions=int(cls._get("PIPELINE_MAX_REVISIONS", "2")),
                min_pass_score=int(cls._get("PIPELINE_MIN_PASS_SCORE", "7")),
                snapshot_interval=int(cls._get("PIPELINE_SNAPSHOT_INTERVAL", "2")),
                log_level=cls._get("LOG_LEVEL", "INFO"),
            ),
            mcp_servers=cls._mcp_servers(),
        )

    @staticmethod
    def _mcp_servers() -> List[Dict[str, Any]]:
        """Parse the ``MCP_SERVERS`` env var into a server config list.

        Expected shape — a JSON array of stdio server specs::

            MCP_SERVERS='[{"name":"github","command":"npx",
                           "args":["-y","@modelcontextprotocol/server-github"],
                           "env":{"GITHUB_TOKEN":"..."}}]'

        Each entry maps to ``StdioServerParameters``: ``name``, ``command``
        (default ``npx``), ``args``, ``env``, ``cwd``.
        """
        raw = os.environ.get("MCP_SERVERS")
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"MCP_SERVERS must be a JSON array of server configs: {exc}"
            ) from exc
        if not isinstance(parsed, list):
            parsed = [parsed]
        servers: List[Dict[str, Any]] = []
        for srv in parsed:
            if isinstance(srv, dict) and srv.get("command"):
                servers.append(srv)
        return servers

    @staticmethod
    def _get(key: str, default: Optional[str] = None) -> str:
        """Get an env var, raising if required and missing."""
        value = os.environ.get(key)
        if value is not None:
            return value
        if default is not None:
            return default
        raise ValueError(
            f"Required environment variable '{key}' is not set. "
            f"See .env.example for available options."
        )

    @staticmethod
    def _load_dotenv(path: str) -> None:
        """Simple .env loader — no external dependency needed."""
        env_path = Path(path)
        if not env_path.exists():
            return
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("\"'")
            if key not in os.environ:
                os.environ.setdefault(key, value)
