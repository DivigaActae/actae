# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
MCP bridge — expose tools from MCP servers to the researcher agent.

MCP (Model Context Protocol) servers advertise a set of tools. This module
connects to one or more stdio servers, converts their tool schemas to the
OpenAI/DeepSeek function-calling format, and runs calls through the protocol
so they flow into the same Actae ``tool.<name>.started`` / ``.completed``
events as the builtin tools.

Design notes:
  - Text results are returned in full (no truncation), so the dashboard shows
    everything a tool returned.
  - Binary blocks (image/audio/embedding/...) cannot be inlined — a single
    event or state snapshot is capped at 1 MiB by Actae
    (actae/src/events/handler.rs ::MAX_EVENT_SIZE) and binary base64 blobs
    can exceed that. They are replaced with a compact JSON note (type, mime,
    byte size) rather than the raw bytes.
  - Unknown tool names fall back to the builtin ``src/tools.run_tool`` so the
    researcher's builtin tools keep working when MCP is configured.

Usage (see ``MCP_SERVERS`` env var in src/config.py)::

    async with await MCPTools.start(config.mcp_servers) as mcp:
        tools = list(mcp.schemas)          # merge into the researcher's toolset
        execute = mcp.execute              # passed as ``execute_tool``
"""

import json
import logging
from contextlib import AsyncExitStack
from typing import Any, Dict, List, Optional

from src.tools import run_tool

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    _MCP_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - guarded optional dep
    ClientSession = None  # type: ignore
    StdioServerParameters = None  # type: ignore
    stdio_client = None  # type: ignore
    _MCP_IMPORT_ERROR = exc

logger = logging.getLogger(__name__)

# Cap on the serialized string fed back to the model for one MCP text payload.
# Kept generous (uncapped for result storage) — text blocks are not truncated;
# this only guards the size of a single combined model round-trip.
_MAX_TEXT_ROUNDTRIP = 1_000_000
_MAX_RESOURCE_TEXT = 2000


def _serialize_content(content: Any) -> str:
    """Convert MCP call_tool ContentBlock list into a displayable string.

    Text blocks are concatenated in full; binary blocks are summarized as a
    JSON note (they cannot be inlined inside Actae's 1 MiB event payloads).
    """
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        try:
            return json.dumps(content, default=str, ensure_ascii=False)
        except Exception:
            return str(content)

    parts: List[str] = []
    total = 0
    for block in content:
        if isinstance(block, str):
            parts.append(block)
            total += len(block)
            continue
        b = block.model_dump() if hasattr(block, "model_dump") else dict(block)
        btype = b.get("type")
        if btype == "text":
            text = b.get("text", "")
            parts.append(text)
            total += len(text)
        elif btype in ("image", "audio", "video", "blob"):
            data = b.get("data") or ""
            parts.append(json.dumps({
                "type": btype,
                "mime_type": b.get("mimeType"),
                "size_bytes": len(data),
                "note": "binary content omitted (would exceed Actae event limits); stored as size reference",
            }))
        elif btype == "embedding":
            emb = b.get("embeddings") or []
            parts.append(json.dumps({
                "type": "embedding",
                "count": len(emb),
                "dimensions": len(emb[0]) if emb else 0,
            }))
        elif btype == "resource":
            r = b.get("resource") or {}
            note = r.get("text") or ""
            parts.append(json.dumps({
                "type": "resource",
                "uri": r.get("uri"),
                "mime_type": r.get("mimeType"),
                "text_preview": note[:_MAX_RESOURCE_TEXT],
                "text_length": len(note),
            }))
        else:
            parts.append(json.dumps({"type": btype, "value": b}, default=str))
        if total > _MAX_TEXT_ROUNDTRIP:
            break
    return "\n\n".join(parts)


class MCPTools:
    """Live connections to a set of MCP servers and their tools.

    After ``await MCPTools.start(configs)`` the instance exposes:
      * ``schemas`` — OpenAI-style tool definitions for the LLM
      * ``execute(name, arguments)`` — runs builtin or MCP tools, returns str
    Use it as an async context manager so the stdio sessions are cleaned up.
    """

    def __init__(self) -> None:
        self._conns: List[Dict[str, Any]] = []
        self.schemas: List[Dict[str, Any]] = []
        self._tools: Dict[str, Dict[str, Any]] = {}

    @classmethod
    async def start(cls, server_configs: List[Dict[str, Any]]) -> "MCPTools":
        """Connect to MCP servers and build tool schemas. Returns an instance.

        Returns a closed (empty) instance when there is nothing to connect to
        or when the ``mcp`` Python package is unavailable.
        """
        self = cls()
        if not server_configs:
            return self
        if _MCP_IMPORT_ERROR is not None:
            logger.warning("'mcp' SDK not installed — MCP tools disabled: %s", _MCP_IMPORT_ERROR)
            return self

        for cfg in server_configs:
            try:
                await self._connect(cfg)
            except Exception as exc:
                logger.error("MCP server %s failed to start: %s", cfg.get("name", "?"), exc)
        return self

    async def _connect(self, config: Dict[str, Any]) -> None:
        name = config.get("name") or config.get("command", "mcp")
        params = StdioServerParameters(
            command=config.get("command", "npx"),
            args=[str(a) for a in config.get("args", [])],
            env=config.get("env"),
            cwd=config.get("cwd"),
        )
        stack = AsyncExitStack()
        read, write = await stack.enter_async_context(stdio_client(params))
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()

        result = await session.list_tools()
        tool_names: List[str] = []
        for tool in result.tools:
            schema = {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description or "",
                    "parameters": tool.inputSchema or {"type": "object", "properties": {}},
                },
            }
            self.schemas.append(schema)
            self._tools[tool.name] = {"session": session, "server": name}
            tool_names.append(tool.name)

        self._conns.append({"server": name, "stack": stack, "session": session})
        logger.info("MCP server '%s' connected with %d tools: %s", name, len(tool_names), tool_names)

    async def execute(self, name: str, arguments: Dict[str, Any]) -> str:
        """Run a tool by name. MCP tools are dispatched to their server
        session; everything else falls back to the builtin runner."""
        entry = self._tools.get(name)
        if entry is None:
            return await run_tool(name, arguments)

        session = entry["session"]
        logger.debug("MCP tool call: %s(%s)", name, arguments)
        try:
            res = await session.call_tool(name, arguments=arguments or {})
            if getattr(res, "isError", False):
                detail = ""
                if getattr(res, "content", None):
                    first = res.content[0]
                    detail = getattr(first, "text", None) or _serialize_content(res.content)
                return json.dumps({"error": f"MCP tool '{name}' returned an error", "detail": detail}, ensure_ascii=False)
            return _serialize_content(res.content)
        except Exception as exc:
            logger.warning("MCP tool '%s' failed: %s", name, exc)
            return json.dumps({"error": f"MCP tool '{name}' failed: {exc}"}, ensure_ascii=False)

    async def aclose(self) -> None:
        for conn in self._conns:
            try:
                await conn["stack"].aclose()
            except Exception as exc:
                logger.warning("Error closing MCP server '%s': %s", conn["server"], exc)
        self._conns.clear()

    async def __aenter__(self) -> "MCPTools":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()