# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
LangGraph Multi-Agent Research Pipeline — Production Enterprise Edition

A production-grade multi-agent system demonstrating four AI agents
(Researcher → Analyst → Writer → Critic) collaborating on research reports.

Two versions demonstrate the difference Actae makes:
  - 01_plain_langgraph.py  — Pure LangGraph, in-memory state
  - 02_actae_langgraph.py   — Same graph + Actae persistence & observability
  - 03_actae_websocket.py   — Real-time event streaming via WebSocket

Run:
    export DEEPSEEK_API_KEY=sk-...
    python 01_plain_langgraph.py
    python 02_actae_langgraph.py     # requires Actae running
    python 03_actae_websocket.py     # requires Actae running
"""

__version__ = "1.0.0"
