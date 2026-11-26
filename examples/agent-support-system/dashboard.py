# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Real-time Agent Execution Dashboard.

Shows live agent conversation streams, execution replay, and cross-channel
querying. Connects to Actae via HTTP for replay/query and WebSocket for
live subscription.

Usage:
  python dashboard.py
  # Open http://localhost:9090
"""

import asyncio
import json
import os
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from config import ACTAE_API_URL, DASHBOARD_HOST, DASHBOARD_PORT

app = FastAPI(title="Actae Agent Support Dashboard")

# ---------------------------------------------------------------------------
# In-memory event store for the dashboard
# ---------------------------------------------------------------------------

RECENT_CHANNELS = deque(maxlen=100)
CHANNEL_EVENTS: dict[str, list[dict]] = {}
EVENT_COUNTERS = defaultdict(int)


def _ensure_channel(channel_id: str):
    if channel_id not in CHANNEL_EVENTS:
        CHANNEL_EVENTS[channel_id] = []
        RECENT_CHANNELS.appendleft(channel_id)


# ---------------------------------------------------------------------------
# Actae API client
# ---------------------------------------------------------------------------

class ActaeClient:
    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip("/")
        self._verify = api_url.startswith("https")

    async def replay(self, channel_id: str, cursor: int = 0, limit: int = 500) -> list:
        url = f"{self.api_url}/api/v1/events/replay/{channel_id}?cursor={cursor}&limit={limit}"
        async with httpx.AsyncClient(verify=False, timeout=15.0) as c:
            resp = await c.get(url)
            if resp.status_code == 200:
                return resp.json().get("events", [])
        return []

    async def query(self, event_type: str | None = None,
                    actor: str | None = None,
                    limit: int = 100) -> list:
        body = {"limit": limit}
        if event_type:
            body["event_type"] = event_type
        if actor:
            body["actor"] = actor
        url = f"{self.api_url}/api/v1/events/query"
        async with httpx.AsyncClient(verify=False, timeout=15.0) as c:
            try:
                resp = await c.post(url, json=body, headers={"Content-Type": "application/json"})
                if resp.status_code == 200:
                    return resp.json().get("events", [])
            except Exception:
                pass
        return []

    async def latest_cursor(self, channel_id: str) -> int | None:
        url = f"{self.api_url}/api/v1/events/cursor/{channel_id}"
        async with httpx.AsyncClient(verify=self._verify, timeout=10.0) as c:
            try:
                resp = await c.get(url)
                if resp.status_code == 200:
                    return resp.json().get("latest_cursor")
            except Exception:
                pass
        return None

    async def health(self) -> dict:
        url = f"{self.api_url}/healthz"
        async with httpx.AsyncClient(verify=self._verify, timeout=5.0) as c:
            try:
                resp = await c.get(url)
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                pass
        return {"status": "unreachable"}


actae = ActaeClient(ACTAE_API_URL)


# ---------------------------------------------------------------------------
# SSE endpoint for live event stream
# ---------------------------------------------------------------------------

@app.get("/api/stream")
async def event_stream(request: Request):
    async def generate():
        try:
            events = await actae.replay("_latest_")
            yield f"data: {json.dumps({'type': 'init', 'channels': list(RECENT_CHANNELS)[:20]})}\n\n"
        except Exception:
            yield f"data: {json.dumps({'type': 'init', 'channels': []})}\n\n"

        while True:
            if await request.is_disconnected():
                break
            try:
                health = await actae.health()
                yield f"data: {json.dumps({'type': 'heartbeat', 'health': health})}\n\n"
            except Exception:
                pass
            await asyncio.sleep(5)

    from fastapi.responses import StreamingResponse
    return StreamingResponse(generate(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def api_health():
    health = await actae.health()
    return health


@app.get("/api/channels")
async def list_channels(limit: int = Query(50)):
    channels = list(RECENT_CHANNELS)[:limit]
    result = []
    for ch in channels:
        events = CHANNEL_EVENTS.get(ch, [])
        result.append({
            "channel_id": ch,
            "event_count": len(events),
            "types": list({e.get("type") for e in events}),
            "last_event": events[-1] if events else None,
        })
    return {"channels": result}


@app.get("/api/channels/{channel_id}")
async def get_channel(channel_id: str, cursor: int = Query(0)):
    events = await actae.replay(channel_id, cursor=cursor)
    for e in events:
        _ensure_channel(e.get("channel_id", channel_id))
        ch = e.get("channel_id", channel_id)
        if ch not in CHANNEL_EVENTS:
            CHANNEL_EVENTS[ch] = []
        CHANNEL_EVENTS[ch].append(e)
        EVENT_COUNTERS[e.get("type", "unknown")] += 1
    return {"channel_id": channel_id, "events": events}


@app.get("/api/channels/{channel_id}/timeline")
async def channel_timeline(channel_id: str):
    events = await actae.replay(channel_id)
    timeline = []
    for e in events:
        timeline.append({
            "cursor": e.get("cursor"),
            "type": e.get("type"),
            "label": e.get("type", "").replace(".", " · "),
            "payload": e.get("payload", {}),
            "timestamp": e.get("timestamp", ""),
        })
    return {"channel_id": channel_id, "timeline": timeline}


@app.get("/api/stats")
async def stats():
    return {
        "channels_tracked": len(RECENT_CHANNELS),
        "events_cached": sum(len(v) for v in CHANNEL_EVENTS.values()),
        "type_breakdown": dict(EVENT_COUNTERS.most_common(20)),
    }


# ---------------------------------------------------------------------------
# HTML pages
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Actae · Agent Support Dashboard</title>
<style>
  :root {
    --bg: #0d1117;
    --surface: #161b22;
    --border: #30363d;
    --text: #e6edf3;
    --muted: #8b949e;
    --green: #3fb950;
    --yellow: #d29922;
    --red: #f85149;
    --blue: #58a6ff;
    --purple: #bc8cff;
    --orange: #f0883e;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: var(--bg); color: var(--text); font-size: 14px; }
  .layout { display: grid; grid-template-columns: 280px 1fr 320px; height: 100vh; }
  .panel { border-right: 1px solid var(--border); overflow-y: auto; }
  .panel:last-child { border-right: none; }

  .panel-header { padding: 12px 16px; border-bottom: 1px solid var(--border);
                  font-weight: 600; font-size: 13px; text-transform: uppercase;
                  letter-spacing: 0.5px; color: var(--muted); }
  .channel-item { padding: 10px 16px; border-bottom: 1px solid var(--border);
                  cursor: pointer; transition: background 0.15s; }
  .channel-item:hover { background: var(--surface); }
  .channel-item .name { font-weight: 500; }
  .channel-item .meta { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .channel-item .scenario { display: inline-block; font-size: 11px;
    padding: 1px 6px; border-radius: 4px; background: var(--surface); margin-top: 4px; }

  .channel-item.active { background: var(--surface); border-left: 3px solid var(--blue); }

  .timeline { padding: 8px 0; }
  .event { display: flex; gap: 12px; padding: 6px 16px; align-items: flex-start; }
  .event:hover { background: rgba(88,166,255,0.05); }
  .event-cursor { font-size: 11px; color: var(--muted); min-width: 36px; font-family: monospace; }
  .event-line { flex: 1; }
  .event-header { display: flex; align-items: center; gap: 8px; }
  .event-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .event-type { font-family: monospace; font-size: 13px; }
  .event-time { font-size: 11px; color: var(--muted); font-family: monospace; }
  .event-payload { font-size: 12px; color: var(--muted); margin-top: 2px;
                   font-family: monospace; white-space: pre-wrap; word-break: break-all; }

  .dot-started { background: var(--green); }
  .dot-completed { background: var(--blue); }
  .dot-tool { background: var(--yellow); }
  .dot-human { background: var(--purple); }
  .dot-message { background: var(--orange); }
  .dot-checkpoint { background: var(--muted); }
  .dot-error { background: var(--red); }
  .dot-default { background: var(--muted); }

  .stats-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; padding: 12px 16px; }
  .stat-card { background: var(--surface); padding: 10px; border-radius: 6px;
               border: 1px solid var(--border); }
  .stat-value { font-size: 20px; font-weight: 600; }
  .stat-label { font-size: 11px; color: var(--muted); text-transform: uppercase; margin-top: 2px; }

  .type-badge { display: inline-block; font-size: 11px; padding: 1px 6px;
    border-radius: 3px; background: var(--surface); margin: 1px; font-family: monospace; }

  .json-toggle { float: right; font-size: 11px; color: var(--blue); cursor: pointer; }
  .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
           background: rgba(0,0,0,0.6); z-index: 100; }
  .modal-content { background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
                   width: 90%; max-width: 800px; margin: 40px auto; padding: 24px; max-height: 80vh; overflow-y: auto; }
  .modal-close { float: right; font-size: 20px; cursor: pointer; color: var(--muted); }

  .search-box { width: 100%; padding: 8px 12px; background: var(--surface);
    border: 1px solid var(--border); border-radius: 6px; color: var(--text);
    font-size: 13px; margin-bottom: 8px; }
  .search-box:focus { outline: none; border-color: var(--blue); }

  .empty-state { padding: 40px; text-align: center; color: var(--muted); }

  .status-dot { width: 6px; height: 6px; border-radius: 50%; display: inline-block; margin-right: 6px; }
  .status-connected { background: var(--green); }
  .status-disconnected { background: var(--red); }
</style>
</head>
<body>
<div class="layout">
  <!-- Sidebar: channel list -->
  <div class="panel">
    <div class="panel-header">
      Conversations
      <span class="status-dot status-disconnected" id="statusDot"></span>
    </div>
    <input class="search-box" id="searchInput" placeholder="Filter channels..." oninput="filterChannels()">
    <div id="channelList"></div>
  </div>

  <!-- Center: timeline -->
  <div class="panel" style="display:flex;flex-direction:column;">
    <div class="panel-header">
      Execution Timeline
      <span style="font-weight:400;text-transform:none;letter-spacing:0;color:var(--muted);font-size:12px" id="channelName"></span>
    </div>
    <div class="timeline" id="timeline"></div>
  </div>

  <!-- Right: stats -->
  <div class="panel">
    <div class="panel-header">System Stats</div>
    <div class="stats-grid" id="statsGrid"></div>
    <div class="panel-header" style="margin-top:8px;">Event Types</div>
    <div style="padding:12px 16px" id="typeBreakdown"></div>
  </div>
</div>

<!-- Detail modal -->
<div class="modal" id="detailModal" onclick="this.style.display='none'">
  <div class="modal-content" onclick="event.stopPropagation()">
    <span class="modal-close" onclick="document.getElementById('detailModal').style.display='none'">&times;</span>
    <h3 style="margin-bottom:12px;">Event Detail</h3>
    <pre id="detailContent" style="background:var(--bg);padding:16px;border-radius:6px;overflow-x:auto;"></pre>
  </div>
</div>

<script>
let channels = [];
let selectedChannel = null;
let currentTimeline = [];

function eventDot(type) {
  if (type.startsWith('agent.started')) return 'dot-started';
  if (type.startsWith('agent.completed') || type.endsWith('.completed')) return 'dot-completed';
  if (type.startsWith('tool.')) return 'dot-tool';
  if (type.startsWith('human.')) return 'dot-human';
  if (type.startsWith('message.')) return 'dot-message';
  if (type.startsWith('checkpoint.')) return 'dot-checkpoint';
  if (type.startsWith('error')) return 'dot-error';
  return 'dot-default';
}

function eventLabel(type) {
  return type.replace(/\./g, ' · ');
}

function renderChannels(list) {
  const el = document.getElementById('channelList');
  if (!list.length) {
    el.innerHTML = '<div class="empty-state">No conversations yet. Start the simulator:<br><code style="font-size:12px;background:var(--surface);padding:2px 6px;border-radius:3px;">python simulator.py stream</code></div>';
    return;
  }
  el.innerHTML = list.map(ch => {
    const isActive = ch.channel_id === selectedChannel;
    const scenario = ch.last_event ? (ch.last_event.payload || {}).agent || '' : '';
    const lastType = ch.last_event ? ch.last_event.type : '';
    const dot = eventDot(lastType);
    return `<div class="channel-item${isActive ? ' active' : ''}" onclick="selectChannel('${ch.channel_id}')">
      <div class="name"><span class="event-dot ${dot}" style="display:inline-block;vertical-align:middle;margin-right:6px;"></span>${ch.channel_id.slice(0, 12)}</div>
      <div class="meta">${ch.event_count} events</div>
      ${scenario ? `<div class="scenario">${scenario}</div>` : ''}
    </div>`;
  }).join('');
}

function renderTimeline(events) {
  currentTimeline = events;
  const el = document.getElementById('timeline');
  if (!events.length) {
    el.innerHTML = '<div class="empty-state">Select a conversation to see its execution timeline.</div>';
    return;
  }
  el.innerHTML = events.map(e => {
    const dot = eventDot(e.type);
    const label = eventLabel(e.type);
    const ts = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '';
    const payload = e.payload ? JSON.stringify(e.payload, null, 2) : '';
    const truncated = payload.length > 120 ? payload.slice(0, 120) + '...' : payload;
    return `<div class="event" onclick="showDetail('${encodeURIComponent(JSON.stringify(e))}')">
      <div class="event-cursor">#${e.cursor}</div>
      <div class="event-line">
        <div class="event-header">
          <span class="event-dot ${dot}"></span>
          <span class="event-type">${label}</span>
          <span class="event-time">${ts}</span>
        </div>
        ${truncated ? `<div class="event-payload">${truncated}</div>` : ''}
      </div>
    </div>`;
  }).join('');
}

function renderStats(stats) {
  const el = document.getElementById('statsGrid');
  el.innerHTML = `
    <div class="stat-card"><div class="stat-value">${stats.channels_tracked}</div><div class="stat-label">Conversations</div></div>
    <div class="stat-card"><div class="stat-value">${stats.events_cached}</div><div class="stat-label">Events Cached</div></div>
  `;
  const breakdown = document.getElementById('typeBreakdown');
  if (stats.type_breakdown) {
    breakdown.innerHTML = Object.entries(stats.type_breakdown).map(([t, n]) =>
      `<span class="type-badge" style="background:var(--surface)">${t} (${n})</span>`
    ).join('');
  }
}

function showDetail(encoded) {
  const e = JSON.parse(decodeURIComponent(encoded));
  document.getElementById('detailContent').textContent = JSON.stringify(e, null, 2);
  document.getElementById('detailModal').style.display = 'block';
}

function filterChannels() {
  const q = document.getElementById('searchInput').value.toLowerCase();
  const filtered = channels.filter(c => c.channel_id.toLowerCase().includes(q));
  renderChannels(filtered);
}

async function selectChannel(channelId) {
  selectedChannel = channelId;
  document.getElementById('channelName').textContent = channelId.slice(0, 16) + '...';
  try {
    const resp = await fetch(`/api/channels/${channelId}`);
    const data = await resp.json();
    renderTimeline(data.events);
  } catch (e) {
    console.error('Failed to load channel:', e);
  }
  // Update sidebar highlight
  renderChannels(channels);
}

async function refresh() {
  try {
    const [chResp, statsResp] = await Promise.all([
      fetch('/api/channels?limit=50'),
      fetch('/api/stats'),
    ]);
    const chData = await chResp.json();
    const statsData = await statsResp.json();
    channels = chData.channels;
    renderChannels(channels);
    renderStats(statsData);
    document.getElementById('statusDot').className = 'status-dot status-connected';
  } catch (e) {
    document.getElementById('statusDot').className = 'status-dot status-disconnected';
  }
}

// Auto-refresh
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTML


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print(f"  Actae Agent Support Dashboard")
    print(f"  Listening on http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
    print(f"  Actae API: {ACTAE_API_URL}")
    print()
    uvicorn.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT, log_level="warning")


if __name__ == "__main__":
    main()
