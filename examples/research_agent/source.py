# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""The real "big source": a live arXiv literature search.

The research agent ingests **real papers** from the open arXiv API (no key,
permissive terms). A search for a topic returns up to `max_results` papers; each
becomes an addressable chunk (`2401.12345` → title + abstract). That is the
material the agent filters, extracts claims from, and writes a brief about.

Reproducibility
---------------
The fetched result is cached to `data/arxiv-<slug>.json` and committed, so the
demo (and its tests) re-run on the *same* inputs offline. `refresh=True` (or
`--refresh` on the CLI) re-hits arXiv. Everything here is stdlib only — no
`requests`, no API key.

arXiv asks API clients to identify themselves; we send a descriptive
User-Agent. See https://info.arxiv.org/help/api/ .
"""

from __future__ import annotations

import datetime as dt
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_TOPIC = "large language model agents"
DEFAULT_MAX_RESULTS = 40

_ARXIV_API = "https://export.arxiv.org/api/query"
_USER_AGENT = "actae-research-demo/1.0 (+https://github.com/divigaactae/actae)"
_DATA_DIR = Path(__file__).resolve().parent / "data"

_ATOM = "{http://www.w3.org/2005/Atom}"
_ARXIV_NS = "{http://arxiv.org/schemas/atom}"


def _slug(topic: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", topic.strip().lower()).strip("-")[:60] or "query"


def snapshot_path(topic: str) -> Path:
    return _DATA_DIR / f"arxiv-{_slug(topic)}.json"


def _clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def short_id(raw_id: str) -> str:
    """`http://arxiv.org/abs/2401.12345v2` -> `2401.12345`."""
    tail = raw_id.rstrip("/").rsplit("/", 1)[-1]
    return re.sub(r"v\d+$", "", tail)


def parse_atom(xml_bytes: bytes) -> List[Dict[str, str]]:
    """Parse an arXiv Atom feed into paper dicts (pure; unit-testable).

    Rejects any document carrying a DTD or entity declarations — `xml.etree`
    is not hardened against hostile XML, and a feed is untrusted input.
    """
    if re.search(rb"<!DOCTYPE|<!ENTITY", xml_bytes, re.IGNORECASE):
        raise ValueError("refusing arXiv feed with DTD/entity declarations (untrusted XML)")
    root = ET.fromstring(xml_bytes)
    papers: List[Dict[str, str]] = []
    for entry in root.findall(f"{_ATOM}entry"):
        raw_id = _clean(entry.findtext(f"{_ATOM}id"))
        if not raw_id:
            continue
        authors = [
            _clean(name.text)
            for name in entry.findall(f"{_ATOM}author/{_ATOM}name")
            if _clean(name.text)
        ]
        categories = [
            cat.attrib.get("term", "")
            for cat in entry.findall(f"{_ATOM}category")
            if cat.attrib.get("term")
        ]
        papers.append(
            {
                "id": short_id(raw_id),
                "title": _clean(entry.findtext(f"{_ATOM}title")),
                "abstract": _clean(entry.findtext(f"{_ATOM}summary")),
                "authors": authors,
                "published": _clean(entry.findtext(f"{_ATOM}published"))[:10],
                "categories": categories,
                "url": f"https://arxiv.org/abs/{short_id(raw_id)}",
            }
        )
    return papers


def fetch_papers(topic: str, max_results: int = DEFAULT_MAX_RESULTS) -> Dict:
    """Query the arXiv API for `topic`; return a snapshot dict.

    Retries transient network failures (connection, timeout, HTTP 5xx/429) with
    backoff; a persistent failure raises rather than returning an empty result.
    """
    query = urllib.parse.urlencode(
        {
            "search_query": f'all:"{topic}"',
            "start": 0,
            "max_results": max(1, min(int(max_results), 100)),
            "sortBy": "relevance",
        }
    )
    request = urllib.request.Request(_ARXIV_API + "?" + query, headers={"User-Agent": _USER_AGENT})
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                papers = parse_atom(response.read())
            break
        except urllib.error.HTTPError as err:
            if err.code not in (429,) and err.code < 500:
                raise
            last_error = err
        except (urllib.error.URLError, TimeoutError, OSError) as err:
            last_error = err
        if attempt < 2:
            time.sleep(0.5 * (2 ** attempt))
    else:
        raise RuntimeError(f"arXiv fetch failed after retries: {last_error}")
    return {
        "topic": topic,
        "source": "arxiv",
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "papers": papers,
    }


def save_snapshot(snapshot: Dict) -> Path:
    path = snapshot_path(snapshot["topic"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def load_snapshot(topic: str) -> Optional[Dict]:
    path = snapshot_path(topic)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def get_papers(
    topic: str = DEFAULT_TOPIC,
    *,
    max_results: int = DEFAULT_MAX_RESULTS,
    refresh: bool = False,
) -> Dict:
    """Return a snapshot, preferring the committed cache unless `refresh`."""
    if not refresh:
        cached = load_snapshot(topic)
        if cached and cached.get("papers"):
            return cached
    snapshot = fetch_papers(topic, max_results)
    if snapshot["papers"]:
        save_snapshot(snapshot)
    return snapshot


def chunks(snapshot: Dict) -> List[Dict[str, str]]:
    """Flatten a snapshot into addressable chunks: {id, section, text, ...}."""
    out: List[Dict[str, str]] = []
    for paper in snapshot.get("papers", []):
        out.append(
            {
                "id": paper["id"],
                "section": paper["title"],
                "text": paper["abstract"],
                "authors": ", ".join(paper.get("authors", [])[:3]),
                "published": paper.get("published", ""),
                "url": paper.get("url", ""),
            }
        )
    return out


def word_count(snapshot: Dict) -> int:
    return sum(len(c["text"].split()) for c in chunks(snapshot))
