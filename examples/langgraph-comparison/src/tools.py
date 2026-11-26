# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Executable tools the researcher agent can call during its LLM loop.

The tools are framework-agnostic (no LangGraph, no Actae). They are plain
async callables that DeepSeek invokes through OpenAI-style function calling.
Each tool has an OpenAPI-style JSON schema so the LLM knows the signature.

Enterprise concerns addressed:
  - No external dependencies for builtin tools (stdlib only: urllib, ast)
  - Full, uncapped tool results so the dashboard shows everything
  - Safe arithmetic via AST evaluation (no eval)
  - Structured JSON-serialized results / errors
"""

import ast
import asyncio
import html
import json
import logging
import math
import operator
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

HTTP_TIMEOUT_S = 12

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _urlopen(req: urllib.request.Request):
    """Open a URL, retrying once without cert verification on TLS errors.

    Development machines and captive proxies often present self-signed certs;
    for research-oriented page fetches we degrade to unverified TLS rather
    than failing the whole pipeline.
    """
    try:
        return urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S)
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, ssl.SSLCertVerificationError):
            logger.warning(
                "TLS verification failed for %s — retrying without verification",
                req.full_url,
            )
            return urllib.request.urlopen(
                req,
                timeout=HTTP_TIMEOUT_S,
                context=ssl._create_unverified_context(),
            )
        raise


#  ---------- Tool implementations (blocking, wrapped in to_thread) ---------- 

# Practical read ceiling for page fetches. Results are intentionally NOT
# further truncated so the dashboard gets the full text. Stays under Actae's
# 1 MiB per-event payload limit (actae/src/events/handler.rs ::MAX_EVENT_SIZE).
MAX_FETCH_BYTES = 900_000

_MAX_SEARCH_RESULTS = 8 

_HTML_SEARCH_URL = "https://html.duckduckgo.com/html/"

_RESULT_LINK_RE = re.compile(
    r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S
)
_SNIPPET_RE = re.compile(r'class="result__snippet"[^>]*>(.*?)</a>', re.S)
_ANCHOR_URL_RE = re.compile(r"uddg=([^&]+)")


def _resolve_ddg_url(href: str) -> str:
    """Resolve DDG's redirect links (//duckduckgo.com/l/?uddg=<url>) to the target."""
    if "uddg=" in href:
        m = _ANCHOR_URL_RE.search(href)
        if m:
            return urllib.parse.unquote(m.group(1))
    return urllib.parse.urljoin("https://html.duckduckgo.com", href)


def _web_search_duckduckgo(query: str) -> List[Dict[str, Any]]:
    """Search the web by scraping DuckDuckGo's HTML search results (no key needed).

    Returns an empty list when DDG serves its anti-bot page or the query has
    no results; callers should fall back to another engine.
    """
    url = _HTML_SEARCH_URL + "?" + urllib.parse.urlencode({"q": query})
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with _urlopen(req) as resp:
            page = resp.read().decode("utf-8", "replace")
    except Exception:
        return []

    results: List[Dict[str, Any]] = []
    for href, title in _RESULT_LINK_RE.findall(page):
        snippet_match = _SNIPPET_RE.search(page, page.index(href))
        snippet = re.sub(r"<[^>]+>", "", snippet_match.group(1)) if snippet_match else ""
        results.append({
            "source": _resolve_ddg_url(urllib.parse.unquote(href)),
            "title": html.unescape(re.sub(r"<[^>]+>", "", title).strip()),
            "snippet": html.unescape(re.sub(r"\s+", " ", snippet).strip())[:400],
        })
        if len(results) >= _MAX_SEARCH_RESULTS:
            break
    return results


#  ---------- Google News RSS engine (no-key, anti-bot-proof) ----------

_GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"


def _search_google_news(query: str) -> List[Dict[str, Any]]:
    """Search recent news via Google News RSS (no key needed, never captcha'd).

    Returns dated, attributed headlines. The item links are Google redirect
    URLs that fetch_page cannot follow, so results carry title/source/date
    only — enough to ground claims and pick new search terms.
    """
    params = {
        "q": query,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }
    url = _GOOGLE_NEWS_RSS_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with _urlopen(req) as resp:
            xml = resp.read().decode("utf-8", "replace")
    except Exception:
        return []

    results: List[Dict[str, Any]] = []
    for item in re.findall(r"<item>(.*?)</item>", xml, re.S):
        def _extract(tag: str) -> str:
            m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", item, re.S)
            if not m:
                return ""
            return re.sub(r"<!\[CDATA\[|\]\]>", "", m.group(1)).strip()

        title = re.sub(r"<[^>]+>", "", _extract("title")).strip()
        if not title:
            continue
        snippet = re.sub(r"<[^>]+>", " ", _extract("description"))
        results.append({
            "source": html.unescape(_extract("source") or "Google News"),
            "title": html.unescape(title),
            "date": _extract("pubDate"),
            "snippet": html.unescape(re.sub(r"\s+", " ", snippet).strip())[:400],
        })
        if len(results) >= _MAX_SEARCH_RESULTS:
            break
    return results


def _web_search(query: str) -> str:
    """Run all search engines and merge results, keeping the best available.

    DDG (general web, snippets) is tried first; Google News RSS (dated
    headlines, always reachable) fills the remaining slots. Both may fail on
    empty — the query wording is then up to the model to refine.
    """
    results = _web_search_duckduckgo(query)
    seen = {r["source"] for r in results}
    for item in _search_google_news(query):
        if len(results) >= _MAX_SEARCH_RESULTS:
            break
        if item["source"] in seen:
            continue
        seen.add(item["source"])
        results.append(item)

    if not results:
        return f"No results found for '{query}'."
    return json.dumps(results, ensure_ascii=False, indent=2)


def _fetch_page(url: str) -> str:
    """Fetch a URL and strip HTML tags to extract readable text."""
    if not url.startswith(("http://", "https://")):
        return json.dumps({"error": "fetch_page requires an http(s) URL"})
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with _urlopen(req) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            html = resp.read(MAX_FETCH_BYTES).decode(charset, "replace")
    except Exception as exc:
        return json.dumps({"error": f"fetch_page failed: {exc}"})

    text = re.sub(r"<(script|style|noscript)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return "No textual content could be extracted from the page."
    return f"url: {url}\n{text}"


#  Safe arithmetic evaluator (AST-based, no eval) 
_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}
_ALLOWED_FUNCS: Dict[str, Any] = {
    "abs": abs, "max": max, "min": min, "round": round, "pow": pow,
    "sqrt": math.sqrt, "log": math.log, "log10": math.log10,
    "sum": sum, "floor": math.floor, "ceil": math.ceil,
}


def _eval_expr(node: ast.AST) -> Any:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
        return _BIN_OPS[type(node.op)](_eval_expr(node.left), _eval_expr(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPS:
        return _UNARY_OPS[type(node.op)](_eval_expr(node.operand))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        fn = _ALLOWED_FUNCS.get(node.func.id)
        if fn is None:
            raise ValueError(f"Function '{node.func.id}' is not allowed")
        return fn(*[_eval_expr(a) for a in node.args])
    raise ValueError(f"Unsupported expression element: {type(node).__name__}")


def _calculate(expression: str) -> str:
    """Safely evaluate a numeric expression (whitelisted AST nodes only)."""
    try:
        tree = ast.parse(expression.strip(), mode="eval")
        value = _eval_expr(tree.body)
        return json.dumps({"expression": expression, "result": value})
    except Exception as exc:
        return json.dumps({"error": f"invalid expression: {exc}"})


#  ---------- Public API ---------- 

TOOLDEF_RESEARCH: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                "Search the web for up-to-date information on a topic. "
                "Returns retrieved snippets with sources. Use before writing "
                "findings so claims are grounded in current facts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query, e.g. 'AI agent adoption 2026 enterprise'",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_page",
            "description": (
                "Fetch a web page by URL (http/https) and extract its readable "
                "text. Use to follow up on a specific source returned by web_search."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "format": "uri",
                        "description": "The http(s) URL to fetch",
                    }
                },
                "required": ["url"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculate",
            "description": "Evaluate a numeric expression (arithmetic, sqrt, pow, etc.).",
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "string",
                        "description": "e.g. '4 * (1 + 5)' or 'sqrt(144) + log10(1000)'",
                    }
                },
                "required": ["expression"],
            },
        },
    },
]

_HANDLERS: Dict[str, Any] = {
    "web_search": _web_search,
    "fetch_page": _fetch_page,
    "calculate": _calculate,
}


async def run_tool(name: str, arguments: Dict[str, Any]) -> str:
    """Execute a tool by name and return a bounded string result.

    Unknown tools and tool errors are returned as JSON error objects rather
    than raised, so the LLM can decide how to proceed.
    """
    t0 = time.time()
    handler = _HANDLERS.get(name)
    if handler is None:
        return json.dumps({"error": f"unknown tool '{name}'"})

    # Arguments come from the model and may be malformed — validate.
    if name == "web_search":
        query = arguments.get("query", "")
        if not query:
            return json.dumps({"error": "web_search requires a 'query' argument"})
        payload = lambda: handler(query)  # noqa: E731
    elif name == "fetch_page":
        url = arguments.get("url", "")
        if not url:
            return json.dumps({"error": "fetch_page requires a 'url' argument"})
        payload = lambda: handler(url)  # noqa: E731
    elif name == "calculate":
        expr = arguments.get("expression", "")
        if not expr:
            return json.dumps({"error": "calculate requires an 'expression' argument"})
        payload = lambda: handler(expr)  # noqa: E731
    else:
        return json.dumps({"error": f"unknown tool '{name}'"})

    result = await asyncio.to_thread(payload)
    logger.info(
        "Tool %s executed in %.0fms -> %d chars",
        name, (time.time() - t0) * 1000, len(result),
    )
    return result