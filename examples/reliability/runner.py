#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Reliability evidence runner — prove Actae's failure semantics, publicly.

Runs a small matrix of real scenarios against a live Actae instance and writes
a machine-readable JSON report plus a Markdown summary:

    CRASH         a worker is killed with os._exit(137) after a side effect;
                  resume must recover state and replay (not re-fire) the effect.
    FORK          a fork replays an inherited effect (auto), refuses a new
                  irreversible one (block), and stores an intervention.
    COORDINATION  consumer groups never double-deliver an active claim, acked
                  work is not redelivered, and a lapsed lease is reclaimed.

Usage:
    cd actae && cargo run
    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-... \
        python3 examples/reliability/runner.py \
            --out examples/reliability-reports

Exit code 0 when every check passes. The committed report is real evidence,
not an illustration.
"""

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from report import Report, render_markdown, Suite  # noqa: E402
from scenarios import SUITES  # noqa: E402

from actae_client import ActaeClient  # noqa: E402


async def _server_version(client: ActaeClient) -> str:
    try:
        data = await client._request("GET", "/healthz")
        if isinstance(data, dict):
            return str(data.get("version", "")) or "unknown"
    except Exception:
        pass
    return "unknown"


async def run(args: argparse.Namespace) -> int:
    selected = [name.strip() for name in args.suites.split(",") if name.strip()]
    unknown = [name for name in selected if name not in SUITES]
    if unknown:
        print(f"unknown suites: {', '.join(unknown)} (available: {', '.join(SUITES)})")
        return 2

    run_id = args.run_id or f"reliability-{uuid.uuid4().hex[:8]}"
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    client = ActaeClient(api_key=args.api_key, endpoint=args.url)
    await client.connect()
    try:
        version = await _server_version(client)
        suites: list[Suite] = []
        for name in selected:
            print(f"running suite: {name}")
            suite = await SUITES[name](client, run_id, out_dir)
            suites.append(suite)
        report = Report(
            run_id=run_id,
            actae_url=args.url,
            suites=suites,
            server_version=version,
        )
    finally:
        await client.disconnect()

    payload = report.to_dict()
    json_path = out_dir / f"{run_id}.json"
    md_path = out_dir / f"{run_id}.md"
    body = json.dumps(payload, indent=2) + "\n"
    markdown = render_markdown(report)
    json_path.write_text(body, encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    # A stable pointer for docs/tooling ("latest" like the fidelity reports).
    (out_dir / "latest.json").write_text(body, encoding="utf-8")
    (out_dir / "latest.md").write_text(markdown, encoding="utf-8")

    print()
    for suite in report.suites:
        marker = "PASS" if suite.ok else "FAIL"
        print(f"  [{marker}] {suite.name}: {suite.passed}/{suite.total}")
        for check in suite.checks:
            if not check.ok:
                print(f"         ✗ {check.name} — {check.detail}")
        if suite.error:
            print(f"         ! {suite.error}")
    print()
    print(f"{report.passed}/{report.total} checks — {report.verdict}")
    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    return report.exit_code()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.getenv("ACTAE_URL") or os.getenv("ACTAE_ENDPOINT") or "http://localhost:8002",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ACTAE_API_KEY") or "sk-dev-0000000000000000000000",
    )
    parser.add_argument(
        "--suites",
        default="crash,fork,coordination,stale_worker,atomicity,long_tool,session_fence",
        help="comma-separated subset of: crash,fork,coordination,stale_worker,atomicity,long_tool,session_fence",
    )
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[1] / "reliability-reports"),
        help="output directory for the JSON + Markdown reports",
    )
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
