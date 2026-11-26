# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Offline unit tests for examples/research_agent (no server, no network).

Pins the demo's pure logic against the **real** committed arXiv snapshot: the
Atom parser, the snapshot chunker, the token ledger + report arithmetic, JSON
parsing, and the deterministic scripted model (which is extractive — it returns
real claims from the real abstracts and catches its one fabricated citation).
The live fork / durability behaviour is the demo itself. Skipped without
langgraph.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("langgraph")
pytest.importorskip("langchain_core")

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "examples" / "research_agent"))
sys.path.insert(0, str(_ROOT / "examples"))

import pipeline  # noqa: E402
import source  # noqa: E402
from fork_token_report import format_token_report  # noqa: E402


class _FakeActae:
    """Minimal client satisfying ActaeCheckpointSaver's constructor checks."""

    async def record(self, *a, **k): ...
    async def save_state(self, *a, **k): ...
    async def latest_state(self, *a, **k): ...
    async def list_states(self, *a, **k): ...
    async def get_state(self, *a, **k): ...


_ATOM = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/2401.12345v2</id>
    <title>A  Test   Paper</title>
    <summary>First sentence here. Second sentence.</summary>
    <published>2024-01-20T00:00:00Z</published>
    <author><name>Ada Lovelace</name></author>
    <author><name>Alan Turing</name></author>
    <category term="cs.AI"/>
  </entry>
</feed>
"""


def test_parse_atom_normalizes_fields():
    papers = source.parse_atom(_ATOM)
    assert len(papers) == 1
    paper = papers[0]
    assert paper["id"] == "2401.12345"          # version stripped
    assert paper["title"] == "A Test Paper"      # whitespace collapsed
    assert paper["authors"] == ["Ada Lovelace", "Alan Turing"]
    assert paper["url"].endswith("2401.12345")
    assert paper["categories"] == ["cs.AI"]


def test_snapshot_is_committed_and_chunks_are_addressable():
    snapshot = source.load_snapshot(source.DEFAULT_TOPIC)
    assert snapshot is not None, "the committed arXiv snapshot is missing"
    assert snapshot["source"] == "arxiv"
    chunks = source.chunks(snapshot)
    assert len(chunks) == len(snapshot["papers"]) >= 10
    ids = [c["id"] for c in chunks]
    assert len(ids) == len(set(ids)), "chunk ids must be unique"
    assert all(c["text"].strip() for c in chunks)
    assert source.word_count(snapshot) > 1000


def test_token_ledger_and_report():
    base = pipeline.TokenLedger()
    base.record("filter_source", 1000, 50, 100)
    base.record("extract_notes", 400, 100, 80)
    base.record("write_report", 300, 100, 60)
    fork = pipeline.TokenLedger()
    fork.record("write_report", 300, 100, 60)

    assert base.total_tokens() == 1950
    assert fork.total_tokens() == 400
    assert base.count("filter_source") == 1 and fork.count("filter_source") == 0

    text = format_token_report(base, fork, ["filter_source", "extract_notes", "write_report"])
    assert "tokens saved on the fork run" in text
    assert "1,550" in text  # 1950 - 400


def test_extract_json_handles_fences_and_noise():
    assert pipeline._extract_json('```json\n[1, 2, 3]\n```') == [1, 2, 3]
    assert pipeline._extract_json('Sure: ["a", "b"] done') == ["a", "b"]
    with pytest.raises(ValueError):
        pipeline._extract_json("no json here")


def _prompts(snapshot):
    chunks = source.chunks(snapshot)[:4]
    rendered = pipeline._render_chunks(chunks, 600)
    model = pipeline.ScriptedModel()
    filt_user = f"Question: q\n\nPapers:\n{rendered}\n\nReply with the JSON object of relevant arXiv ids."
    filt = asyncio.run(model.complete(system="", user=filt_user, step="filter_source", json_mode=True))
    ids = pipeline._extract_json(filt.text)["ids"]
    return model, chunks, rendered, ids


def test_scripted_model_is_extractive_and_catches_its_fabricated_citation():
    snapshot = source.load_snapshot(source.DEFAULT_TOPIC)
    model, chunks, rendered, ids = _prompts(snapshot)
    assert ids == [c["id"] for c in chunks][: model.SELECT_K]

    extract_user = f"Question: q\n\nPapers:\n{rendered}\n\nReply with the JSON object of findings."
    notes = pipeline._extract_json(
        asyncio.run(model.complete(system="", user=extract_user, step="extract_notes", json_mode=True)).text
    )["findings"]
    # Real, extractive findings cite real ids …
    assert any(c["id"] in n for n in notes for c in chunks)
    # … plus one fabricated citation the checker must catch.
    assert any(model.FABRICATED in n for n in notes)

    fc_user = f"Findings:\n{json.dumps(notes, indent=2)}\n\nPapers:\n{rendered}\n\nReply with the JSON object of verdicts."
    verdicts = pipeline._extract_json(
        asyncio.run(model.complete(system="", user=fc_user, step="fact_check", json_mode=True)).text
    )["verdicts"]
    fabricated = next(v for v in verdicts if model.FABRICATED in v["claim"])
    assert fabricated["verdict"] == "unsupported"
    assert any(v["verdict"] == "supported" for v in verdicts), "real findings must be supported"

    # The corrected report notes the removal; the unverified one does not.
    checked_user = (
        f"Question: q\n\nFindings:\n" + "\n".join(f"- {n}" for n in notes)
        + "\n\nVerification results:\n" + json.dumps(verdicts, indent=2)
    )
    plain_user = f"Question: q\n\nFindings:\n" + "\n".join(f"- {n}" for n in notes)
    checked = asyncio.run(model.complete(system="", user=checked_user, step="write_report")).text
    plain = asyncio.run(model.complete(system="", user=plain_user, step="write_report")).text
    assert "Corrections:" in checked
    assert "Corrections:" not in plain
    assert "retrieved arXiv papers" in checked


def test_first_json_is_balanced_inside_strings_and_ignores_later_values():
    # A bracket inside a string must not end the scan early …
    assert pipeline._first_json('prefix {"a": "x ] y", "b": [1, 2]} suffix') == {"a": "x ] y", "b": [1, 2]}
    # … and a second JSON value later in the text must not over-match.
    assert pipeline._first_json('[1, 2] then {"late": true}') == [1, 2]
    with pytest.raises(ValueError):
        pipeline._first_json("no json at all")


def test_items_rejects_a_bad_shape():
    assert pipeline._items({"ids": ["a"]}, "ids") == ["a"]
    assert pipeline._items(["a"], "ids") == ["a"]
    with pytest.raises(pipeline.PipelineError):
        pipeline._items({"ids": "nope"}, "ids")


def test_parse_atom_rejects_dtd_and_entities():
    hostile = b'<?xml version="1.0"?><!DOCTYPE feed [<!ENTITY x "y">]><feed><entry/></feed>'
    with pytest.raises(ValueError):
        source.parse_atom(hostile)


def test_graph_shape_with_and_without_fact_check():
    from actae_client.adapters.langgraph import ActaeCheckpointSaver

    saver = ActaeCheckpointSaver(_FakeActae(), channel="research")
    base = pipeline.build_graph(pipeline.ScriptedModel(), saver, pipeline.TokenLedger(), with_fact_check=False)
    enhanced = pipeline.build_graph(pipeline.ScriptedModel(), saver, pipeline.TokenLedger(), with_fact_check=True)

    base_nodes = set(base.get_graph().nodes)
    enh_nodes = set(enhanced.get_graph().nodes)
    assert {"load_source", "filter_source", "extract_notes", "write_report"} <= base_nodes
    assert "fact_check" not in base_nodes
    assert "fact_check" in enh_nodes


def test_checkpoint_after_covers_all_nodes():
    assert set(pipeline._NODE_OUTPUT) == {
        "load_source",
        "filter_source",
        "extract_notes",
        "fact_check",
        "write_report",
    }


@pytest.mark.skipif(
    not os.environ.get("ACTAE_ARXIV_LIVE"), reason="set ACTAE_ARXIV_LIVE=1 to hit arXiv"
)
def test_live_arxiv_fetch():
    """Opt-in: fetch a few papers from the real arXiv API."""
    snapshot = source.fetch_papers("large language model agents", max_results=5)
    papers = snapshot["papers"]
    assert len(papers) >= 1
    for paper in papers:
        assert paper["id"] and paper["title"] and paper["abstract"]
        assert paper["url"].startswith("https://arxiv.org/abs/")


def test_report_styles_and_scripted_rendering():
    assert set(pipeline.REPORT_STYLES) >= {"brief", "exec", "skeptical"}
    assert pipeline._style_of("Write an executive one-pager: ...") == "exec"
    assert pipeline._style_of("Write a skeptical review: ...") == "skeptical"
    assert pipeline._style_of("Write a concise research brief: ...") == "brief"

    snapshot = source.load_snapshot(source.DEFAULT_TOPIC)
    model, _, rendered, _ = _prompts(snapshot)
    user = "Question: q\n\nFindings:\n- A finding (2401.12345)."

    async def render(system: str) -> str:
        return (await model.complete(system=system, user=user, step="write_report")).text

    brief = asyncio.run(render(pipeline.REPORT_STYLES["brief"]))
    exec_ = asyncio.run(render(pipeline.REPORT_STYLES["exec"]))
    skep = asyncio.run(render(pipeline.REPORT_STYLES["skeptical"]))
    assert brief.startswith("Research brief")
    assert exec_.startswith("Executive one-pager")
    assert "Recommendation:" in exec_
    assert skep.startswith("Skeptical review")
    assert "caveat:" in skep
