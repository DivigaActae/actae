# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Real repository tools for the repo-maintenance agent demo.

No server, no ledger here on purpose: these are the actual filesystem/test
operations. ``agent.py`` wraps the *side-effecting* ones in Actae's idempotent
execution ledger so a crash cannot fire them twice.

The workspace is a throwaway copy of ``fixture_repo/`` so runs never mutate the
committed fixture.
"""

from __future__ import annotations

import difflib
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

BENCH = Path(__file__).resolve().parent
FIXTURE = BENCH / "fixture_repo"


def seed_workspace(dest: Path) -> Path:
    """Copy the fixture repo into ``dest`` (fresh, reproducible workspace)."""
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(FIXTURE, dest)
    return dest


class RepoWorkspace:
    """Read/search/edit/test against a copied repository."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._original: Dict[str, str] = {}

    def read_file(self, rel: str) -> str:
        text = (self.root / rel).read_text(encoding="utf-8")
        self._original.setdefault(rel, text)
        return text

    def search(self, needle: str, glob: str = "*.py") -> List[Dict[str, object]]:
        hits: List[Dict[str, object]] = []
        for path in sorted(self.root.rglob(glob)):
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if needle in line:
                    hits.append(
                        {"file": str(path.relative_to(self.root)), "line": lineno, "text": line.strip()}
                    )
        return hits

    def edit_file(self, rel: str, old: str, new: str) -> bool:
        """Replace the first occurrence of ``old`` with ``new``. Returns changed."""
        path = self.root / rel
        text = path.read_text(encoding="utf-8")
        self._original.setdefault(rel, text)
        if old not in text:
            return False
        path.write_text(text.replace(old, new, 1), encoding="utf-8")
        return True

    def run_tests(self) -> Tuple[int, str]:
        """Run the repo's test suite in a subprocess. Returns (returncode, output)."""
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "tests", "-q"],
            cwd=str(self.root),
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = (completed.stdout + completed.stderr).strip()
        return completed.returncode, output[-2000:]

    def file_hash(self, rel: str) -> str:
        return hashlib.sha256((self.root / rel).read_bytes()).hexdigest()

    def diff(self, rel: str) -> str:
        original = self._original.get(rel)
        if original is None:
            return ""
        current = (self.root / rel).read_text(encoding="utf-8")
        return "".join(
            difflib.unified_diff(
                original.splitlines(keepends=True),
                current.splitlines(keepends=True),
                fromfile=f"a/{rel}",
                tofile=f"b/{rel}",
            )
        )


# The known-correct fix, used by the deterministic policy and by verification.
CORRECT_MEDIAN_FIX = (
    """    if not values:
        return 0.0
    ordered = sorted(values)
    # BUG: for an even-length sequence the median is the mean of the two
    # middle values, not the upper-middle element.
    return ordered[len(ordered) // 2]""",
    """    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2""",
)

# A plausible-but-wrong edit used to script a failing baseline (it touches the
# already-correct ``average`` instead of the buggy ``median``).
WRONG_EDIT = (
    "    return sum(values) / len(values)",
    "    return sum(values) / float(len(values))",
)


def expected_fixed_hash(reference_root: Path) -> str:
    """SHA-256 of ``calc/stats.py`` after applying the correct fix."""
    text = (reference_root / "calc" / "stats.py").read_text(encoding="utf-8")
    old, new = CORRECT_MEDIAN_FIX
    assert old in text, "fixture changed: correct-fix anchor no longer present"
    fixed = text.replace(old, new, 1)
    return hashlib.sha256(fixed.encode("utf-8")).hexdigest()
