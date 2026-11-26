# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Real public-repo tools for the OSS repo agent.

Clones a real public GitHub repository, then exposes filesystem and test tools
over a throwaway copy. There is deliberately no ledger here: these are the real
operations. ``agent.py`` wraps the side-effecting ones (edits, test runs, the
report) in Actae's idempotent execution ledger so a crash or a fork cannot fire
them twice.
"""

from __future__ import annotations

import difflib
import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

_SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", ".mypy_cache", ".pytest_cache"}


def clone_repo(url: str, dest: Path, *, refresh: bool = False, depth: int = 1) -> Path:
    """Clone ``url`` into ``dest`` (cached). Reuses an existing checkout unless
    ``refresh`` is set. Returns the checkout path."""
    dest = Path(dest)
    if dest.exists() and not refresh:
        return dest
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        ["git", "clone", "--depth", str(depth), url, str(dest)],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"git clone failed for {url}:\n{(proc.stderr or proc.stdout).strip()}"
        )
    return dest


def prepare_workspace(cache_repo: Path, dest: Path) -> Path:
    """Copy the cached checkout into a fresh, isolated workspace."""
    dest = Path(dest)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(cache_repo, dest, ignore=shutil.ignore_patterns(*_SKIP_DIRS))
    return dest


def apply_mutation(workspace: Path, target_file: str, mutation: Tuple[str, str]) -> bool:
    """Introduce the task's bug by replacing ``mutation[0]`` with ``mutation[1]``."""
    path = Path(workspace) / target_file
    text = path.read_text(encoding="utf-8")
    old, new = mutation
    if old not in text:
        raise RuntimeError(f"mutation anchor not found in {target_file}: {old!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    return True


class RepoWorkspace:
    """Read / search / edit / test against a copied public repository."""

    def __init__(self, root: Path, *, pythonpath: str | None = None) -> None:
        self.root = Path(root)
        self.pythonpath = pythonpath
        self._original: Dict[str, str] = {}

    def read_file(self, rel: str) -> str:
        text = (self.root / rel).read_text(encoding="utf-8")
        self._original.setdefault(rel, text)
        return text

    def search(self, needle: str, glob: str = "*.py") -> List[Dict[str, object]]:
        hits: List[Dict[str, object]] = []
        for path in sorted(self.root.rglob(glob)):
            if any(part in _SKIP_DIRS for part in path.relative_to(self.root).parts):
                continue
            for lineno, line in enumerate(
                path.read_text(encoding="utf-8", errors="replace").splitlines(), 1
            ):
                if needle in line:
                    hits.append(
                        {
                            "file": str(path.relative_to(self.root)),
                            "line": lineno,
                            "text": line.strip(),
                        }
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

    def run_tests(self, test_path: str, *, timeout: int = 300) -> Tuple[int, str]:
        """Run the repo's real test file. Returns (returncode, tail of output)."""
        env = dict(os.environ)
        if self.pythonpath:
            prefix = str(self.root / self.pythonpath)
            env["PYTHONPATH"] = prefix + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", test_path, "-q"],
            cwd=str(self.root),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
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
