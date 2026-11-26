# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Task registry for the OSS repo agent.

Each task pins a **real public repository** and a small, self-contained bug we
reintroduce deterministically (so a run is reproducible): the mutation is the
inverse of a real, correct implementation, the wrong fix is a plausible but
incorrect attempt, and the correct fix restores the real implementation. The
repo's own test file is the oracle.

Data safety: these are public, permissively licensed repositories (no
customer/work data). Point the agent at any other public repo by adding an
entry here.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

# itsdangerous `int_to_bytes` must strip *leading* zero bytes. Reintroduce the
# bug (drop the strip), and give the agent a plausible-but-wrong attempt
# (`.rstrip`, which strips trailing zeros and therefore does nothing here)
# before the correct fix (`.lstrip`). The repo's own test_encoding.py is the
# oracle (test_int_bytes covers 0 and 192).
_ITD_STRIP = 'return _int_to_bytes(num).lstrip(b"\\x00")'
_ITD_NOSTRIP = "return _int_to_bytes(num)"
_ITD_RSTRIP = 'return _int_to_bytes(num).rstrip(b"\\x00")'


Task = Dict[str, object]

TASKS: Dict[str, Task] = {
    "itsdangerous-int-to-bytes": {
        "name": "itsdangerous-int-to-bytes",
        "repo_url": "https://github.com/pallets/itsdangerous",
        "license": "BSD-3-Clause",
        "target_file": "src/itsdangerous/encoding.py",
        "test_path": "tests/test_itsdangerous/test_encoding.py",
        "pythonpath": "src",
        "locate_needle": "def int_to_bytes",
        "issue": (
            "Serialized integers carry leading NUL bytes, so round-tripped "
            "signatures don't match. `int_to_bytes` should drop leading zeros."
        ),
        # (present, replacement): correct -> buggy at setup.
        "mutation": (_ITD_STRIP, _ITD_NOSTRIP),
        # (present, replacement): the baseline's first attempt (still wrong).
        "wrong_fix": (_ITD_NOSTRIP, _ITD_RSTRIP),
        # (present, replacement): applied on the fork (correct).
        "correct_fix": (_ITD_RSTRIP, _ITD_STRIP),
        # Canonical correct line, for verification.
        "correct_line": _ITD_STRIP,
    },
}

DEFAULT_TASK = "itsdangerous-int-to-bytes"


def get_task(name: str) -> Task:
    if name not in TASKS:
        raise KeyError(f"unknown task {name!r}; known: {', '.join(TASKS)}")
    return TASKS[name]


def task_names() -> List[str]:
    return sorted(TASKS)


def anchors_are_consistent(task: Task) -> bool:
    """The mutation / wrong-fix / correct-fix anchors must chain exactly."""
    mut_old, mut_new = task["mutation"]  # type: ignore[misc]
    wrong_old, wrong_new = task["wrong_fix"]  # type: ignore[misc]
    correct_old, correct_new = task["correct_fix"]  # type: ignore[misc]
    return mut_new == wrong_old and wrong_new == correct_old and correct_new == mut_old
