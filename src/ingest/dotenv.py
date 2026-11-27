"""Minimal ``.env`` loader for the command line tools (no extra dependency).

Looks for ``.env`` in the current directory and its parents, so the commands
work from the repository root as well as from ``src``. Variables already set
in the environment win over the file. Supported syntax: ``KEY=value`` lines,
optional ``export`` prefix, ``#`` comments, single or double quotes.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from pathlib import Path


def find_dotenv(start: Path | None = None, name: str = ".env") -> Path | None:
    here = (start or Path.cwd()).resolve()
    for directory in (here, *here.parents):
        candidate = directory / name
        if candidate.is_file():
            return candidate
    return None


def parse_dotenv(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        if key:
            values[key] = value
    return values


def load_dotenv(
    path: Path | None = None, environ: MutableMapping[str, str] | None = None
) -> Path | None:
    """Load ``path`` (or the nearest ``.env``) into ``environ``; return the file used.

    Empty values in the file do not override anything.
    """
    environ = os.environ if environ is None else environ
    path = path or find_dotenv()
    if path is None:
        return None
    for key, value in parse_dotenv(Path(path).read_text()).items():
        if value and not environ.get(key):
            environ[key] = value
    return path
