"""Tests for the minimal .env loader."""

from __future__ import annotations

from ingest.dotenv import find_dotenv, load_dotenv, parse_dotenv


def test_parse_dotenv_syntax():
    text = """
# comment
ENV=dev
export METEOFRANCE_API_KEY="abc def"
SINGLE='x=y'
TRAILING=value # comment
EMPTY=
not a line
"""
    assert parse_dotenv(text) == {
        "ENV": "dev",
        "METEOFRANCE_API_KEY": "abc def",
        "SINGLE": "x=y",
        "TRAILING": "value",
        "EMPTY": "",
    }


def test_find_dotenv_in_parent(tmp_path):
    (tmp_path / ".env").write_text("A=1\n")
    deep = tmp_path / "src" / "ingest"
    deep.mkdir(parents=True)
    assert find_dotenv(deep) == tmp_path / ".env"


def test_environment_wins_over_file_and_empty_values_are_ignored(tmp_path):
    p = tmp_path / ".env"
    p.write_text("A=file\nB=file\nC=\nD=file\n")
    env = {"A": "env", "C": "env", "D": ""}
    assert load_dotenv(p, env) == p
    assert env == {"A": "env", "B": "file", "C": "env", "D": "file"}


def test_no_dotenv_is_fine(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    env: dict[str, str] = {}
    assert load_dotenv(None, env) is None
    assert env == {}
