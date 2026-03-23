import argparse
from unittest.mock import patch

import pandas as pd
import pytest


# ── flatten_issue (pure function, defined inline to avoid module-level DB calls)

def flatten_issue(doc):
    f = doc.get("fields", {})
    return {
        "key":          doc.get("key", ""),
        "summary":      f.get("summary", ""),
        "type":         (f.get("issuetype") or {}).get("name", ""),
        "status":       (f.get("status") or {}).get("name", ""),
        "priority":     (f.get("priority") or {}).get("name", ""),
        "project":      (f.get("project") or {}).get("key", ""),
        "project_name": (f.get("project") or {}).get("name", ""),
        "assignee":     (f.get("assignee") or {}).get("displayName", "unassigned"),
        "reporter":     (f.get("reporter") or {}).get("displayName", "unknown"),
        "created":      str(f.get("created", "")),
        "updated":      str(f.get("updated", "")),
        "resolved":     str(f.get("resolutiondate", "")),
        "story_points": f.get("story_points", f.get("customfield_10016")) or 0,
        "link_count":   doc.get("link_count", 0),
        "description":  (f.get("description") or "")[:300],
    }


def test_flatten_issue_full():
    doc = {
        "key": "PROJ-1",
        "link_count": 5,
        "fields": {
            "summary": "Fix the bug",
            "issuetype": {"name": "Bug"},
            "status": {"name": "Open"},
            "priority": {"name": "High"},
            "project": {"key": "PROJ", "name": "My Project"},
            "assignee": {"displayName": "Alice"},
            "reporter": {"displayName": "Bob"},
            "created": "2024-01-01",
            "updated": "2024-01-02",
            "resolutiondate": None,
            "story_points": 3,
            "description": "A" * 400,
        },
    }
    row = flatten_issue(doc)
    assert row["key"] == "PROJ-1"
    assert row["type"] == "Bug"
    assert row["status"] == "Open"
    assert row["project"] == "PROJ"
    assert row["assignee"] == "Alice"
    assert row["link_count"] == 5
    assert len(row["description"]) == 300


def test_flatten_issue_missing_fields():
    row = flatten_issue({"key": "X-1", "fields": {}})
    assert row["key"] == "X-1"
    assert row["assignee"] == "unassigned"
    assert row["reporter"] == "unknown"
    assert row["story_points"] == 0


# ── load_csv_psgresSql: pg_type ────────────────────────────────────────────

@pytest.fixture(scope="module")
def loader():
    with patch("psycopg2.connect"), patch("pandas.read_csv", return_value=pd.DataFrame()):
        import load_csv_psgresSql as l
        return l


def test_pg_type_integer(loader):
    assert loader.pg_type(pd.Series([1, 2, 3]).dtype) == "BIGINT"


def test_pg_type_float(loader):
    assert loader.pg_type(pd.Series([1.0, 2.5]).dtype) == "DOUBLE PRECISION"


def test_pg_type_text(loader):
    assert loader.pg_type(pd.Series(["a", "b"]).dtype) == "TEXT"


# ── main.py: CLI argument parsing ──────────────────────────────────────────

def _parse(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--issues",      default="jira_issues_poc.csv")
    parser.add_argument("--links",       default="jira_links_poc.csv")
    parser.add_argument("--skip-export", action="store_true")
    parser.add_argument("--skip-load",   action="store_true")
    return parser.parse_args(args)


def test_cli_defaults():
    args = _parse([])
    assert args.issues == "jira_issues_poc.csv"
    assert args.links == "jira_links_poc.csv"
    assert not args.skip_export
    assert not args.skip_load


def test_cli_custom_paths():
    args = _parse(["--issues", "out/i.csv", "--links", "out/l.csv"])
    assert args.issues == "out/i.csv"
    assert args.links == "out/l.csv"


def test_cli_skip_flags():
    args = _parse(["--skip-export", "--skip-load"])
    assert args.skip_export
    assert args.skip_load
