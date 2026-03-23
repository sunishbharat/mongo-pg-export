# Jira Issue Pipeline

A two-stage data pipeline that extracts Jira issue data from a local MongoDB database and loads it into PostgreSQL for analysis.

## Pipeline

```
MongoDB (JiraReposAnon) → CSV files → PostgreSQL (jira_project)
```

1. **`mongo_conv_csv.py`** — Queries all MongoDB collections for the top 1000 issues with 3+ issue links, flattens them, and exports to CSV.
2. **`load_csv_psgresSql.py`** — Loads the CSVs into PostgreSQL, creating indexed `issues` and `ticket_links` tables.

## Prerequisites

- Python >= 3.12
- [uv](https://github.com/astral-sh/uv)
- MongoDB running on `localhost:27017` with the `JiraReposAnon` database populated
- PostgreSQL running on `localhost:5432` with a `jira_project` database created

```sql
CREATE DATABASE jira_project;
```

## Usage

```bash
# Install dependencies
uv sync

# Stage 1: Export from MongoDB to CSV
uv run python mongo_conv_csv.py

# Stage 2: Load CSVs into PostgreSQL
uv run python load_csv_psgresSql.py
```

## Output

| Table / File | Description |
|---|---|
| `jira_issues_poc.csv` | One row per Jira issue (key, summary, type, status, project, timestamps, story points, link count) |
| `jira_links_poc.csv` | One row per directed issue link (from_key, to_key, link_type, direction) |
| `issues` (PostgreSQL) | Same as above, PII columns excluded |
| `ticket_links` (PostgreSQL) | Same as above, with indexes on from/to keys and link type |
