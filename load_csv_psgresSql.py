"""
load_csv_psgresSql.py
---------------------
Loads Jira issue and link data from CSV files into a PostgreSQL database.

Reads:
    jira_issues_poc.csv  — one row per Jira issue
    jira_links_poc.csv   — one row per directed issue link

Writes to PostgreSQL (database: jira_project):
    issues        — Jira issues, excluding PII/noise columns
    ticket_links  — directed issue links with indexes for fast querying

Usage:
    uv run python load_csv_psgresSql.py
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import io

# ── Database connection settings ───────────────────────────────────────
DB_USER     = "postgres"
DB_PASSWORD = "postgres"
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "jira_project"

conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT,
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)

# ── Columns to exclude from all CSVs before loading ───────────────────
EXCLUDE_COLS = {"assignee", "reporter", "priority", "description"}

issues_df = pd.read_csv("jira_issues_poc.csv")
issues_df = issues_df.drop(columns=[c for c in EXCLUDE_COLS if c in issues_df.columns])

links_df  = pd.read_csv("jira_links_poc.csv")
links_df  = links_df.drop(columns=[c for c in EXCLUDE_COLS if c in links_df.columns])

print(f"Issues: {issues_df.shape}")
print(f"Links:  {links_df.shape}")


def df_to_table(cur, df, table_name):
    """Bulk-load a DataFrame into an existing PostgreSQL table using COPY.

    Args:
        cur:        Active psycopg2 cursor.
        df:         DataFrame whose columns match the target table.
        table_name: Name of the destination table.
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cols = ", ".join(df.columns)
    cur.copy_expert(
        f"COPY {table_name} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        buf
    )


def pg_type(dtype):
    """Map a pandas dtype to the corresponding PostgreSQL column type.

    Args:
        dtype: A pandas dtype object.

    Returns:
        A PostgreSQL type string: 'BIGINT', 'DOUBLE PRECISION', or 'TEXT'.
    """
    if pd.api.types.is_integer_dtype(dtype):   return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):     return "DOUBLE PRECISION"
    return "TEXT"


def create_table(cur, df, table_name):
    """Drop (if exists) and recreate a table whose schema is derived from a DataFrame.

    Column names and types are inferred from the DataFrame. Existing data
    in the table is discarded.

    Args:
        cur:        Active psycopg2 cursor.
        df:         DataFrame whose structure defines the table schema.
        table_name: Name of the table to create.
    """
    col_defs = ", ".join(f'"{c}" {pg_type(df[c].dtype)}' for c in df.columns)
    cur.execute(f'DROP TABLE IF EXISTS {table_name}')
    cur.execute(f'CREATE TABLE {table_name} ({col_defs})')


def print_all_columns(conn):
    """Print all tables and their columns in the connected database's public schema.

    Output is grouped by table name, with each column's name and data type.

    Args:
        conn: An open psycopg2 connection.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)
        rows = cur.fetchall()
    current_table = None
    for table, column, dtype in rows:
        if table != current_table:
            print(f"\n[{table}]")
            current_table = table
        print(f"  {column:<30} {dtype}")


# ── Create tables, load data, and add indexes ──────────────────────────
with conn.cursor() as cur:
    create_table(cur, issues_df, "issues")
    df_to_table(cur, issues_df, "issues")

    create_table(cur, links_df, "ticket_links")
    df_to_table(cur, links_df, "ticket_links")

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_issues_key     ON issues(key);
        CREATE INDEX IF NOT EXISTS idx_issues_status  ON issues(status);
        CREATE INDEX IF NOT EXISTS idx_issues_type    ON issues(type);
        CREATE INDEX IF NOT EXISTS idx_issues_project ON issues(project);
        CREATE INDEX IF NOT EXISTS idx_links_from     ON ticket_links(from_key);
        CREATE INDEX IF NOT EXISTS idx_links_to       ON ticket_links(to_key);
        CREATE INDEX IF NOT EXISTS idx_links_type     ON ticket_links(link_type);
    """)
    conn.commit()
    print("Done — tables created with indexes")

    # ── Quick validation ───────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM issues")
    print(f"issues table:       {cur.fetchone()[0]} rows")

    cur.execute("SELECT COUNT(*) FROM ticket_links")
    print(f"ticket_links table: {cur.fetchone()[0]} rows")

    print("\nTop issue types:")
    cur.execute("SELECT type, COUNT(*) AS n FROM issues GROUP BY type ORDER BY n DESC")
    for row in cur.fetchall():
        print(f"  {row[0]:<20} {row[1]}")

    print("\nTop link types:")
    cur.execute("SELECT link_type, COUNT(*) AS n FROM ticket_links GROUP BY link_type ORDER BY n DESC LIMIT 8")
    for row in cur.fetchall():
        print(f"  {row[0]:<30} {row[1]}")

print("\nAll columns in database:")
print_all_columns(conn)

conn.close()
