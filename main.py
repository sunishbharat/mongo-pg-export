import argparse
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Jira ETL pipeline: MongoDB → CSV → PostgreSQL"
    )
    parser.add_argument("--issues",  default="jira_issues_poc.csv", help="Output path for issues CSV (default: jira_issues_poc.csv)")
    parser.add_argument("--links",   default="jira_links_poc.csv",  help="Output path for links CSV (default: jira_links_poc.csv)")
    parser.add_argument("--skip-export", action="store_true", help="Skip Stage 1 (MongoDB → CSV) and use existing CSV files")
    parser.add_argument("--skip-load",   action="store_true", help="Skip Stage 2 (CSV → PostgreSQL)")
    args = parser.parse_args()

    if not args.skip_export:
        print("=== Stage 1: Exporting from MongoDB to CSV ===")
        result = subprocess.run(
            [sys.executable, "mongo_conv_csv.py"],
            env={**__import__("os").environ, "ISSUES_OUT": args.issues, "LINKS_OUT": args.links}
        )
        if result.returncode != 0:
            print("Stage 1 failed. Aborting.")
            sys.exit(result.returncode)

    if not args.skip_load:
        print("\n=== Stage 2: Loading CSV into PostgreSQL ===")
        result = subprocess.run(
            [sys.executable, "load_csv_psgresSql.py"],
            env={**__import__("os").environ, "ISSUES_IN": args.issues, "LINKS_IN": args.links}
        )
        if result.returncode != 0:
            print("Stage 2 failed.")
            sys.exit(result.returncode)

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
