#!/usr/bin/env python3
"""
Run the sample SQL queries from the project brief and save results to outputs/query_results.md

Usage:
  python run_queries.py --db production.db
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


QUERIES = {
    "Total records loaded (sensor_readings)": "SELECT COUNT(*) AS cnt FROM sensor_readings;",
    "Latest hourly summary for Line_1": """
        SELECT * FROM hourly_summary
        WHERE line_id = 'Line_1'
        ORDER BY hour DESC
        LIMIT 10;
    """,
    "High defect rate hours (> 5%)": """
        SELECT hour, line_id, machine_id, defect_rate
        FROM hourly_summary
        WHERE defect_rate > 5.0
        ORDER BY defect_rate DESC
        LIMIT 20;
    """,
    "Data quality distribution": """
        SELECT data_quality, COUNT(*) AS count
        FROM sensor_readings
        GROUP BY data_quality;
    """,
    "Join sensor data with quality checks (sample)": """
        SELECT
          s.timestamp,
          s.machine_id,
          s.temperature,
          q.result AS quality_result
        FROM sensor_readings s
        LEFT JOIN quality_checks q
          ON s.machine_id = q.machine_id
         AND s.timestamp = q.timestamp
        LIMIT 10;
    """,
    "Average temperature by machine": """
        SELECT machine_id,
               AVG(temperature) AS avg_temp,
               MIN(temperature) AS min_temp,
               MAX(temperature) AS max_temp
        FROM sensor_readings
        GROUP BY machine_id
        ORDER BY machine_id;
    """,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run sample SQL queries for the ETL project")
    p.add_argument("--db", default="production.db", help="Path to SQLite db")
    p.add_argument("--out", default="outputs/query_results.md", help="Markdown output file")
    return p.parse_args()


def _format_rows(cursor: sqlite3.Cursor, rows: list[tuple]) -> str:
    cols = [d[0] for d in cursor.description] if cursor.description else []
    if not cols:
        return "_(no columns)_\n"

    # Make a simple Markdown table
    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for r in rows:
        lines.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}. Run run_etl.py first.")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    md_parts = [f"# Query Results\n\nDatabase: `{db_path}`\n"]

    for title, sql in QUERIES.items():
        md_parts.append(f"## {title}\n")
        cur.execute(sql)
        rows = cur.fetchall()
        md_parts.append("```sql\n" + sql.strip() + "\n```\n")
        md_parts.append(_format_rows(cur, rows))
        md_parts.append("\n")

    conn.close()

    out_path.write_text("\n".join(md_parts), encoding="utf-8")
    print(f"✅ Wrote query results to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
