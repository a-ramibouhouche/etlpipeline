"""
SQLite database helpers for the ETL pipeline.

Creates the schema required by the project and loads data using idempotent inserts.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Iterable, Sequence, Optional

import pandas as pd


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sensor_readings (
  record_id TEXT PRIMARY KEY,
  timestamp DATETIME,
  line_id TEXT,
  machine_id TEXT,
  temperature REAL,
  pressure REAL,
  vibration REAL,
  power REAL,
  data_quality TEXT
);

CREATE TABLE IF NOT EXISTS quality_checks (
  check_id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp DATETIME,
  line_id TEXT,
  machine_id TEXT,
  result TEXT, -- 'pass' or 'fail'
  defect_type TEXT
);

CREATE TABLE IF NOT EXISTS hourly_summary (
  summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
  hour DATETIME,
  line_id TEXT,
  machine_id TEXT,
  avg_temperature REAL,
  min_temperature REAL,
  max_temperature REAL,
  avg_pressure REAL,
  avg_vibration REAL,
  total_checks INTEGER,
  defect_count INTEGER,
  defect_rate REAL
);
"""

INDEX_SQL = """
-- Prevent duplicates on reruns (incremental load pattern)
CREATE UNIQUE INDEX IF NOT EXISTS ux_quality
  ON quality_checks(timestamp, line_id, machine_id, result, defect_type);

CREATE UNIQUE INDEX IF NOT EXISTS ux_hourly
  ON hourly_summary(hour, line_id, machine_id);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(db_path))


def create_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(SCHEMA_SQL)
    cur.executescript(INDEX_SQL)
    conn.commit()


def _chunked(seq: Sequence[tuple], chunk_size: int) -> Iterable[Sequence[tuple]]:
    for i in range(0, len(seq), chunk_size):
        yield seq[i : i + chunk_size]


def load_sensor_readings(conn: sqlite3.Connection, df: pd.DataFrame, chunk_size: int = 5000) -> int:
    cols = ["record_id", "timestamp", "line_id", "machine_id", "temperature", "pressure", "vibration", "power", "data_quality"]
    insert_sql = f"INSERT OR IGNORE INTO sensor_readings ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))});"

    rows: list[tuple] = []
    for r in df[cols].itertuples(index=False, name=None):
        r = list(r)
        # timestamp -> ISO string
        ts = r[1]
        r[1] = ts.isoformat(sep=" ") if pd.notna(ts) else None
        rows.append(tuple(r))

    cur = conn.cursor()
    before = cur.execute("SELECT COUNT(*) FROM sensor_readings;").fetchone()[0]

    for batch in _chunked(rows, chunk_size):
        cur.executemany(insert_sql, batch)

    conn.commit()
    after = cur.execute("SELECT COUNT(*) FROM sensor_readings;").fetchone()[0]
    return int(after - before)


def load_quality_checks(conn: sqlite3.Connection, df: pd.DataFrame, chunk_size: int = 5000) -> int:
    cols = ["timestamp", "line_id", "machine_id", "result", "defect_type"]
    insert_sql = f"INSERT OR IGNORE INTO quality_checks ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))});"

    rows: list[tuple] = []
    for r in df[cols].itertuples(index=False, name=None):
        r = list(r)
        ts = r[0]
        r[0] = ts.isoformat(sep=" ") if pd.notna(ts) else None
        rows.append(tuple(r))

    cur = conn.cursor()
    before = cur.execute("SELECT COUNT(*) FROM quality_checks;").fetchone()[0]

    for batch in _chunked(rows, chunk_size):
        cur.executemany(insert_sql, batch)

    conn.commit()
    after = cur.execute("SELECT COUNT(*) FROM quality_checks;").fetchone()[0]
    return int(after - before)


def load_hourly_summary(conn: sqlite3.Connection, df: pd.DataFrame, chunk_size: int = 5000) -> int:
    """
    Uses INSERT OR REPLACE so that each (hour, line_id, machine_id) row is updated on reruns.
    """
    cols = [
        "hour",
        "line_id",
        "machine_id",
        "avg_temperature",
        "min_temperature",
        "max_temperature",
        "avg_pressure",
        "avg_vibration",
        "total_checks",
        "defect_count",
        "defect_rate",
    ]
    insert_sql = f"INSERT OR REPLACE INTO hourly_summary ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))});"

    rows: list[tuple] = []
    for r in df[cols].itertuples(index=False, name=None):
        r = list(r)
        hr = r[0]
        r[0] = hr.isoformat(sep=" ") if pd.notna(hr) else None
        rows.append(tuple(r))

    cur = conn.cursor()
    before = cur.execute("SELECT COUNT(*) FROM hourly_summary;").fetchone()[0]

    for batch in _chunked(rows, chunk_size):
        cur.executemany(insert_sql, batch)

    conn.commit()
    after = cur.execute("SELECT COUNT(*) FROM hourly_summary;").fetchone()[0]
    # could be smaller/unchanged; return approximate inserted/updated as delta won't capture replaces well
    return int(after - before)


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    cur = conn.cursor()
    tables = ["sensor_readings", "quality_checks", "hourly_summary"]
    out = {}
    for t in tables:
        out[t] = int(cur.execute(f"SELECT COUNT(*) FROM {t};").fetchone()[0])
    return out
