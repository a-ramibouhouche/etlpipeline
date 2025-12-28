#!/usr/bin/env python3
"""
Run the full ETL pipeline and generate production.db

Usage:
  python run_etl.py \
    --sensor-csv data/smart_manufacturing_data.csv \
    --quality-csv data/industrial_fault_detection_data_1000.csv \
    --db production.db \
    --days 7
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.etl import (
    extract_sensor_data,
    extract_quality_data,
    clean_sensor_data,
    standardize_sensor_data,
    transform_quality_data,
    join_sensor_quality,
    calculate_hourly_summary,
)
from src.db import connect, create_schema, load_sensor_readings, load_quality_checks, load_hourly_summary, table_counts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ETL Pipeline: CSV -> Transform -> SQLite")
    p.add_argument("--sensor-csv", required=True, help="Path to sensor CSV file")
    p.add_argument("--quality-csv", required=True, help="Path to quality CSV file")
    p.add_argument("--db", default="production.db", help="SQLite database output path")
    p.add_argument("--days", type=int, default=7, help="Keep last N days of sensor data (relative to dataset max timestamp)")
    p.add_argument("--outputs", default="outputs", help="Folder to write intermediate outputs (optional)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    outputs_dir = Path(args.outputs)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    print("=== EXTRACT ===")
    sensor_raw = extract_sensor_data(args.sensor_csv, days=args.days)
    quality_raw = extract_quality_data(args.quality_csv)

    print(f"Sensor extracted rows:  {len(sensor_raw):,}")
    print(f"Quality extracted rows: {len(quality_raw):,}")

    if sensor_raw.empty:
        raise SystemExit("Sensor dataset is empty after extraction. Check the file/path and timestamp parsing.")

    print("\n=== TRANSFORM ===")
    sensor_clean = clean_sensor_data(sensor_raw)
    sensor_std = standardize_sensor_data(sensor_clean)

    quality_std = transform_quality_data(quality_raw, sensor_std) if not quality_raw.empty else pd.DataFrame(
        columns=["timestamp", "line_id", "machine_id", "result", "defect_type"]
    )

    joined = join_sensor_quality(sensor_std, quality_std)
    hourly = calculate_hourly_summary(joined)

    # Optional: save intermediate CSVs for debugging/report screenshots
    sensor_std.to_csv(outputs_dir / "sensor_readings_clean.csv", index=False)
    quality_std.to_csv(outputs_dir / "quality_checks_clean.csv", index=False)
    hourly.to_csv(outputs_dir / "hourly_summary.csv", index=False)

    print(f"Standardized sensor rows: {len(sensor_std):,}")
    print(f"Standardized quality rows: {len(quality_std):,}")
    print(f"Hourly summary rows:       {len(hourly):,}")

    print("\n=== LOAD ===")
    conn = connect(args.db)
    try:
        create_schema(conn)

        inserted_sensor = load_sensor_readings(conn, sensor_std)
        inserted_quality = load_quality_checks(conn, quality_std) if not quality_std.empty else 0
        inserted_hourly = load_hourly_summary(conn, hourly)

        counts = table_counts(conn)
        print(f"Inserted sensor_readings (new): {inserted_sensor:,}")
        print(f"Inserted quality_checks (new):  {inserted_quality:,}")
        print(f"Inserted hourly_summary (delta): {inserted_hourly:,} (note: REPLACE may not change count)")

        print("\nDatabase table counts:")
        for t, c in counts.items():
            print(f"  - {t}: {c:,}")

        print(f"\n✅ Done. SQLite DB written to: {Path(args.db).resolve()}")
        print(f"Intermediate CSVs saved in:    {outputs_dir.resolve()}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
