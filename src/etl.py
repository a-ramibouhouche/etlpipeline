"""
ETL Pipeline Project: Production Line Data Integration

Implements:
- Extract sensor + quality CSV files (with error handling)
- Transform: cleaning, standardization, join, hourly aggregation
- Load into a normalized SQLite database (production.db)

This code is written to match the project brief (see ETL Pipeline Project PDF).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import re
from typing import Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Helpers
# -----------------------------

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase column names, remove spaces/special chars, replace with underscores.

    Example: "Machine ID" -> "machine_id"
    """
    df = df.copy()
    new_cols = []
    for c in df.columns:
        c2 = str(c).strip().lower()
        c2 = re.sub(r"[^\w]+", "_", c2)  # anything not alnum/_ becomes underscore
        c2 = re.sub(r"_+", "_", c2).strip("_")
        new_cols.append(c2)
    df.columns = new_cols
    return df


def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_machine_str(x) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip().lower()
    if not s:
        return None
    if s.startswith("machine_"):
        return s
    # numeric -> machine_{n}
    try:
        return f"machine_{int(float(s))}"
    except Exception:
        return s


def _derive_line_id(machine_id: Optional[str]) -> Optional[str]:
    """
    Derive a line_id from a machine_id like 'machine_17' -> 'Line_2'
    (10 machines per line: 1-10 Line_1, 11-20 Line_2, ...)
    """
    if machine_id is None or pd.isna(machine_id):
        return None
    m = re.findall(r"\d+", str(machine_id))
    if not m:
        return None
    num = int(m[0])
    line_num = (num - 1) // 10 + 1
    return f"Line_{line_num}"


def _make_record_id(timestamp: pd.Timestamp, line_id: str, machine_id: str) -> str:
    """
    Deterministic record_id so rerunning the pipeline is idempotent.
    """
    key = f"{timestamp.isoformat()}|{line_id}|{machine_id}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


# -----------------------------
# Phase 1: EXTRACT
# -----------------------------

def extract_sensor_data(sensor_csv_path: str | Path, days: int = 7) -> pd.DataFrame:
    """
    Reads the sensor CSV file.
    - Handles missing files (prints error, doesn't crash)
    - Filters data for the last `days` days relative to the dataset's MAX timestamp
      (robust for static datasets where "today" might not be present)
    - Returns a DataFrame
    """
    sensor_csv_path = Path(sensor_csv_path)

    if not sensor_csv_path.exists():
        print(f"[ERROR] Sensor file not found: {sensor_csv_path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(sensor_csv_path)
    except Exception as e:
        print(f"[ERROR] Failed reading sensor CSV '{sensor_csv_path}': {e}")
        return pd.DataFrame()

    df = standardize_columns(df)

    if "timestamp" not in df.columns:
        raise ValueError("Sensor CSV must include a 'timestamp' column.")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    if df.empty:
        return df

    max_ts = df["timestamp"].max()
    cutoff = max_ts - pd.Timedelta(days=days)
    df = df[df["timestamp"] >= cutoff].copy()

    return df


def extract_quality_data(quality_csv_path: str | Path) -> pd.DataFrame:
    """
    Reads the quality inspection CSV file.
    - Extracts only completed inspections (if a status column exists)
    - Handles encoding issues (try UTF-8, then ISO-8859-1)
    - Returns a DataFrame
    """
    quality_csv_path = Path(quality_csv_path)

    if not quality_csv_path.exists():
        print(f"[ERROR] Quality file not found: {quality_csv_path}")
        return pd.DataFrame()

    encodings = ["utf-8", "ISO-8859-1"]
    df = None
    last_exc = None

    for enc in encodings:
        try:
            df = pd.read_csv(quality_csv_path, encoding=enc)
            break
        except UnicodeDecodeError as e:
            last_exc = e
            continue

    if df is None:
        print(f"[ERROR] Could not read quality CSV '{quality_csv_path}' with {encodings}. Last error: {last_exc}")
        return pd.DataFrame()

    df = standardize_columns(df)

    # If status column exists, keep only completed inspections
    status_col = _first_existing_col(df, ["status", "inspection_status", "qc_status"])
    if status_col:
        def is_completed(v) -> bool:
            if pd.isna(v):
                return False
            if isinstance(v, (int, float, np.integer, np.floating)):
                return int(v) == 1
            s = str(v).strip().lower()
            return s in {"completed", "complete", "done", "finished"}

        df = df[df[status_col].apply(is_completed)].copy()

    return df


# -----------------------------
# Phase 2: TRANSFORM
# -----------------------------

def clean_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans sensor readings:
    - Replaces error codes (-999, -1, NULL) with NaN
    - Validates ranges:
        Temperature: 0 to 150 C
        Pressure: 0 to 10 bar
        Vibration: 0 to 100 mm/s
    - Replaces invalid values with previous valid reading (forward fill)
      (forward fill is applied per machine_id when available)
    - Adds data_quality flag: good, estimated, invalid
    """
    df = df.copy()
    df = standardize_columns(df)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Map optional power column from energy_consumption (common in IoT datasets)
    if "power" not in df.columns and "energy_consumption" in df.columns:
        df["power"] = df["energy_consumption"]

    sensor_cols = [c for c in ["temperature", "pressure", "vibration", "power"] if c in df.columns]

    # Replace common error codes / null strings
    error_values = [-999, -1, "-999", "-1", "NULL", "null", "NaN", "nan", ""]
    for c in sensor_cols:
        df[c] = df[c].replace(error_values, np.nan)
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Validate ranges
    ranges: dict[str, Tuple[float, float]] = {
        "temperature": (0.0, 150.0),
        "pressure": (0.0, 10.0),
        "vibration": (0.0, 100.0),
    }

    invalid_mask = pd.DataFrame(False, index=df.index, columns=sensor_cols)
    for col, (lo, hi) in ranges.items():
        if col in df.columns:
            bad = (df[col] < lo) | (df[col] > hi)
            invalid_mask[col] = bad
            df.loc[bad, col] = np.nan

    missing_before = df[sensor_cols].isna() | invalid_mask

    # Forward fill per machine_id (recommended for industrial streams)
    sort_cols = ["timestamp"]
    if "machine_id" in df.columns:
        sort_cols = ["machine_id", "timestamp"]

    df = df.sort_values(sort_cols)

    if "machine_id" in df.columns:
        df[sensor_cols] = df.groupby("machine_id")[sensor_cols].ffill()
    else:
        df[sensor_cols] = df[sensor_cols].ffill()

    still_missing = df[sensor_cols].isna().any(axis=1)
    was_estimated = missing_before.any(axis=1) & ~still_missing

    df["data_quality"] = np.where(
        still_missing,
        "invalid",
        np.where(was_estimated, "estimated", "good"),
    )

    return df


def standardize_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize sensor data to match the DB schema:
    - timestamps to datetime
    - machine_id to lowercase string format: machine_{n}
    - create line_id
    - create deterministic record_id
    - keep only schema columns
    """
    df = df.copy()
    df = standardize_columns(df)

    if "timestamp" not in df.columns:
        raise ValueError("Sensor data must contain 'timestamp'.")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    if "machine_id" not in df.columns:
        raise ValueError("Sensor data must contain 'machine_id'.")

    df["machine_id"] = df["machine_id"].apply(_to_machine_str)
    df["line_id"] = df["machine_id"].apply(_derive_line_id)

    # Ensure numeric columns exist (fill with NaN if not)
    for col in ["temperature", "pressure", "vibration", "power"]:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "data_quality" not in df.columns:
        df["data_quality"] = "good"

    df["record_id"] = [
        _make_record_id(ts, line, mach)
        for ts, line, mach in zip(df["timestamp"], df["line_id"], df["machine_id"])
    ]

    out_cols = [
        "record_id",
        "timestamp",
        "line_id",
        "machine_id",
        "temperature",
        "pressure",
        "vibration",
        "power",
        "data_quality",
    ]
    return df[out_cols].copy()


def transform_quality_data(df: pd.DataFrame, sensor_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform quality data to match DB schema:
    - standardize column names
    - parse timestamps
    - infer result ('pass'/'fail') and defect_type
    - if timestamps don't overlap sensor_df timestamps, auto-align by shifting the whole
      quality timeline to end at sensor_df.max(timestamp). This is mainly to keep the demo
      join meaningful when sample datasets come from different time windows.
    - if machine_id missing, infer machine_id + line_id by matching sensor_df on timestamp
      (works well when sensor_df has 1 reading per timestamp)
    """
    df = df.copy()
    df = standardize_columns(df)

    # Find timestamp column
    if "timestamp" not in df.columns:
        # common alternatives
        ts = _first_existing_col(df, ["time", "datetime", "date_time"])
        if ts:
            df = df.rename(columns={ts: "timestamp"})
        else:
            # fallback: any column containing 'time'
            time_cols = [c for c in df.columns if "time" in c]
            if not time_cols:
                raise ValueError("Quality data must include a timestamp/time column.")
            df = df.rename(columns={time_cols[0]: "timestamp"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # Infer result
    if "result" in df.columns:
        df["result"] = df["result"].astype(str).str.lower().str.strip()
        df["result"] = df["result"].replace({"passed": "pass", "failed": "fail", "ok": "pass", "ng": "fail"})
    else:
        label_col = _first_existing_col(df, ["fault_label", "defect_flag", "defect", "anomaly_flag"])
        if label_col:
            df[label_col] = pd.to_numeric(df[label_col], errors="coerce").fillna(0).astype(int)
            df["result"] = np.where(df[label_col] == 1, "fail", "pass")
        else:
            df["result"] = "pass"

    if "defect_type" not in df.columns:
        df["defect_type"] = np.where(df["result"].eq("fail"), "fault", None)

    # Align quality timestamps if no overlap with sensor timestamps
    if sensor_df is not None and not sensor_df.empty:
        s_min = sensor_df["timestamp"].min()
        s_max = sensor_df["timestamp"].max()
        has_overlap = df["timestamp"].between(s_min, s_max).any()
        if not has_overlap:
            shift = s_max - df["timestamp"].max()
            df["timestamp"] = df["timestamp"] + shift

    # Add machine_id/line_id if missing (map from sensor_df on timestamp)
    if "machine_id" not in df.columns or df["machine_id"].isna().all():
        if sensor_df is not None and not sensor_df.empty:
            # Expecting sensor_df already standardized (machine_id, line_id present)
            lookup = sensor_df[["timestamp", "machine_id", "line_id"]].drop_duplicates(subset=["timestamp"])
            df = df.merge(lookup, on="timestamp", how="left")
        else:
            df["machine_id"] = None
            df["line_id"] = None
    else:
        df["machine_id"] = df["machine_id"].apply(_to_machine_str)

    if "line_id" not in df.columns or df["line_id"].isna().all():
        df["line_id"] = df["machine_id"].apply(_derive_line_id)

    out = df[["timestamp", "line_id", "machine_id", "result", "defect_type"]].copy()
    out = out.dropna(subset=["timestamp", "machine_id", "line_id"])

    return out


def join_sensor_quality(sensor_df: pd.DataFrame, quality_df: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join sensor readings with quality checks on timestamp + line_id + machine_id.
    Adds quality_status: passed / failed / not_checked
    """
    sensor_df = sensor_df.copy()
    quality_df = quality_df.copy()

    joined = sensor_df.merge(
        quality_df,
        on=["timestamp", "line_id", "machine_id"],
        how="left",
        suffixes=("", "_q"),
    )

    joined["quality_status"] = np.where(
        joined["result"].isna(),
        "not_checked",
        np.where(joined["result"].eq("pass"), "passed", "failed"),
    )
    return joined


def calculate_hourly_summary(joined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create hourly aggregates:
    - group by hour, line_id, machine_id
    - avg/min/max temperature
    - avg pressure, avg vibration
    - total checks, defect_count, defect_rate
    """
    df = joined_df.copy()
    df["hour"] = df["timestamp"].dt.floor("h")

    for col in ["temperature", "pressure", "vibration"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    summary = (
        df.groupby(["hour", "line_id", "machine_id"], dropna=False)
          .agg(
              avg_temperature=("temperature", "mean"),
              min_temperature=("temperature", "min"),
              max_temperature=("temperature", "max"),
              avg_pressure=("pressure", "mean"),
              avg_vibration=("vibration", "mean"),
              total_checks=("result", lambda x: x.notna().sum()),
              defect_count=("result", lambda x: (x == "fail").sum()),
          )
          .reset_index()
    )

    summary["defect_rate"] = np.where(
        summary["total_checks"] > 0,
        summary["defect_count"] / summary["total_checks"] * 100.0,
        0.0,
    )

    return summary
