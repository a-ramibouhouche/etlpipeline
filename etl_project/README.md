# ETL Pipeline Project — Production Line Data Integration

This repo implements the project brief: **Extract → Transform → Load** two CSV datasets into a centralized **SQLite** database for querying daily operations.

## Folder structure

```
etl_pipeline_project/
  src/
    etl.py        # extract + transform logic
    db.py         # sqlite schema + loaders
  run_etl.py      # main runner: builds production.db
  run_queries.py  # runs sample SQL queries and writes outputs/query_results.md
  outputs/        # generated CSVs + query results (created when you run)
  data/           # put your CSV files here (NOT committed)
```

## Requirements

- Python 3.10+ recommended
- pip packages in `requirements.txt`

## Setup

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate

pip install -r requirements.txt
```

## Put the data in `data/`

Place your CSVs like:

- `data/smart_manufacturing_data.csv`
- `data/industrial_fault_detection_data_1000.csv`

> If your filenames differ, just pass the correct paths to `run_etl.py`.

## Run the ETL pipeline

```bash
python run_etl.py \
  --sensor-csv data/smart_manufacturing_data.csv \
  --quality-csv data/industrial_fault_detection_data_1000.csv \
  --db production.db \
  --days 7
```

Outputs:
- `production.db` (SQLite DB with 3 tables)
- `outputs/sensor_readings_clean.csv`
- `outputs/quality_checks_clean.csv`
- `outputs/hourly_summary.csv`

```bash
python run_queries.py --db production.db
```

This writes: `outputs/query_results.md`