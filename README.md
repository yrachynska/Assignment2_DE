# Support Call Enrichment Pipeline
### Project Overview
1. **MySQL Database:** Contains general information about calls (who called, time, status).

2. **JSON Files:** Contain details like call duration and short descriptions.

Bonus tasks:
Data Quality Checks: duration ≥ 0, employee exists, unique call_id

Observability: every step is logging row counts

Backfill-friendly DAG: catchup, start_date

Retry & alert strategy: retries


### Pipeline Structure
The DAG consists of three main tasks:

detect_new_calls: Finds new call_ids in MySQL since the last run.

load_telephony_details: Reads JSON files from the local folder for those specific IDs.

transform_and_load_duckdb: Joins the data, checks for quality, and saves it to DuckDB.

