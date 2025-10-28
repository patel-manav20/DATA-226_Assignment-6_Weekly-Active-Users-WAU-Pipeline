# DATA-226 Assignment - 6: Weekly Active Users (WAU) Pipeline

## Overview
Airflow ETL/ELT pipeline to calculate Weekly Active Users using Snowflake and Preset.

## Files
- `wau_etl.py` - Loads data from S3 to Snowflake RAW schema
- `session_summary_elt.py` - Transforms and joins data in ANALYTICS schema

## Architecture

S3 → ETL DAG → RAW Tables → ELT DAG → SESSION_SUMMARY → Preset

## Tables
**RAW Schema:**
- `USER_SESSION_CHANNEL` (USERID, SESSIONID, CHANNEL)
- `SESSION_TIMESTAMP` (SESSIONID, TS)

**ANALYTICS Schema:**
- `SESSION_SUMMARY` (deduplicated & joined)

## Setup
1. Set Airflow Variables:
   - `snowflake_database`: `USER_DB_HYENA`
   - `snowflake_conn_id`: `snowflake_conn`
2. Configure Snowflake connection in Airflow UI
3. Run `wau_etl` DAG (auto-triggers `session_summary_elt`)

## Preset Setup & Visualization

1. **Connect Snowflake**: preset.io → Settings → Database Connections → Add Snowflake (Database: `USER_DB_HYENA`, Schema: `ANALYTICS`)

2. **Import Dataset**: Data → Datasets → Add `SESSION_SUMMARY` table

3. **Create WAU Chart**: 
   - Charts → New Time-series Chart
   - Time: `TS` (Weekly)
   - Metric: `COUNT_DISTINCT(USERID)` → Rename to `WAU`
   - Save

**Output:** Weekly active users trend line chart
