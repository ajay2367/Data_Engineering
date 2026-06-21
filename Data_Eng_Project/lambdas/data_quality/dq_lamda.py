"""
Lambda: Data Quality Checks
────────────────────────────
Called by Step Functions after the Silver layer is built.
Validates data quality before allowing the Gold aggregation to proceed.

Checks performed:
  1. Row count — is there enough data?
  2. Null percentage — are critical columns populated?
  3. Schema validation — do expected columns exist?
  4. Value range checks — are numeric values reasonable?
  5. Freshness — is the data recent enough?

Required Environment Variables:
    SNS_ALERT_TOPIC_ARN     — SNS topic for alerts (optional)

Recommended Environment Variables:
    ATHENA_WORKGROUP        — Athena workgroup to use (default: primary)
    ATHENA_OUTPUT           — S3 output path for Athena query results (recommended)
    DQ_MIN_ROW_COUNT        — Minimum row count threshold (default: 10)
    DQ_MAX_NULL_PERCENT     — Maximum null percentage for critical columns (default: 5.0)
    DQ_FRESHNESS_HOURS      — How fresh data should be (default: 48)
    DQ_SAMPLE_LIMIT         — Number of rows to sample from Athena (default: 10000)
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any

import boto3
import awswrangler as wr
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ──────────────────────────────────────────────────────────────────────────────
# AWS Clients / Environment
# ──────────────────────────────────────────────────────────────────────────────
sns_client = boto3.client("sns")

SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "").strip()
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary").strip()
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "").strip() or None

# ──────────────────────────────────────────────────────────────────────────────
# Thresholds
# ──────────────────────────────────────────────────────────────────────────────
MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = 50_000_000_000  # 50B — sanity check for view counts
FRESHNESS_HOURS = int(os.environ.get("DQ_FRESHNESS_HOURS", "48"))
SAMPLE_LIMIT = int(os.environ.get("DQ_SAMPLE_LIMIT", "10000"))

# ──────────────────────────────────────────────────────────────────────────────
# Expected critical columns
# ──────────────────────────────────────────────────────────────────────────────
CRITICAL_COLUMNS = {
    "clean_statistics": ["video_id", "title", "channel_title", "views", "region"],
    "clean_reference_data": ["id", "region"],
}


# ──────────────────────────────────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────────────────────────────────
def publish_sns_alert(subject: str, message: Any) -> None:
    """Publish alert to SNS if topic is configured."""
    if not SNS_TOPIC:
        logger.warning("SNS topic not configured; skipping alert.")
        return

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject,
            Message=json.dumps(message, indent=2, default=str)
            if not isinstance(message, str)
            else message,
        )
        logger.info("Published SNS alert successfully.")
    except Exception as e:
        logger.error(f"Failed to publish SNS alert: {e}")


def safe_json(data: Any) -> Any:
    """Convert data to JSON-safe format."""
    return json.loads(json.dumps(data, default=str))


def check_row_count(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """Check that table has minimum number of rows."""
    count = len(df)
    passed = count >= MIN_ROW_COUNT
    return {
        "check": "row_count",
        "table": table_name,
        "value": int(count),
        "threshold": int(MIN_ROW_COUNT),
        "passed": bool(passed),
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
    }


def check_null_percentage(df: pd.DataFrame, table_name: str) -> List[Dict[str, Any]]:
    """Check null percentages for critical columns."""
    results: List[Dict[str, Any]] = []
    cols = CRITICAL_COLUMNS.get(table_name, [])

    for col in cols:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Column '{col}' missing from table",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0.0
        passed = null_pct <= MAX_NULL_PCT
        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(float(null_pct), 2),
            "threshold": float(MAX_NULL_PCT),
            "passed": bool(passed),
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
        })

    return results


def check_schema(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """Check that expected columns exist."""
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(df.columns)
    missing = sorted(list(expected - actual))
    passed = len(missing) == 0

    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": missing,
        "passed": bool(passed),
        "message": (
            f"Missing columns: {missing}"
            if missing
            else "All expected columns present"
        ),
    }


def check_value_ranges(df: pd.DataFrame, table_name: str) -> List[Dict[str, Any]]:
    """Check that numeric values are within reasonable ranges."""
    results: List[Dict[str, Any]] = []

    if table_name != "clean_statistics":
        return results

    if "views" in df.columns:
        views_numeric = pd.to_numeric(df["views"], errors="coerce")
        negative = int((views_numeric < 0).sum())
        extreme = int((views_numeric > MAX_VIEWS).sum())
        passed = negative == 0 and extreme == 0

        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "views",
            "negative_count": negative,
            "extreme_count": extreme,
            "passed": bool(passed),
            "message": f"Views: {negative} negative, {extreme} extreme (>{MAX_VIEWS})",
        })

    return results


def check_freshness(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """Check that data includes recent records."""
    ts_col = None
    if "_processed_at" in df.columns:
        ts_col = "_processed_at"
    elif "_ingestion_timestamp" in df.columns:
        ts_col = "_ingestion_timestamp"

    if ts_col is None:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found — skipping freshness check (backfill/legacy data)",
        }

    try:
        parsed = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
        latest = parsed.max()

        if pd.isna(latest):
            return {
                "check": "freshness",
                "table": table_name,
                "passed": True,
                "message": f"No parseable timestamps found in column '{ts_col}' — skipping",
            }

        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
        passed = latest.to_pydatetime() >= cutoff

        return {
            "check": "freshness",
            "table": table_name,
            "timestamp_column": ts_col,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "passed": bool(passed),
            "message": f"Latest: {latest}, Cutoff: {cutoff}",
        }

    except Exception as e:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": f"Could not parse timestamps: {e} — skipping",
        }


def read_table_sample(database: str, table_name: str) -> pd.DataFrame:
    """
    Read a limited sample from Athena using awswrangler.
    Explicitly sets workgroup and optionally s3_output.
    """
    query = f'SELECT * FROM "{table_name}" LIMIT {SAMPLE_LIMIT}'

    read_kwargs = {
        "sql": query,
        "database": database,
        "ctas_approach": False,
        "workgroup": ATHENA_WORKGROUP,
    }

    if ATHENA_OUTPUT:
        read_kwargs["s3_output"] = ATHENA_OUTPUT

    logger.info(
        f"Executing Athena query for {database}.{table_name} "
        f"using workgroup='{ATHENA_WORKGROUP}', "
        f"s3_output='{ATHENA_OUTPUT}', limit={SAMPLE_LIMIT}"
    )

    df = wr.athena.read_sql_query(**read_kwargs)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Lambda handler
# ──────────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Run data quality checks on Silver layer tables.

    Expected event:
    {
        "layer": "silver",
        "database": "yt_pipeline_silver_dev",
        "tables": ["clean_statistics", "clean_reference_data"]
    }
    """
    logger.info("Received event: %s", json.dumps(event, default=str))

    database = event.get("database", "yt_pipeline_silver_dev")
    tables = event.get("tables", ["clean_statistics"])

    if not isinstance(tables, list) or not tables:
        return {
            "quality_passed": False,
            "checks_passed": 0,
            "checks_total": 1,
            "details": [{
                "check": "input_validation",
                "passed": False,
                "message": "Event field 'tables' must be a non-empty list"
            }]
        }

    all_results: List[Dict[str, Any]] = []
    overall_passed = True

    for table_name in tables:
        logger.info(f"Running DQ checks on {database}.{table_name} ...")

        try:
            df = read_table_sample(database=database, table_name=table_name)
            logger.info(f"Read {len(df)} rows from {database}.{table_name}")
        except Exception as e:
            logger.error(f"Could not read {database}.{table_name}: {e}", exc_info=True)
            all_results.append({
                "check": "read_table",
                "table": table_name,
                "database": database,
                "passed": False,
                "message": str(e),
                "workgroup": ATHENA_WORKGROUP,
                "s3_output": ATHENA_OUTPUT,
            })
            overall_passed = False
            continue

        # Run all checks
        checks: List[Dict[str, Any]] = []
        checks.append(check_row_count(df, table_name))
        checks.extend(check_null_percentage(df, table_name))
        checks.append(check_schema(df, table_name))
        checks.extend(check_value_ranges(df, table_name))
        checks.append(check_freshness(df, table_name))

        for check in checks:
            logger.info(
                "  %s: %s — %s",
                check["check"],
                "PASS" if check["passed"] else "FAIL",
                check["message"],
            )
            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # Summary
    passed_count = sum(1 for r in all_results if r.get("passed") is True)
    total_count = len(all_results)

    summary = {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "details": safe_json(all_results),
        "athena_workgroup": ATHENA_WORKGROUP,
        "athena_output": ATHENA_OUTPUT,
        "database": database,
        "tables": tables,
    }

    logger.info(
        "DQ Summary: %s/%s checks passed. Overall: %s",
        passed_count,
        total_count,
        "PASS" if overall_passed else "FAIL",
    )

    # Publish failures to SNS
    if not overall_passed:
        failed = [r for r in all_results if not r.get("passed", False)]
        publish_sns_alert(
            subject="[YT Pipeline] Data quality checks FAILED",
            message={
                "summary": {
                    "quality_passed": False,
                    "checks_passed": passed_count,
                    "checks_total": total_count,
                    "database": database,
                    "tables": tables,
                    "athena_workgroup": ATHENA_WORKGROUP,
                },
                "failed_checks": failed,
            },
        )

    return summary