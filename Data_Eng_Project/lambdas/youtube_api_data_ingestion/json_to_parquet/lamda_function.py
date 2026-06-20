import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd

# ── Logging ─────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── CONFIG (UPDATED FOR YOUR PIPELINE) ───────────────────
BRONZE_BUCKET = "data-eng-project-yt-data-bronze-layer"
SILVER_BUCKET = "data-eng-project-yt-data-silver-layer"

GLUE_DB = os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
GLUE_TABLE = os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "arn:aws:sns:ap-south-2:926193199974:yt-datapipeline-alerts-dev")

# Silver output path
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/reference_data/"

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


# ── READ JSON FROM BRONZE ────────────────────────────────
def read_json_from_s3(bucket: str, key: str) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


# ── VALIDATION ──────────────────────────────────────────
def validate_category_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("Empty DataFrame — no records found")

    required_cols = {"id", "snippet.title"}
    missing = required_cols - set(df.columns)

    if missing:
        logger.warning(f"Missing expected columns: {missing}")

    # Deduplication
    before = len(df)
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="last")
    after = len(df)

    if before != after:
        logger.info(f"Removed {before - after} duplicate records")

    return df


# ── SNS ALERT ───────────────────────────────────────────
def send_alert(subject: str, message: str):
    if SNS_TOPIC:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message
        )


# ── MAIN LAMBDA ─────────────────────────────────────────
def lambda_handler(event, context):

    records = event.get("Records", [])

    processed = []
    errors = []

    for record in records:
        key = "unknown"

        try:
            # ── Extract S3 Event Info ────────────────────
            s3_info = record["s3"]
            bucket = s3_info["bucket"]["name"]
            key = unquote_plus(s3_info["object"]["key"])

            logger.info(f"Processing: s3://{bucket}/{key}")

            # ── Read JSON ───────────────────────────────
            raw_data = read_json_from_s3(bucket, key)

            # Handle YouTube category JSON
            if "items" in raw_data and isinstance(raw_data["items"], list):
                df = pd.json_normalize(raw_data["items"])
            else:
                df = pd.json_normalize(raw_data)

            logger.info(f"Raw shape: {df.shape}")

            # ── Validate ───────────────────────────────
            df = validate_category_data(df)

            # ── Metadata Columns ───────────────────────
            df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            df["_source_file"] = key

            # Extract region from path
            region = "unknown"
            for part in key.split("/"):
                if part.startswith("region="):
                    region = part.split("=")[1]
                    break

            df["region"] = region

            logger.info(f"Clean shape: {df.shape}, region: {region}")

            # ── Write to Silver (PARQUET) ──────────────
            wr.s3.to_parquet(
                df=df,
                path=SILVER_PATH,
                dataset=True,
                database=GLUE_DB,
                table=GLUE_TABLE,
                partition_cols=["region"],
                mode="overwrite_partitions",
                schema_evolution=True,
            )

            logger.info(f"Written to Silver: {SILVER_PATH}")

            processed.append({
                "key": key,
                "region": region,
                "rows": len(df)
            })

        except Exception as e:
            logger.error(f"Error processing {key}: {str(e)}", exc_info=True)

            errors.append({
                "key": key,
                "error": str(e)
            })

    # ── ALERTING ────────────────────────────────────────
    if errors:
        send_alert(
            subject="[YT Pipeline] Bronze → Silver failed",
            message=json.dumps(errors, indent=2),
        )

    return {
        "statusCode": 200,
        "processed": processed,
        "errors": errors,
    }