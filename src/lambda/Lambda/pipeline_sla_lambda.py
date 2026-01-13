import json
import boto3
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

s3 = boto3.client("s3")
sns = boto3.client("sns")

RAW_BUCKET = "de-sla-raw-sirisha-01"
RESULTS_BUCKET = "de-sla-results-sirisha-01"
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

ET = ZoneInfo("America/New_York")

SLA_RULES = {
    "orders": {
        "type": "daily",
        "expected_hour": 9,
        "late_min": 60,
        "critical_min": 240
    },
    "payments": {
        "type": "hourly",
        "expected_within_min": 15,
        "late_min": 30,
        "critical_min": 120
    },
    "products": {
        "type": "weekly",
        "expected_weekday": 0,
        "expected_hour": 10,
        "late_min": 360,
        "critical_min": 1440
    }
}

def latest_s3_object(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    latest = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not latest or obj["LastModified"] > latest["LastModified"]:
                latest = obj

    if not latest:
        return None, None

    return latest["LastModified"], latest["Key"]

def lambda_handler(event, context):
    now_utc = datetime.now(timezone.utc)
    results = []
    critical = []

    for source, cfg in SLA_RULES.items():
        prefix = f"staging/{source}/"
        last_time, last_key = latest_s3_object(RAW_BUCKET, prefix)

        if not last_time:
            status = "missing"
            score = 0
            delay = None
        else:
            delay = int((now_utc - last_time).total_seconds() / 60)
            if delay > cfg["critical_min"]:
                status = "critically_late"
                score = 50 if source == "products" else 0
            elif delay > cfg["late_min"]:
                status = "late"
                score = 75
            else:
                status = "on_time"
                score = 100

        record = {
            "source": source,
            "status": status,
            "freshness_score": score,
            "latest_object_key": last_key,
            "check_time_utc": now_utc.isoformat()
        }

        s3.put_object(
            Bucket=RESULTS_BUCKET,
            Key=f"metrics/{source}/latest.json",
            Body=json.dumps(record),
            ContentType="application/json"
        )

        results.append(record)

        if status == "critically_late":
            critical.append(record)

    if critical and SNS_TOPIC_ARN:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="🚨 SLA CRITICAL ALERT",
            Message=json.dumps(critical, indent=2)
        )

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }
