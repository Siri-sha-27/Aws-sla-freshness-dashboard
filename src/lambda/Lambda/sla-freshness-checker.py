import json
import os
import boto3
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# AWS clients
s3 = boto3.client("s3")
sns = boto3.client("sns")

# Buckets
RAW_BUCKET = "de-sla-raw-sirisha-01"
RESULTS_BUCKET = "de-sla-results-sirisha-01"

# SNS
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")

# Timezone
ET = ZoneInfo("America/New_York")

# SLA CONFIG
SLA = {
    "orders": {
        "type": "daily",
        "expected_hour_local": 9,
        "expected_minute_local": 0,
        "late_threshold_min": 60,
        "critical_threshold_min": 240,
        "required": True
    },
    "payments": {
        "type": "hourly",
        "expected_within_min": 15,
        "late_threshold_min": 30,
        "critical_threshold_min": 120,
        "required": True
    },
    "products": {
        "type": "weekly",
        "expected_weekday": 0,  # Monday
        "expected_hour_local": 10,
        "expected_minute_local": 0,
        "late_threshold_min": 360,
        "critical_threshold_min": 1440,
        "required": False
    }
}

SOURCES = ["orders", "payments", "products"]


# ---------------- HELPERS ----------------

def utc_now():
    return datetime.now(timezone.utc)


def list_latest_object(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    latest = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest is None or obj["LastModified"] > latest["LastModified"]:
                latest = obj

    if latest is None:
        return None, None

    return latest["LastModified"], latest["Key"]


def expected_time_for_source(source, check_time_utc):
    cfg = SLA[source]

    # HOURLY
    if cfg["type"] == "hourly":
        return check_time_utc.replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(minutes=cfg["expected_within_min"])

    # DAILY
    if cfg["type"] == "daily":
        check_local = check_time_utc.astimezone(ET)
        expected_local = check_local.replace(
            hour=cfg["expected_hour_local"],
            minute=cfg["expected_minute_local"],
            second=0,
            microsecond=0
        )
        if check_local < expected_local:
            expected_local -= timedelta(days=1)
        return expected_local.astimezone(timezone.utc)

    # WEEKLY
    if cfg["type"] == "weekly":
        check_local = check_time_utc.astimezone(ET)
        expected_local = check_local.replace(
            hour=cfg["expected_hour_local"],
            minute=cfg["expected_minute_local"],
            second=0,
            microsecond=0
        )
        days_back = (expected_local.weekday() - cfg["expected_weekday"]) % 7
        expected_local -= timedelta(days=days_back)
        if check_local < expected_local:
            expected_local -= timedelta(days=7)
        return expected_local.astimezone(timezone.utc)

    return check_time_utc


def compute_status_delay_score(source, latest_time_utc, check_time_utc):
    cfg = SLA[source]

    if latest_time_utc is None:
        return "missing", None, (0 if cfg["required"] else 50), None

    expected_utc = expected_time_for_source(source, check_time_utc)

    # MAIN DELAY (expected - actual)
    delay_min = max(
        0,
        int((expected_utc - latest_time_utc).total_seconds() // 60)
    )

    # WEEKLY STALENESS GUARD
    if cfg["type"] == "weekly":
        age_min = int((check_time_utc - latest_time_utc).total_seconds() // 60)
        if age_min > 7 * 24 * 60:
            delay_min = age_min

    # STATUS
    if delay_min == 0:
        status = "on_time"
    elif delay_min <= cfg["late_threshold_min"]:
        status = "slightly_late"
    elif delay_min > cfg["critical_threshold_min"]:
        status = "critically_late"
    else:
        status = "slightly_late"

    # SCORE
    if status == "on_time":
        score = 100
    else:
        penalty = min(100, delay_min // 10)
        score = max(0, 100 - penalty)

    if not cfg["required"]:
        score = max(score, 50)

    return status, delay_min, score, expected_utc


def put_result(source, result, check_time_utc):
    key = (
        f"metrics/source={source}/"
        f"year={check_time_utc:%Y}/"
        f"month={check_time_utc:%m}/"
        f"day={check_time_utc:%d}/"
        f"hour={check_time_utc:%H}/sla_result.json"
    )
    s3.put_object(
        Bucket=RESULTS_BUCKET,
        Key=key,
        Body=json.dumps(result),
        ContentType="application/json"
    )
    return key


def send_sns_alert(critical_results):
    if not SNS_TOPIC_ARN:
        return

    lines = ["CRITICAL SLA BREACH DETECTED"]
    for r in critical_results:
        lines.append(
            f"{r['source']} | delay={r['delay_minutes']} min | "
            f"score={r['freshness_score']} | file={r['latest_object_key']}"
        )

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="SLA ALERT: critically_late",
        Message="\n".join(lines)
    )


# ---------------- LAMBDA ----------------

def lambda_handler(event, context):
    check_time = utc_now()
    results = []

    for source in SOURCES:
        latest_time, latest_key = list_latest_object(
            RAW_BUCKET, f"staging/{source}/"
        )

        status, delay, score, expected = compute_status_delay_score(
            source, latest_time, check_time
        )

        result = {
            "source": source,
            "check_time_utc": check_time.isoformat(),
            "check_time_et": check_time.astimezone(ET).isoformat(),
            "expected_by_utc": expected.isoformat() if expected else None,
            "expected_by_et": expected.astimezone(ET).isoformat() if expected else None,
            "latest_object_time_utc": latest_time.isoformat() if latest_time else None,
            "latest_object_time_et": latest_time.astimezone(ET).isoformat() if latest_time else None,
            "latest_object_key": latest_key,
            "status": status,
            "delay_minutes": delay,
            "freshness_score": score
        }

        result["written_to"] = put_result(source, result, check_time)
        results.append(result)

    critical = [r for r in results if r["status"] == "critically_late"]
    if critical:
        send_sns_alert(critical)

    return {
        "statusCode": 200,
        "body": json.dumps(results, indent=2)
    }
