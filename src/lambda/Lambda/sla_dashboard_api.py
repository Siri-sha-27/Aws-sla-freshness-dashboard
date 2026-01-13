import json
import os
import time
import boto3

athena = boto3.client("athena")

ATHENA_DB = os.environ.get("ATHENA_DB", "sla_db")
ATHENA_OUTPUT_S3 = os.environ.get("ATHENA_OUTPUT_S3", "")  # must be s3://bucket/prefix/

def run_athena_query(sql: str) -> str:
    """Start Athena query and return QueryExecutionId."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3},
    )
    return resp["QueryExecutionId"]

def wait_for_query(qid: str, timeout_sec: int = 30) -> None:
    """Wait until Athena query finishes (SUCCEEDED/FAILED/CANCELLED)."""
    start = time.time()
    while True:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}")

        if time.time() - start > timeout_sec:
            raise TimeoutError("Athena query timed out")

        time.sleep(1)

def fetch_all_rows(qid: str) -> list[dict]:
    """
    Return Athena results as list of dicts.
    NOTE: First row is header.
    """
    rows_out = []
    next_token = None
    header = None

    while True:
        kwargs = {"QueryExecutionId": qid, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token

        resp = athena.get_query_results(**kwargs)
        rows = resp["ResultSet"]["Rows"]

        for i, r in enumerate(rows):
            values = [c.get("VarCharValue", "") for c in r.get("Data", [])]

            # header row (first page, first row)
            if header is None:
                header = values
                continue

            # map row -> dict by header
            row_dict = {}
            for idx, col in enumerate(header):
                row_dict[col] = values[idx] if idx < len(values) else ""
            rows_out.append(row_dict)

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return rows_out

def get_pipeline_sla_latest():
    # Your existing view: sla_latest_status
    sql = """
    SELECT *
    FROM sla_latest_status
    ORDER BY source;
    """
    qid = run_athena_query(sql)
    wait_for_query(qid)
    return fetch_all_rows(qid)

def get_business_kpi():
    sql = """
    SELECT *
    FROM orders_business_sla_kpi;
    """
    qid = run_athena_query(sql)
    wait_for_query(qid)
    rows = fetch_all_rows(qid)

    # Usually KPI view returns 1 row. Return {} if empty.
    if not rows:
        return {}
    return rows[0]

def get_business_trend_90d():
    sql = """
    SELECT *
    FROM orders_business_sla_trend_90d
    ORDER BY delivered_day;
    """
    qid = run_athena_query(sql)
    wait_for_query(qid)
    return fetch_all_rows(qid)

def lambda_handler(event, context):
    try:
        pipeline_sla = get_pipeline_sla_latest()
        business_kpi = get_business_kpi()
        business_trend_90d = get_business_trend_90d()

        payload = {
            "pipeline_sla": pipeline_sla,
            "business_kpi": business_kpi,
            "business_trend_90d": business_trend_90d,
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(payload),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({"error": str(e)}),
        }
