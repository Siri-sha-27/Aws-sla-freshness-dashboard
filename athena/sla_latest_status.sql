CREATE OR REPLACE VIEW sla_latest_status AS
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY source ORDER BY check_time_utc DESC) rn
  FROM sla_results
)
SELECT
  source,
  status,
  freshness_score,
  latest_object_key,
  check_time_utc
FROM ranked
WHERE rn = 1;
