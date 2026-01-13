CREATE OR REPLACE VIEW "orders_business_sla_trend_90d" AS 
WITH
  base AS (
   SELECT
     TRY(date_parse(NULLIF(trim(BOTH FROM order_delivered_customer_date), ''), '%Y-%m-%d %H:%i:%s')) delivered_ts
   , TRY(date_parse(NULLIF(trim(BOTH FROM order_estimated_delivery_date), ''), '%Y-%m-%d %H:%i:%s')) estimated_ts
   FROM
     olist_orders
   WHERE (order_status = 'delivered')
) 
, mx AS (
   SELECT max(date(delivered_ts)) max_day
   FROM
     base
   WHERE (delivered_ts IS NOT NULL)
) 
SELECT
  date(delivered_ts) delivered_day
, COUNT(*) total_delivered
, SUM((CASE WHEN (delivered_ts > estimated_ts) THEN 1 ELSE 0 END)) late_orders
, ROUND(((1E2 * SUM((CASE WHEN (delivered_ts > estimated_ts) THEN 1 ELSE 0 END))) / COUNT(*)), 2) late_percentage
FROM
  base
, mx
WHERE ((delivered_ts IS NOT NULL) AND (estimated_ts IS NOT NULL) AND (date(delivered_ts) >= date_add('day', -90, mx.max_day)))
GROUP BY 1
ORDER BY 1 DESC
