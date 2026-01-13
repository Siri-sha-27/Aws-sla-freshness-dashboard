CREATE OR REPLACE VIEW "orders_business_sla_kpi" AS 
SELECT
  COUNT(*) total_delivered
, SUM((CASE WHEN (delivered_ts > estimated_ts) THEN 1 ELSE 0 END)) late_orders
, ROUND(((1E2 * SUM((CASE WHEN (delivered_ts > estimated_ts) THEN 1 ELSE 0 END))) / COUNT(*)), 2) late_percentage
, ROUND(AVG((CASE WHEN (delivered_ts > estimated_ts) THEN date_diff('day', estimated_ts, delivered_ts) END)), 2) avg_days_late
FROM
  (
   SELECT
     TRY(date_parse(NULLIF(trim(BOTH FROM order_delivered_customer_date), ''), '%Y-%m-%d %H:%i:%s')) delivered_ts
   , TRY(date_parse(NULLIF(trim(BOTH FROM order_estimated_delivery_date), ''), '%Y-%m-%d %H:%i:%s')) estimated_ts
   FROM
     olist_orders
   WHERE (order_status = 'delivered')
)  t
WHERE ((delivered_ts IS NOT NULL) AND (estimated_ts IS NOT NULL))
