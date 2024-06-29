CREATE TABLE mart.f_customer_retention AS
WITH all_data_with_period_id AS
(SELECT *, 
DATE_PART('week', to_date(date_time, '%YYYY.%MM.%DD')) * (DATE_PART('year', to_date(date_time, '%YYYY.%MM.%DD')) - 2024 + 1) AS period_id 
FROM staging.user_order_log uol
),
new_customers_count_data AS(
SELECT customer_id, period_id, item_id, COUNT(*), SUM(payment_amount) AS new_customers_revenue
FROM all_data_with_period_id
WHERE status = 'shipped'
GROUP BY period_id, customer_id, item_id
HAVING COUNT(*) = 1
ORDER BY period_id, customer_id, item_id
),
returning_customers_count_data AS(
SELECT customer_id, period_id, item_id, COUNT(*), SUM(payment_amount) AS returning_customers_revenue
FROM all_data_with_period_id
WHERE status = 'shipped'
GROUP BY period_id, customer_id, item_id
HAVING COUNT(*) > 1
ORDER BY period_id, customer_id, item_id
),
refunded_customer_count_data AS (
SELECT COUNT(*) AS amount, customer_id, period_id, item_id
FROM all_data_with_period_id
WHERE status = 'refunded'
GROUP BY period_id, item_id, customer_id
),
new_customers_result AS (
SELECT COUNT(customer_id) AS new_customers_count, period_id, item_id, SUM(new_customers_revenue) AS new_customers_revenue
FROM new_customers_count_data
GROUP BY period_id, item_id
),
returning_customers_result AS (
SELECT COUNT(customer_id) AS returning_customers_count, period_id, item_id, SUM(returning_customers_revenue) AS returning_customers_revenue
FROM returning_customers_count_data
GROUP BY period_id, item_id
),
refunded_customer_result AS (
SELECT COUNT(customer_id) AS refunded_customer_count, SUM(amount) AS customers_refunded, period_id, item_id
FROM refunded_customer_count_data
GROUP BY period_id, item_id
)
SELECT COALESCE(new_customers_count, 0) AS new_customers_count, 
COALESCE(returning_customers_count, 0) AS returning_customers_count, 
COALESCE(refunded_customer_count, 0) AS refunded_customer_count, 
'weekly' AS period_name, period_id, item_id,
COALESCE(new_customers_revenue, 0) AS new_customers_revenue, 
COALESCE(returning_customers_revenue, 0) AS returning_customers_revenue, 
COALESCE(customers_refunded, 0) AS customers_refunded
FROM new_customers_result 
LEFT JOIN returning_customers_result USING (period_id, item_id)
LEFT JOIN refunded_customer_result USING (period_id, item_id)
ORDER BY period_id, item_id
