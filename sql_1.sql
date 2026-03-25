SELECT
    u.user_id,
    u.region,
    count(o.order_id) AS orders_cnt,
    sum(o.amount) AS revenue
FROM dim_users u
LEFT JOIN fact_orders o
    ON u.user_id = o.user_id
LEFT JOIN fact_payments p
    ON o.order_id = p.order_id
WHERE p.status = 'paid'
  AND o.created_at >= current_date - interval '30 day'
GROUP BY
    u.user_id,
    u.region;