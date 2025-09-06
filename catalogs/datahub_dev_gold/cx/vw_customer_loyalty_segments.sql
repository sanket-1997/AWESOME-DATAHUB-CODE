CREATE OR REPLACE VIEW datahub_dev_gold.cx.vw_customer_loyalty_segments AS(

SELECT
    c.customer_id,
    c.name AS customer_name,
    COALESCE(c.loyalty_points, 0),
    CASE 
        WHEN COALESCE(c.loyalty_points, 0) < 100 THEN 'Bronze'
        WHEN c.loyalty_points BETWEEN 100 AND 200 THEN 'Silver'
        WHEN c.loyalty_points BETWEEN 200 AND 300 THEN 'Gold'
        WHEN c.loyalty_points > 300 THEN 'Platinum'
    END AS loyalty_tier,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_price) AS total_spent
FROM datahub_dev_gold.consumer.fact_orders f
JOIN datahub_dev_gold.consumer.dim_customers c
    ON f.customer_id_sk = c.customer_id_sk
WHERE c.effective_end_date ='9999-12-31'
GROUP BY c.customer_id, c.name, c.loyalty_points);