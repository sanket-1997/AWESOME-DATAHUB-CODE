CREATE OR REPLACE VIEW datahub_dev_gold.sc.vw_supplier_category_performance AS(
WITH supplier_category_sales AS (
    SELECT
        s.supplier_id,
        s.name AS supplier_name,
        p.category,
        SUM(f.total_price) AS category_revenue,
        AVG(f.total_price) AS avg_order_value,
        COUNT(DISTINCT f.order_id) AS total_orders
    FROM datahub_dev_gold.consumer.fact_orders f
    JOIN datahub_dev_gold.consumer.dim_products p
        ON f.product_id_sk = p.product_id_sk
    JOIN datahub_dev_gold.consumer.dim_suppliers s
        ON p.supplier_id_sk = s.supplier_id_sk
    GROUP BY s.supplier_id, s.name, p.category
),
category_totals AS (
    SELECT category, SUM(category_revenue) AS total_category_revenue
    FROM supplier_category_sales
    GROUP BY category
)
SELECT
    scs.supplier_id,
    scs.supplier_name,
    scs.category,
    scs.category_revenue,
    scs.avg_order_value,
    scs.total_orders,
    ROUND((scs.category_revenue / ct.total_category_revenue) * 100, 2) AS revenue_share_pct,
    CASE 
        WHEN (scs.category_revenue / ct.total_category_revenue) > 0.6 THEN 'Dominant Supplier'
        ELSE 'Competitive Market'
    END AS supplier_role
FROM supplier_category_sales scs
JOIN category_totals ct
    ON scs.category = ct.category);
