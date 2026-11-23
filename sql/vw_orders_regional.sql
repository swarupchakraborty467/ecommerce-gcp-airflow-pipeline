-- =============================================================================
-- BigQuery Gold View: vw_orders_regional
-- Joins fact_orders with dim_customers and formats Brazilian state codes into
-- ISO 3166-2 subdivisions (e.g., BR-SP) for accurate Looker Studio geo-mapping.
-- =============================================================================

CREATE OR REPLACE VIEW `project-a04fb5f5-51c7-472d-baf.analytics_gold.vw_orders_regional` AS
SELECT
    f.order_id,
    f.customer_id,
    c.customer_unique_id,
    f.order_status,
    f.order_purchase_time,
    f.order_date,
    f.actual_delivery_days,
    f.delivery_sla_delta_days,
    f.gross_item_value,
    f.total_shipping_cost,
    f.order_total_value,
    f.total_items,
    f.payment_methods,
    'Brazil' AS customer_country,
    c.customer_state,
    CONCAT('BR-', c.customer_state) AS customer_state_iso,
    c.customer_city
FROM `project-a04fb5f5-51c7-472d-baf.analytics_gold.fact_orders` f
LEFT JOIN `project-a04fb5f5-51c7-472d-baf.analytics_silver.dim_customers` c
    ON f.customer_id = c.customer_id;
