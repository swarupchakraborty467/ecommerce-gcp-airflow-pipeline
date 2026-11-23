from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- Configuration ---
PROJECT_ID = "project-a04fb5f5-51c7-472d-baf"
BUCKET_NAME = "ecommerce-analytics-lake-nov26"
BQ_LOCATION = "asia-south1"  # Changed to "asia-south1" as BigQuery datasets were created in Mumbai

# Mapping of GCS subfolders to BigQuery staging table names
INGESTION_TABLES = {
    "customers_csv": "stg_customers",
    "orders_csv": "stg_orders",
    "order_items_csv": "stg_items",
    "order_payments_csv": "stg_payments",
    "products_csv": "stg_products",
    "sellers_csv": "stg_sellers",
}

# --- Silver Layer SQL (Dimensions) ---
SILVER_DIMENSIONS_SQL = f"""

-- 1. Customer Dimension
CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics_silver.dim_customers` AS
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    UPPER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM `{PROJECT_ID}.raw_ecommerce.stg_customers`
WHERE customer_id IS NOT NULL;

-- 2. Product Dimension
CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics_silver.dim_products` AS
SELECT DISTINCT
    product_id,
    COALESCE(product_category_name, 'unknown') AS product_category,
    CAST(product_weight_g AS FLOAT64) AS weight_grams
FROM `{PROJECT_ID}.raw_ecommerce.stg_products`
WHERE product_id IS NOT NULL;

-- 3. Seller Dimension
CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics_silver.dim_sellers` AS
SELECT DISTINCT
    seller_id,
    UPPER(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state
FROM `{PROJECT_ID}.raw_ecommerce.stg_sellers`
WHERE seller_id IS NOT NULL;

"""

# --- Gold Layer SQL (Fact Orders & Customer RFM) ---
GOLD_FACTS_RFM_SQL = f"""

-- 1. Fact Orders Table
CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics_gold.fact_orders` AS
WITH aggregated_items AS (
    SELECT 
        order_id,
        COUNT(order_item_id) AS total_items,
        SUM(price) AS gross_item_value,
        SUM(freight_value) AS total_shipping_cost
    FROM `{PROJECT_ID}.raw_ecommerce.stg_items`
    GROUP BY order_id
),
aggregated_payments AS (
    SELECT 
        order_id,
        STRING_AGG(DISTINCT payment_type, ', ') AS payment_methods,
        SUM(payment_value) AS total_paid_amount
    FROM `{PROJECT_ID}.raw_ecommerce.stg_payments`
    GROUP BY order_id
)
SELECT 
    o.order_id,
    o.customer_id,
    o.order_status,
    TIMESTAMP(o.order_purchase_timestamp) AS order_purchase_time,
    DATE(o.order_purchase_timestamp) AS order_date,
    DATE_DIFF(DATE(o.order_delivered_customer_date), DATE(o.order_purchase_timestamp), DAY) AS actual_delivery_days,
    DATE_DIFF(DATE(o.order_estimated_delivery_date), DATE(o.order_delivered_customer_date), DAY) AS delivery_sla_delta_days,
    COALESCE(i.gross_item_value, 0) AS gross_item_value,
    COALESCE(i.total_shipping_cost, 0) AS total_shipping_cost,
    COALESCE(p.total_paid_amount, 0) AS order_total_value,
    COALESCE(i.total_items, 0) AS total_items,
    COALESCE(p.payment_methods, 'Unknown') AS payment_methods
FROM `{PROJECT_ID}.raw_ecommerce.stg_orders` o
LEFT JOIN aggregated_items i ON o.order_id = i.order_id
LEFT JOIN aggregated_payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered';

-- 2. RFM Customer Segmentation Table
CREATE OR REPLACE TABLE `{PROJECT_ID}.analytics_gold.agg_customer_rfm` AS
WITH max_dataset_date AS (
    SELECT MAX(order_date) AS reference_date FROM `{PROJECT_ID}.analytics_gold.fact_orders`
),
customer_aggregates AS (
    SELECT 
        c.customer_unique_id,
        MAX(f.order_date) AS last_order_date,
        COUNT(DISTINCT f.order_id) AS order_frequency,
        SUM(f.order_total_value) AS total_monetary_value
    FROM `{PROJECT_ID}.analytics_gold.fact_orders` f
    JOIN `{PROJECT_ID}.analytics_silver.dim_customers` c ON f.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),
rfm_scores AS (
    SELECT 
        ca.customer_unique_id,
        DATE_DIFF(m.reference_date, ca.last_order_date, DAY) AS recency_days,
        ca.order_frequency,
        ca.total_monetary_value,
        NTILE(4) OVER (ORDER BY DATE_DIFF(m.reference_date, ca.last_order_date, DAY) DESC) AS r_score,
        NTILE(4) OVER (ORDER BY ca.order_frequency ASC) AS f_score,
        NTILE(4) OVER (ORDER BY ca.total_monetary_value ASC) AS m_score
    FROM customer_aggregates ca
    CROSS JOIN max_dataset_date m
)
SELECT 
    customer_unique_id,
    recency_days,
    order_frequency,
    total_monetary_value,
    (r_score + f_score + m_score) AS composite_rfm_score,
    CASE 
        WHEN (r_score + f_score + m_score) >= 10 THEN 'Champions / High Value'
        WHEN (r_score + f_score + m_score) BETWEEN 7 AND 9 THEN 'Loyal Customers'
        WHEN (r_score + f_score + m_score) BETWEEN 4 AND 6 THEN 'At-Risk'
        ELSE 'Churned'
    END AS customer_segment
FROM rfm_scores;

"""

default_args = {
    "owner": "swarup_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="ecommerce_gcp_etl_pipeline",
    default_args=default_args,
    description="Automated GCS to BigQuery Silver & Gold e-commerce pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["ecommerce", "gcp", "bigquery"],
) as dag:

    transform_silver_dimensions = BigQueryInsertJobOperator(
        task_id="transform_silver_dimensions",
        configuration={
            "query": {
                "query": SILVER_DIMENSIONS_SQL,
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    transform_gold_fact_and_rfm = BigQueryInsertJobOperator(
        task_id="transform_gold_fact_and_rfm",
        configuration={
            "query": {
                "query": GOLD_FACTS_RFM_SQL,
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    for gcs_folder, bq_table in INGESTION_TABLES.items():
        load_gcs_to_bq = GCSToBigQueryOperator(
            task_id=f"ingest_{bq_table}",
            bucket=BUCKET_NAME,
            source_objects=[f"raw_csv/{gcs_folder}/*.csv"],
            destination_project_dataset_table=f"{PROJECT_ID}.raw_ecommerce.{bq_table}",
            source_format="CSV",
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            allow_quoted_newlines=True,
        )

        load_gcs_to_bq >> transform_silver_dimensions

    transform_silver_dimensions >> transform_gold_fact_and_rfm
