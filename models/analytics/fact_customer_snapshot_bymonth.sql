WITH fact_customer_snapshot_bymonth__source AS (
    SELECT *
    FROM {{ ref("fact_sales_order_line") }}
)

, fact_customer_snapshot_bymonth__summarize AS (
    SELECT
        customer_key
        , DATE_TRUNC(order_date, MONTH) AS year_month
        , MAX(order_date) AS last_active_day
        , DATE_DIFF('2016-05-31', MAX(order_date), day) AS lifetime_recency 
        , COUNT(DISTINCT order_date) AS frequency
        , SUM(net_sales) AS monetary
    FROM `fact_customer_snapshot_bymonth__source`
    GROUP BY 1,2
    
)

, fact_customer_snapshot_bymonth__add_stg_year_month AS (
    SELECT
        *
        , LEAD(year_month,1) OVER (PARTITION BY customer_key ORDER BY year_month) AS year_month_1_following
    FROM `fact_customer_snapshot_bymonth__summarize`
)  

    SELECT
        *
        , DATE_DIFF(year_month_1_following, last_active_day, day) AS recency
         
       
    FROM `fact_customer_snapshot_bymonth__add_stg_year_month`