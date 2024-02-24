WITH stg_fact_target_salesperson__source AS (
    SELECT
        order_date
        , sales_person_key
        , net_sales
    FROM {{ ref("fact_sales_order_line") }}

)

, stg_fact_target_salesperson__group_by AS (
    SELECT
        DATE_TRUNC(order_date, month) AS year_month
        , sales_person_key AS salesperson_person_key
        , SUM(net_sales) AS net_sales
    FROM `stg_fact_target_salesperson__source`
    GROUP BY 1,2
)

    SELECT
        year_month
        , salesperson_person_key
        , net_sales
    FROM `stg_fact_target_salesperson__group_by`



    

    