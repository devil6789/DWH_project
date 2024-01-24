WITH fact_sales_order__source AS (
SELECT * 
FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename_column AS (
  SELECT 
        order_id AS sales_order_key
        , customer_id AS customer_key
        , picked_by_person_id AS picked_by_person_key
  FROM `fact_sales_order__source`     
)

, fact_sales_order__cast_type AS (
  SELECT 
        cast(sales_order_key as int) as sales_order_key
        , cast(customer_key as int) as customer_key
        , CAST(picked_by_person_key as int) as picked_by_person_key
  FROM `fact_sales_order__rename_column`       
)

SELECT 
      sales_order_key	
      , customer_key
      , coalesce(picked_by_person_key, 0) as picked_by_person_key
FROM `fact_sales_order__cast_type`

